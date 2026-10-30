// Orchestrator of the backfill repair (see docs/backfill-design.md,
// sections 2, 3 and 7).
//
// Samples and counters (kinds "sample" and "counter"): for each configured
// entity it detects gaps in the lookback window (cadence-aware, pure
// detector), fetches the Home Assistant recorder history for each repairable
// gap and replays the fetched states through a fresh IngestEngine (per
// entity) into the injected writer, so bucket and delta semantics stay with
// the engine and repaired rows are byte-for-byte what the worker would have
// written, with the backfill source label.
//
// Hourly prices (kind "price"): missing hours on the hourly grid are
// repaired from the Zonneplan forecast attribute first (plugin
// zonneplan-forecast) and from the recorder history of the tariff entity as
// the fallback (plugin zonneplan-history), rows labelled zonneplan-backfill.
// Hours the sources do not cover stay missing: recent ones are retried on
// the next run (outcome partial), hours older than the recorder retention
// window are registered permanent (the sources are confirmed unable to cover
// them; the fallback source was consulted and answered).
//
// Daily gas prices (kind "gas_price"): missing local days are recovered from
// the recorder history of the configured gas price entity, first observation
// per local day wins (engine semantics), same permanent/retry rules.
//
// Counter entities additionally get the class 2 catch-up scan
// (section 7.3, catchup.mjs): existing attribution-suspect deltas in the
// lookback window are reported in backfill_log (outcome
// 'attribution-suspect'), strictly read-only: no writes to energy_delta,
// no modification of existing rows, no redistribution.
//
// Every path writes exactly one backfill_log row per entity and kind with
// the outcome and honest bookkeeping (rows_written next to rows_missing),
// carrying the run provenance (run_kind, source). Everything with a side
// effect is injected (row loaders, source fetches, db writer, log store),
// so unit tests and the verification script can run the real orchestration
// against fakes. The run is bounded: at most maxRowsPerRun replayed states
// per run (outcome partial when it stops mid-way), and the detection window
// is bounded by the lookback days. dryRun performs detection and fetches but
// no writes at all. A source that is merely down never confirms inability:
// its outcome is source-down (retried next run), never permanent.
// Corroborated entity silence (other configured entities have states in the
// same range) is confirmed absence of data: the gap range is registered in
// the permanent-range registry with source 'corroboration', so later runs
// skip it instead of re-detecting and refetching it forever.

import { BUCKET_MS, DAY_MS, HOUR_MS, dayOf } from "../buckets.mjs";
import { IngestEngine } from "../engine.mjs";
import { detectBucketGaps, detectPriceGaps, detectGasPriceGaps } from "./detect.mjs";
import { flagAttributionSuspects, CATCHUP_WINDOW_MS, DEFAULT_CATCHUP_FACTOR } from "./catchup.mjs";
import { BackfillWriter } from "./writer.mjs";
import { localDayStartMs, nextDay } from "./sources/gas-price-history.mjs";

/** Row label written into price and gas_price rows by these paths. */
export const PRICE_BACKFILL_SOURCE = "zonneplan-backfill";

/**
 * SQL row loader: the bucket starts an entity already has in the database.
 * For samples from measurement, for counters from energy_delta.
 *
 * @param {object} pool - pg.Pool
 * @param {Map<string, number>} ids - haEntityId -> entity.id (from DbWriter)
 */
export function createRowLoader(pool, ids) {
  return async function loadRows(cfg, fromMs, toMs) {
    const id = ids.get(cfg.haEntityId);
    if (id === undefined) return [];
    const table = cfg.kind === "counter" ? "energy_delta" : "measurement";
    const res = await pool.query(
      `SELECT ts FROM ${table}
       WHERE entity_id = $1 AND ts >= $2 AND ts < $3
       ORDER BY ts`,
      [id, new Date(fromMs).toISOString(), new Date(toMs).toISOString()]
    );
    return res.rows.map((r) => new Date(r.ts).getTime());
  };
}

/**
 * SQL row loader: the delta rows (ts and delta_kwh) a counter entity already
 * has in energy_delta. Input for the class 2 catch-up detection: the repair
 * run scans these rows for attribution-suspect values (report only).
 *
 * @param {object} pool - pg.Pool
 * @param {Map<string, number>} ids - haEntityId -> entity.id (from DbWriter)
 */
export function createDeltaLoader(pool, ids) {
  return async function loadDeltas(cfg, fromMs, toMs) {
    const id = ids.get(cfg.haEntityId);
    if (id === undefined) return [];
    const res = await pool.query(
      `SELECT ts, delta_kwh FROM energy_delta
       WHERE entity_id = $1 AND ts >= $2 AND ts < $3
       ORDER BY ts`,
      [id, new Date(fromMs).toISOString(), new Date(toMs).toISOString()]
    );
    return res.rows.map((r) => ({ tsMs: new Date(r.ts).getTime(), deltaKwh: Number(r.delta_kwh) }));
  };
}

/** SQL loader: hour starts already present in the price table (tariff normaal). */
export function createPriceHoursLoader(pool) {
  return async function loadPriceHours(fromMs, toMs) {
    const res = await pool.query(
      `SELECT ts FROM price
       WHERE tariff = 'normaal' AND ts >= $1 AND ts < $2
       ORDER BY ts`,
      [new Date(fromMs).toISOString(), new Date(toMs).toISOString()]
    );
    return res.rows.map((r) => new Date(r.ts).getTime());
  };
}

/** SQL loader: local days already present in the gas_price table. */
export function createGasDaysLoader(pool) {
  return async function loadGasDays(fromDay, toDay) {
    const res = await pool.query(
      `SELECT day FROM gas_price
       WHERE day >= $1 AND day <= $2
       ORDER BY day`,
      [fromDay, toDay]
    );
    return res.rows.map((r) => String(r.day));
  };
}

/**
 * Run the backfill repair over the configured entities.
 *
 * @param {object} options
 * @param {Array} options.entities - the entities from ingest/config.json
 * @param {number} [options.lookbackDays=7] - EMS_BACKFILL_LOOKBACK_DAYS
 * @param {number} [options.maxRowsPerRun=5000] - EMS_BACKFILL_MAX_ROWS_PER_RUN
 * @param {number} [options.maxGapBuckets=36] - EMS_BACKFILL_MAX_GAP_BUCKETS
 * @param {number} [options.catchupFactor=5] - EMS_BACKFILL_CATCHUP_FACTOR,
 *   the class 2 catch-up threshold: |delta_kwh| above this factor times the
 *   median |delta| of the trailing 7 days flags a row attribution-suspect
 * @param {number} [options.recorderRetentionDays=40] -
 *   EMS_BACKFILL_RECORDER_RETENTION_DAYS; hours/days older than this window
 *   cannot be covered by the recorder and may be registered permanent
 * @param {string} [options.timeZone] - EMS_TIMEZONE
 * @param {string} [options.source] - source label written into repaired rows
 *   (energy_delta.source; 'backfill-ha-history' for this path)
 * @param {string} [options.logSource] - plugin recorded in backfill_log.source
 *   for sample/counter runs ('ha-history')
 * @param {string} [options.runKind] - provenance in backfill_log.run_kind
 *   ('cli' or 'auto')
 * @param {() => number} [options.now] - injectable clock
 * @param {number|null} [options.sinceMs] - explicit window start (--since)
 * @param {Function} options.loadRows - async (cfg, fromMs, toMs) => number[]
 * @param {Function} [options.loadDeltas] - async (cfg, fromMs, toMs) =>
 *   {tsMs, deltaKwh}[]; required for the class 2 catch-up scan of counter
 *   entities (absent: no scan, no attribution-suspect rows)
 * @param {Function} options.fetchHistory - async (entityId, fromMs, toMs) =>
 *   {states: [{entity_id, state, last_updated_ts}]} | {error: {code, message}}
 * @param {object} options.dbWriter - DbWriter (or fake with the same contract)
 * @param {object} options.log - store from createBackfillLog (or a fake)
 * @param {Function} [options.loadPriceHours] - async (fromMs, toMs) => number[]
 * @param {Function} [options.fetchPriceForecast] - async () =>
 *   {hours: [{startMs, priceEur}]} | {error}
 * @param {Function} [options.fetchPriceHistory] - async (fromMs, toMs) =>
 *   {hours: [{startMs, priceEur}]} | {error}
 * @param {Function} [options.loadGasDays] - async (fromDay, toDay) => string[]
 * @param {Function} [options.fetchGasHistory] - async (cfg, fromMs, toMs) =>
 *   {days: [{day, priceEur}]} | {error}
 * @param {boolean} [options.dryRun] - detect + fetch, no writes
 * @returns {Promise<{fromMs: number, toMs: number, results: Array<object>}>}
 */
export async function runBackfill(options = {}) {
  const {
    entities = [],
    lookbackDays = 7,
    maxRowsPerRun = 5000,
    maxGapBuckets = 36,
    catchupFactor = DEFAULT_CATCHUP_FACTOR,
    recorderRetentionDays = 40,
    timeZone = "Europe/Amsterdam",
    source = "backfill-ha-history",
    logSource = "ha-history",
    runKind = "cli",
    now = () => Date.now(),
    sinceMs = null,
    loadRows,
    loadDeltas,
    fetchHistory,
    dbWriter,
    log,
    loadPriceHours,
    fetchPriceForecast,
    fetchPriceHistory,
    loadGasDays,
    fetchGasHistory,
    dryRun = false,
  } = options;
  if (typeof loadRows !== "function" || typeof fetchHistory !== "function" || !dbWriter || !log) {
    throw new Error("runBackfill requires loadRows, fetchHistory, dbWriter and log");
  }
  const priceCandidates = entities.filter((e) => e.kind === "price");
  if (priceCandidates.length > 0 && (typeof loadPriceHours !== "function" || typeof fetchPriceForecast !== "function" || typeof fetchPriceHistory !== "function")) {
    throw new Error("runBackfill requires loadPriceHours, fetchPriceForecast and fetchPriceHistory for kind price");
  }
  const gasCandidates = entities.filter((e) => e.kind === "gas_price");
  if (gasCandidates.length > 0 && (typeof loadGasDays !== "function" || typeof fetchGasHistory !== "function")) {
    throw new Error("runBackfill requires loadGasDays and fetchGasHistory for kind gas_price");
  }

  const toMs = now();
  const fromMs = sinceMs !== null ? sinceMs : toMs - Math.max(1, lookbackDays) * DAY_MS;
  const state = { budget: Math.max(0, maxRowsPerRun) };
  const candidates = entities.filter((e) => e.kind === "sample" || e.kind === "counter");

  const c = { entities, fromMs, toMs, state, loadRows, loadDeltas, fetchHistory, dbWriter, log, timeZone, source, logSource, runKind, recorderRetentionDays, maxGapBuckets, catchupFactor, loadPriceHours, fetchPriceForecast, fetchPriceHistory, loadGasDays, fetchGasHistory, dryRun };

  const results = [];
  for (const cfg of candidates) {
    results.push(await repairEntity(cfg, c));
    // Class 2 catch-up scan (report only): existing suspect deltas of counter
    // entities in the lookback window become an attribution-suspect log row.
    // Strictly read-only on the data tables: no writes, no redistribution.
    if (cfg.kind === "counter" && typeof c.loadDeltas === "function") {
      const catchup = await catchupScan(cfg, c);
      if (catchup !== null) results.push(catchup);
    }
  }
  // One price pipeline per run: every kind "price" entity shares the same
  // source (the tariff entity) and the same price table, so the first
  // configured price entity is the audit id for the run.
  if (priceCandidates.length > 0) {
    results.push(await repairPriceEntity(priceCandidates[0], c));
  }
  for (const cfg of gasCandidates) {
    results.push(await repairGasEntity(cfg, c));
  }
  return { fromMs, toMs, results };
}

/** Base result object shared by all per-entity repair functions. */
function baseResult(cfg, kind, c) {
  return {
    haEntityId: cfg.haEntityId,
    kind,
    runKind: c.runKind,
    source: "none",
    outcome: "no-gap",
    rowsWritten: 0,
    rowsMissing: 0,
    statesReplayed: 0,
    rangeStartMs: c.fromMs,
    rangeEndMs: c.toMs,
    detail: null,
  };
}

/** Insert the log row (unless dry-run: no writes at all). */
async function finish(res, c) {
  if (!c.dryRun) {
    await c.log.insert({
      haEntityId: res.haEntityId,
      kind: res.kind,
      runKind: res.runKind,
      source: res.source,
      outcome: res.outcome,
      rangeStartMs: res.rangeStartMs,
      rangeEndMs: res.rangeEndMs,
      rowsWritten: res.rowsWritten,
      rowsMissing: res.rowsMissing,
      detail: res.detail,
    });
  }
}

/** Repair one sample/counter entity; returns the per-entity result. */
async function repairEntity(cfg, c) {
  const rows = await c.loadRows(cfg, c.fromMs, c.toMs);
  const det = detectBucketGaps(rows, c.fromMs, c.toMs, { maxGapBuckets: c.maxGapBuckets });

  // Honest bookkeeping: rows_written counts the rows really inserted;
  // rows_missing counts the detected missing buckets this run did not write
  // (not-attempted or truncated gaps, corroborated-silence buckets,
  // source-down gaps). On a fully repaired gap the two add up to the
  // detected missing buckets with rows_missing 0.
  const totalMissing = det.repairable.reduce((a, g) => a + g.missingBuckets, 0);

  const res = baseResult(cfg, cfg.kind, c);
  res.source = c.logSource;
  res.rowsMissing = totalMissing;
  res.gapsRepairable = det.repairable.length;
  res.gapsReportable = det.reportable.length;

  if (det.repairable.length === 0) {
    res.detail = detailObject(0, 0, 0, 0, det.reportable.length, false, false);
    await finish(res, c);
    return res;
  }

  // The logged range is the union of the entity's repairable gaps.
  res.rangeStartMs = Math.min(...det.repairable.map((g) => g.startMs));
  res.rangeEndMs = Math.max(...det.repairable.map((g) => g.endMs));

  // Permanent registry: skip gaps inside ranges already marked permanent.
  // Source-down is never permanent (docs/backfill-design.md, rule 8): a
  // source that is merely down is a retry case, and every run retries it
  // naturally. Nothing in this path writes permanent rows.
  const actionable = [];
  let permanent = 0;
  for (const g of det.repairable) {
    if (await c.log.isPermanent(cfg.haEntityId, cfg.kind, g.startMs, g.endMs)) {
      permanent += 1;
      continue;
    }
    actionable.push(g);
  }
  if (actionable.length === 0) {
    res.outcome = "permanent";
    res.detail = detailObject(0, 0, 0, 0, permanent, det.reportable.length, false, false);
    await finish(res, c);
    return res;
  }

  // Replay: one fresh engine per entity, gaps in ascending order.
  let tickAt = c.toMs;
  const bfWriter = new BackfillWriter(c.dbWriter, c.source);
  const engine = new IngestEngine(bfWriter, { timeZone: c.timeZone, now: () => tickAt });
  engine.registerEntities([cfg]);

  let repaired = 0;
  let silent = 0;
  let silentRegistered = 0;
  let down = 0;
  let truncated = false;
  let unattempted = false;

  for (const g of actionable) {
    if (c.state.budget <= 0) {
      unattempted = true;
      break;
    }
    // One bucket of padding on both sides: the left padding gives the engine
    // the reading before the gap (baseline rule), the right padding a state
    // after the last missing bucket so the engine finalizes it.
    const hist = await c.fetchHistory(cfg.haEntityId, g.startMs - BUCKET_MS, g.endMs + BUCKET_MS);
    if (hist === null || hist === undefined || hist.error) {
      down += 1;
      continue;
    }
    const states = hist.states ?? [];
    // Only states inside the unpadded gap range classify the gap: the padding
    // states belong to buckets that already have rows and would make every
    // silent gap look like data, so corroboration could never fire.
    const inGap = states.filter((s) => {
      const ts = Math.round(s.last_updated_ts * 1000);
      return ts >= g.startMs && ts < g.endMs && Number.isFinite(parseFloat(s.state));
    });
    if (inGap.length === 0) {
      // No numeric states inside the gap: corroborate (design rule 7). If
      // other configured entities have states in the same range, the
      // recorder was up and this entity was silent: register the range so
      // later runs skip it instead of re-detecting and refetching it forever.
      if (await corroboratedSilence(cfg, g, c)) {
        silent += 1;
        silentRegistered += 1;
        if (!c.dryRun) {
          await c.log.markPermanent({
            haEntityId: cfg.haEntityId,
            kind: cfg.kind,
            runKind: c.runKind,
            source: "corroboration",
            rangeStartMs: g.startMs,
            rangeEndMs: g.endMs,
            rowsMissing: g.missingBuckets,
            detail: {
              summary:
                "corroborated entity silence: no numeric states in the gap, " +
                "other entities have states; nothing to recover",
            },
          });
        }
      } else {
        // Not corroborated: the recorder may have been down or have purged
        // the range. A source outage confirms nothing, so this stays a retry
        // case (source-down), never permanent (honesty rule 8).
        down += 1;
      }
      continue;
    }
    // At least one in-gap state: replay everything the recorder saw,
    // padding included. Writes at padding buckets are no-ops through
    // ON CONFLICT (their rows already exist).
    const n = Math.min(states.length, c.state.budget);
    c.state.budget -= n;
    if (!c.dryRun) {
      tickAt = g.endMs;
      for (const s of states.slice(0, n)) {
        engine.onState(cfg.haEntityId, parseFloat(s.state), Math.round(s.last_updated_ts * 1000));
      }
      engine.tick(); // close the last open bucket of this gap
    }
    res.statesReplayed += n;
    if (n < states.length) truncated = true;
    repaired += 1;
  }

  res.rowsWritten = c.dryRun ? 0 : await bfWriter.flush();
  res.rowsMissing = Math.max(0, totalMissing - res.rowsWritten);

  if (truncated || unattempted || (repaired > 0 && down > 0)) {
    res.outcome = "partial";
  } else if (repaired > 0) {
    res.outcome = "repaired";
  } else if (down > 0) {
    res.outcome = "source-down";
  } else {
    res.outcome = "no-gap"; // every gap was corroborated entity silence
  }
  res.detail = detailObject(repaired, silent, silentRegistered, down, permanent, det.reportable.length, truncated, unattempted);
  await finish(res, c);
  return res;
}

/**
 * Class 2 catch-up scan of one counter entity (see docs/backfill-design.md,
 * section 7.3). Loads the entity's delta rows and flags the
 * attribution-suspect ones among those inside the lookback window. The
 * median context reaches CATCHUP_WINDOW_MS before the window start; the
 * suspects themselves are only reported for rows in the window.
 *
 * Report only: the result carries outcome 'attribution-suspect', source
 * 'none' (no plugin supplied rows), rows_written 0 and the suspects in the
 * structured detail. Nothing is written to energy_delta, no existing row is
 * modified, no energy is redistributed. Returns null when there is nothing
 * to report (no extra log row: silence is not news).
 */
async function catchupScan(cfg, c) {
  const deltas = await c.loadDeltas(cfg, c.fromMs - CATCHUP_WINDOW_MS, c.toMs);
  const suspects = flagAttributionSuspects(deltas, { factor: c.catchupFactor }).filter(
    (s) => s.tsMs >= c.fromMs && s.tsMs < c.toMs
  );
  if (suspects.length === 0) return null;

  const res = {
    haEntityId: cfg.haEntityId,
    kind: cfg.kind,
    runKind: c.runKind,
    source: "none",
    outcome: "attribution-suspect",
    rowsWritten: 0,
    rowsMissing: suspects.length,
    statesReplayed: 0,
    rangeStartMs: suspects[0].tsMs,
    rangeEndMs: suspects[suspects.length - 1].tsMs + BUCKET_MS,
    detail: {
      summary:
        `${suspects.length} suspect delta row(s): |delta_kwh| above ` +
        `${c.catchupFactor} x the median |delta| of the trailing 7 days; ` +
        `report only, no rows written`,
      suspects: suspects.length,
      catchup_factor: c.catchupFactor,
      suspect_rows: suspects.map((s) => ({
        ts: new Date(s.tsMs).toISOString(),
        delta_kwh: s.deltaKwh,
        median_abs_delta: s.medianAbsDelta,
        window_samples: s.windowSamples,
      })),
    },
  };
  await finish(res, c);
  return res;
}

/**
 * No usable states for the entity in the gap range. Do other configured
 * entities have states in that same range? If yes the recorder was up and
 * the entity was silent (nothing to repair); if no, the recorder was down.
 */
async function corroboratedSilence(cfg, g, c) {
  for (const other of c.entities) {
    if (other.haEntityId === cfg.haEntityId) continue;
    const hist = await c.fetchHistory(other.haEntityId, g.startMs, g.endMs);
    if (hist && !hist.error && (hist.states ?? []).length > 0) {
      return true;
    }
  }
  return false;
}

/** Group sorted hour starts into contiguous [startMs, endMs) ranges. */
function toHourRanges(hours) {
  const ranges = [];
  for (const h of hours) {
    const last = ranges[ranges.length - 1];
    if (last && last.endMs === h) last.endMs = h + HOUR_MS;
    else ranges.push({ startMs: h, endMs: h + HOUR_MS });
  }
  return ranges;
}

/** Group sorted day strings into contiguous runs. */
function toDayRuns(days) {
  const runs = [];
  for (const d of days) {
    const last = runs[runs.length - 1];
    if (last && nextDay(last[last.length - 1]) === d) last.push(d);
    else runs.push([d]);
  }
  return runs;
}

/**
 * Register the given hours/days as permanent ranges. Only called when the
 * fallback source was consulted and answered (no fetch error): a merely down
 * source never confirms inability, so it never makes a range permanent.
 */
async function registerPermanentRanges(c, { haEntityId, kind, ranges, detail }) {
  for (const r of ranges) {
    await c.log.markPermanent({
      haEntityId,
      kind,
      runKind: c.runKind,
      source: "zonneplan-history", // the plugin that confirmed inability
      rangeStartMs: r.startMs,
      rangeEndMs: r.endMs,
      rowsMissing: r.missingCount,
      detail,
    });
  }
}

/**
 * Repair the hourly prices. Forecast first, recorder history fallback
 * second; a source that yields nothing for an hour does not fail the run.
 * Hours no source covers are retried while they are recent, and registered
 * permanent once they are older than the recorder retention window.
 */
async function repairPriceEntity(cfg, c) {
  const existing = await c.loadPriceHours(c.fromMs, c.toMs);
  const runs = detectPriceGaps(existing, c.fromMs, c.toMs);
  const detectedHours = [];
  for (const r of runs) {
    for (let t = r.startMs; t < r.endMs; t += HOUR_MS) detectedHours.push(t);
  }
  detectedHours.sort((a, b) => a - b);

  const res = baseResult(cfg, "price", c);
  res.rowsMissing = detectedHours.length;
  const d = {
    detected: detectedHours.length,
    fromForecast: 0,
    fromHistory: 0,
    inPermanent: 0,
    uncoveredRecent: 0,
    registeredPermanent: 0,
    forecastError: null,
    historyError: null,
  };

  if (runs.length > 0) {
    res.rangeStartMs = detectedHours[0];
    res.rangeEndMs = detectedHours[detectedHours.length - 1] + HOUR_MS;
  }

  if (runs.length === 0) {
    res.detail = priceDetail(d);
    await finish(res, c);
    return res;
  }

  // Permanent exclusion: an hour is excluded when a permanent range fully
  // covers it (the registered ranges are hour-aligned).
  const perm = await c.log.permanentRanges(cfg.haEntityId, "price", c.fromMs, c.toMs);
  const hourInPermanent = (h) => perm.some((r) => r.startMs <= h && r.endMs >= h + HOUR_MS);
  const missingHours = detectedHours.filter((h) => !hourInPermanent(h));
  d.inPermanent = detectedHours.length - missingHours.length;

  if (missingHours.length === 0) {
    res.outcome = "permanent";
    res.detail = priceDetail(d);
    await finish(res, c);
    return res;
  }

  // Forecast first (rolling window, one fetch per run).
  let forecastHours = new Map();
  const fr = await c.fetchPriceForecast();
  if (fr === null || fr === undefined || fr.error) {
    d.forecastError = fr?.error?.code ?? "unknown";
  } else {
    forecastHours = new Map((fr.hours ?? []).map((h) => [h.startMs, h.priceEur]));
  }

  const bfWriter = new BackfillWriter(c.dbWriter, PRICE_BACKFILL_SOURCE);
  const fromForecast = [];
  for (const h of missingHours) {
    if (forecastHours.has(h)) {
      if (!c.dryRun) bfWriter.writePrice(cfg, h, forecastHours.get(h));
      fromForecast.push(h);
    }
  }
  d.fromForecast = fromForecast.length;

  // History fallback for the hours the forecast does not cover.
  const remaining = missingHours.filter((h) => !forecastHours.has(h));
  let historyHours = new Map();
  if (remaining.length > 0) {
    const hr = await c.fetchPriceHistory(remaining[0], remaining[remaining.length - 1] + HOUR_MS);
    if (hr === null || hr === undefined || hr.error) {
      d.historyError = hr?.error?.code ?? "unknown";
    } else {
      historyHours = new Map((hr.hours ?? []).map((h) => [h.startMs, h.priceEur]));
    }
  }
  const fromHistory = remaining.filter((h) => historyHours.has(h));
  if (!c.dryRun) {
    for (const h of fromHistory) bfWriter.writePrice(cfg, h, historyHours.get(h));
  }
  d.fromHistory = fromHistory.length;

  res.rowsWritten = c.dryRun ? 0 : await bfWriter.flush();
  res.rowsMissing = Math.max(0, detectedHours.length - res.rowsWritten);

  // Hours no source covers: retried while recent; older than the recorder
  // retention window they are registered permanent, but only when the
  // fallback source answered (a down source confirms nothing).
  const written = new Set([...fromForecast, ...fromHistory]);
  const uncovered = missingHours.filter((h) => !written.has(h));
  const cutoff = c.toMs - Math.max(0, c.recorderRetentionDays) * DAY_MS;
  const permanentHours = d.historyError ? [] : uncovered.filter((h) => h < cutoff);
  d.uncoveredRecent = uncovered.length - permanentHours.length;
  d.registeredPermanent = permanentHours.length;
  if (permanentHours.length > 0 && !c.dryRun) {
    await registerPermanentRanges(c, {
      haEntityId: cfg.haEntityId,
      kind: "price",
      ranges: toHourRanges(permanentHours).map((r) => ({ ...r, missingCount: (r.endMs - r.startMs) / HOUR_MS })),
      detail: { summary: "hours older than recorder retention, no source covers them" },
    });
  }

  if (d.historyError && res.rowsWritten === 0 && permanentHours.length === 0) {
    res.outcome = "source-down";
  } else if (d.uncoveredRecent > 0) {
    res.outcome = "partial";
  } else if (res.rowsWritten > 0) {
    res.outcome = "repaired";
  } else if (permanentHours.length > 0) {
    res.outcome = "permanent";
  }
  res.source =
    fromForecast.length >= fromHistory.length && fromForecast.length > 0
      ? "zonneplan-forecast"
      : fromHistory.length > 0
        ? "zonneplan-history"
        : "none";
  res.detail = priceDetail(d);
  await finish(res, c);
  return res;
}

/** Structured detail for a price run (JSONB). */
function priceDetail(d) {
  const parts = [];
  if (d.fromForecast > 0) parts.push(`${d.fromForecast} hour(s) from forecast`);
  if (d.fromHistory > 0) parts.push(`${d.fromHistory} hour(s) from history`);
  if (d.inPermanent > 0) parts.push(`${d.inPermanent} hour(s) already in permanent range(s)`);
  if (d.uncoveredRecent > 0) parts.push(`${d.uncoveredRecent} hour(s) uncovered (recent, retried)`);
  if (d.registeredPermanent > 0) parts.push(`${d.registeredPermanent} hour(s) registered permanent`);
  if (d.forecastError) parts.push(`forecast source down (${d.forecastError})`);
  if (d.historyError) parts.push(`history source down (${d.historyError})`);
  if (parts.length === 0) parts.push("no gaps");
  return {
    summary: parts.join(", "),
    hours_detected_missing: d.detected,
    hours_from_forecast: d.fromForecast,
    hours_from_history: d.fromHistory,
    hours_in_permanent_ranges: d.inPermanent,
    hours_uncovered_recent: d.uncoveredRecent,
    hours_registered_permanent: d.registeredPermanent,
    forecast_error: d.forecastError,
    history_error: d.historyError,
  };
}

/**
 * Repair the daily gas prices from the recorder history of the configured
 * gas price entity. Uncovered days follow the same retry/permanent rules as
 * the price hours.
 */
async function repairGasEntity(cfg, c) {
  const tz = c.timeZone;
  const fromDay = dayOf(c.fromMs, tz);
  const toDay = dayOf(c.toMs, tz);
  const existingDays = await c.loadGasDays(fromDay, toDay);
  const detectedDays = detectGasPriceGaps(existingDays, fromDay, toDay); // ascending day strings

  const res = baseResult(cfg, "gas_price", c);
  res.source = "zonneplan-history";
  res.rowsMissing = detectedDays.length;
  const d = {
    detected: detectedDays.length,
    fromHistory: 0,
    inPermanent: 0,
    uncoveredRecent: 0,
    registeredPermanent: 0,
    historyError: null,
  };

  if (detectedDays.length > 0) {
    res.rangeStartMs = localDayStartMs(detectedDays[0], tz);
    res.rangeEndMs = localDayStartMs(nextDay(detectedDays[detectedDays.length - 1]), tz);
  }

  if (detectedDays.length === 0) {
    res.detail = gasDetail(d);
    await finish(res, c);
    return res;
  }

  // Permanent exclusion: a day is excluded when a permanent range fully
  // covers its local-day span (the registered ranges are day-aligned).
  const perm = await c.log.permanentRanges(
    cfg.haEntityId, "gas_price",
    localDayStartMs(fromDay, tz), localDayStartMs(nextDay(toDay), tz)
  );
  const dayInPermanent = (day) =>
    perm.some((r) => r.startMs <= localDayStartMs(day, tz) && r.endMs >= localDayStartMs(nextDay(day), tz));
  const missingDays = detectedDays.filter((day) => !dayInPermanent(day));
  d.inPermanent = detectedDays.length - missingDays.length;

  if (missingDays.length === 0) {
    res.outcome = "permanent";
    res.detail = gasDetail(d);
    await finish(res, c);
    return res;
  }

  const fetchStart = localDayStartMs(missingDays[0], tz);
  const fetchEnd = localDayStartMs(nextDay(missingDays[missingDays.length - 1]), tz);
  const hr = await c.fetchGasHistory(cfg, fetchStart, fetchEnd);
  let dayPrices = new Map();
  if (hr === null || hr === undefined || hr.error) {
    d.historyError = hr?.error?.code ?? "unknown";
  } else {
    dayPrices = new Map((hr.days ?? []).map((x) => [x.day, x.priceEur]));
  }

  const bfWriter = new BackfillWriter(c.dbWriter, PRICE_BACKFILL_SOURCE);
  const writtenDays = missingDays.filter((day) => dayPrices.has(day));
  if (!c.dryRun) {
    for (const day of writtenDays) bfWriter.writeGasPrice(cfg, day, dayPrices.get(day));
  }
  d.fromHistory = writtenDays.length;

  res.rowsWritten = c.dryRun ? 0 : await bfWriter.flush();
  res.rowsMissing = Math.max(0, detectedDays.length - res.rowsWritten);

  const uncoveredDays = missingDays.filter((day) => !dayPrices.has(day));
  const cutoff = c.toMs - Math.max(0, c.recorderRetentionDays) * DAY_MS;
  const permanentDays = d.historyError ? [] : uncoveredDays.filter((day) => localDayStartMs(day, tz) < cutoff);
  d.uncoveredRecent = uncoveredDays.length - permanentDays.length;
  d.registeredPermanent = permanentDays.length;
  if (permanentDays.length > 0 && !c.dryRun) {
    await registerPermanentRanges(c, {
      haEntityId: cfg.haEntityId,
      kind: "gas_price",
      ranges: toDayRuns(permanentDays).map((run) => ({
        startMs: localDayStartMs(run[0], tz),
        endMs: localDayStartMs(nextDay(run[run.length - 1]), tz),
        missingCount: run.length,
      })),
      detail: { summary: "days older than recorder retention, no source covers them" },
    });
  }

  if (d.historyError && res.rowsWritten === 0) {
    res.outcome = "source-down";
  } else if (d.uncoveredRecent > 0) {
    res.outcome = "partial";
  } else if (res.rowsWritten > 0) {
    res.outcome = "repaired";
  } else if (permanentDays.length > 0) {
    res.outcome = "permanent";
  }
  res.detail = gasDetail(d);
  await finish(res, c);
  return res;
}

/** Structured detail for a gas price run (JSONB). */
function gasDetail(d) {
  const parts = [];
  if (d.fromHistory > 0) parts.push(`${d.fromHistory} day(s) from history`);
  if (d.inPermanent > 0) parts.push(`${d.inPermanent} day(s) already in permanent range(s)`);
  if (d.uncoveredRecent > 0) parts.push(`${d.uncoveredRecent} day(s) uncovered (recent, retried)`);
  if (d.registeredPermanent > 0) parts.push(`${d.registeredPermanent} day(s) registered permanent`);
  if (d.historyError) parts.push(`history source down (${d.historyError})`);
  if (parts.length === 0) parts.push("no gaps");
  return {
    summary: parts.join(", "),
    days_detected_missing: d.detected,
    days_from_history: d.fromHistory,
    days_in_permanent_ranges: d.inPermanent,
    days_uncovered_recent: d.uncoveredRecent,
    days_registered_permanent: d.registeredPermanent,
    history_error: d.historyError,
  };
}

/**
 * Structured detail for sample/counter runs (JSONB): a human-readable
 * summary field plus the counters of the run.
 */
function detailObject(repaired, silent, silentRegistered, down, permanent, reportable, truncated, unattempted) {
  const parts = [];
  if (repaired > 0) parts.push(`${repaired} gap(s) repaired`);
  if (silent > 0) parts.push(`${silent} silent (corroborated)`);
  if (silentRegistered > 0) parts.push(`${silentRegistered} silent range(s) registered via corroboration`);
  if (down > 0) parts.push(`${down} source-down`);
  if (permanent > 0) parts.push(`${permanent} gap(s) in permanent range(s)`);
  if (reportable > 0) parts.push(`${reportable} run(s) reportable but not repairable`);
  if (truncated) parts.push("row budget exhausted mid-gap");
  if (unattempted) parts.push("row budget exhausted, gap(s) not attempted");
  if (parts.length === 0) parts.push("no gaps");
  return {
    summary: parts.join(", "),
    gaps_repaired: repaired,
    gaps_silent: silent,
    gaps_silent_registered: silentRegistered,
    gaps_source_down: down,
    gaps_permanent: permanent,
    runs_reportable: reportable,
    budget_exhausted_mid_gap: truncated,
    budget_exhausted_not_attempted: unattempted,
  };
}
