// Orchestrator of the backfill repair (see docs/backfill-design.md,
// sections 2, 3 and 7). For each configured entity of kind "sample" or
// "counter" it:
//   1. detects gaps in the lookback window (cadence-aware, pure detector);
//   2. fetches the Home Assistant recorder history for each repairable gap;
//   3. replays the fetched states through a fresh IngestEngine (per entity)
//      into the injected writer, so bucket and delta semantics stay with the
//      engine and repaired rows are byte-for-byte what the worker would have
//      written, with the backfill source label;
//   4. corroborates an empty fetch against the other configured entities
//      (entity silence vs recorder down);
//   5. writes exactly one backfill_log row per entity/kind with the outcome.
//
// Everything with a side effect is injected (row loader, history fetch,
// db writer, log store), so unit tests and the verification script can run
// the real orchestration against fakes. The run is bounded: at most
// maxRowsPerRun replayed states per run (outcome partial when it stops
// mid-way). dryRun performs detection and fetches but no writes at all.

import { BUCKET_MS, DAY_MS } from "../buckets.mjs";
import { IngestEngine } from "../engine.mjs";
import { detectBucketGaps } from "./detect.mjs";
import { BackfillWriter } from "./writer.mjs";

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
 * Run the backfill repair over the configured entities.
 *
 * @param {object} options
 * @param {Array} options.entities - the entities from ingest/config.json
 * @param {number} [options.lookbackDays=7] - EMS_BACKFILL_LOOKBACK_DAYS
 * @param {number} [options.maxRowsPerRun=5000] - EMS_BACKFILL_MAX_ROWS_PER_RUN
 * @param {number} [options.maxGapBuckets=36] - EMS_BACKFILL_MAX_GAP_BUCKETS
 * @param {string} [options.timeZone] - EMS_TIMEZONE
 * @param {string} [options.source] - source label for repaired rows
 * @param {() => number} [options.now] - injectable clock
 * @param {number|null} [options.sinceMs] - explicit window start (--since)
 * @param {Function} options.loadRows - async (cfg, fromMs, toMs) => number[]
 * @param {Function} options.fetchHistory - async (entityId, fromMs, toMs) =>
 *   {states: [{entity_id, state, last_updated_ts}]} | {error: {code, message}}
 * @param {object} options.dbWriter - DbWriter (or fake with the same contract)
 * @param {object} options.log - store from createBackfillLog (or a fake)
 * @param {boolean} [options.dryRun] - detect + fetch, no writes
 * @returns {Promise<{fromMs: number, toMs: number, results: Array<object>}>}
 */
export async function runBackfill(options = {}) {
  const {
    entities = [],
    lookbackDays = 7,
    maxRowsPerRun = 5000,
    maxGapBuckets = 36,
    timeZone = "Europe/Amsterdam",
    source = "backfill-ha-history",
    now = () => Date.now(),
    sinceMs = null,
    loadRows,
    fetchHistory,
    dbWriter,
    log,
    dryRun = false,
  } = options;
  if (typeof loadRows !== "function" || typeof fetchHistory !== "function" || !dbWriter || !log) {
    throw new Error("runBackfill requires loadRows, fetchHistory, dbWriter and log");
  }

  const toMs = now();
  const fromMs = sinceMs !== null ? sinceMs : toMs - Math.max(1, lookbackDays) * DAY_MS;
  const state = { budget: Math.max(0, maxRowsPerRun) };
  const candidates = entities.filter((e) => e.kind === "sample" || e.kind === "counter");

  const results = [];
  for (const cfg of candidates) {
    results.push(await repairEntity(cfg, { entities, fromMs, toMs, state, loadRows, fetchHistory, dbWriter, log, timeZone, source, maxGapBuckets, dryRun }));
  }
  return { fromMs, toMs, results };
}

/** Repair one entity; returns the per-entity result (also the log entry). */
async function repairEntity(cfg, c) {
  const rows = await c.loadRows(cfg, c.fromMs, c.toMs);
  const det = detectBucketGaps(rows, c.fromMs, c.toMs, { maxGapBuckets: c.maxGapBuckets });

  const res = {
    haEntityId: cfg.haEntityId,
    kind: cfg.kind,
    outcome: "no-gap",
    rowsWritten: 0,
    statesReplayed: 0,
    gapsRepairable: det.repairable.length,
    gapsReportable: det.reportable.length,
    rangeStartMs: c.fromMs,
    rangeEndMs: c.toMs,
    detail: "",
  };

  if (det.repairable.length === 0) {
    res.detail = describe(0, 0, 0, det.reportable.length);
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
    res.detail = `${permanent} gap(s) in permanent range(s)`;
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
    const usable = states.filter((s) => Number.isFinite(parseFloat(s.state)));
    if (usable.length === 0) {
      // No data for this entity in the range: corroborate (design rule 7).
      if (await corroboratedSilence(cfg, g, c)) {
        silent += 1;
      } else {
        down += 1;
      }
      continue;
    }
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

  if (truncated || unattempted || (repaired > 0 && down > 0)) {
    res.outcome = "partial";
  } else if (repaired > 0) {
    res.outcome = "repaired";
  } else if (down > 0) {
    res.outcome = "source-down";
  } else {
    res.outcome = "no-gap"; // every gap was corroborated entity silence
  }
  res.detail = describe(repaired, silent, down, det.reportable.length) +
    (truncated ? "; row budget exhausted mid-gap" : "") +
    (unattempted ? "; row budget exhausted, gap(s) not attempted" : "");
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

/** Insert the log row (unless dry-run: no writes at all). */
async function finish(res, c) {
  if (!c.dryRun) {
    await c.log.insert({
      haEntityId: res.haEntityId,
      kind: res.kind,
      outcome: res.outcome,
      rangeStartMs: res.rangeStartMs,
      rangeEndMs: res.rangeEndMs,
      rowsWritten: res.rowsWritten,
      detail: res.detail,
    });
  }
}

function describe(repaired, silent, down, reportable) {
  const parts = [];
  if (repaired > 0) parts.push(`${repaired} gap(s) repaired`);
  if (silent > 0) parts.push(`${silent} silent (corroborated)`);
  if (down > 0) parts.push(`${down} source-down`);
  if (reportable > 0) parts.push(`${reportable} run(s) reportable but not repairable`);
  return parts.length > 0 ? parts.join(", ") : "no gaps";
}
