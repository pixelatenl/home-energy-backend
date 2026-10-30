// Unit tests for the backfill repair orchestrator (runBackfill with injected
// fakes: no I/O, no database, no Home Assistant). Run with: node --test test/
//
// These tests pin the honesty rules of the mechanism (docs/backfill-design.md):
// in particular that a source that is merely down is a retry case and never
// promoted to permanent, no matter how many consecutive runs fail.

import test from "node:test";
import assert from "node:assert/strict";
import { runBackfill } from "../src/backfill/repair.mjs";

const BUCKET = 5 * 60 * 1000;
const HOUR = 60 * 60 * 1000;
const WINDOW = 4 * HOUR;

const counterEntity = {
  haEntityId: "sensor.grid_consumption_total",
  name: "Grid consumption total",
  domain: "grid",
  unit: "kWh",
  kind: "counter",
};

/** Fake DbWriter: counts inserts, ON CONFLICT DO NOTHING returns 0. */
class FakeDb {
  constructor() {
    this.deltas = new Map(); // ts -> delta
    this.prices = new Map(); // hour -> {price, source}
    this.gasDays = new Map(); // day -> {price, source}
  }
  writeEnergyDelta(meta, tsMs, deltaKwh, source = "ingest") {
    const key = meta.haEntityId + "@" + tsMs;
    if (this.deltas.has(key)) return Promise.resolve(0);
    this.deltas.set(key, { delta: deltaKwh, source });
    return Promise.resolve(1);
  }
  writePrice(meta, tsMs, priceEur) {
    const key = meta.haEntityId + "@" + tsMs;
    if (this.prices.has(key)) return Promise.resolve(0);
    this.prices.set(key, { price: priceEur, source: meta.source ?? "zonneplan" });
    return Promise.resolve(1);
  }
  writeGasPrice(meta, day, priceEur) {
    if (this.gasDays.has(day)) return Promise.resolve(0);
    this.gasDays.set(day, { price: priceEur, source: meta.source ?? "zonneplan" });
    return Promise.resolve(1);
  }
  writeMeasurement() {
    return Promise.resolve(0);
  }
}

/** Fake log store: records inserts and answers the permanent registry. */
class FakeLog {
  constructor() {
    this.rows = [];
    this.permanent = [];
  }
  async insert(entry) {
    this.rows.push(entry);
  }
  async markPermanent(entry) {
    this.permanent.push(entry);
  }
  async isPermanent(haEntityId, kind, rangeStartMs, rangeEndMs) {
    return this.permanent.some(
      (p) =>
        p.haEntityId === haEntityId &&
        p.kind === kind &&
        p.rangeStartMs <= rangeStartMs &&
        p.rangeEndMs >= rangeEndMs
    );
  }
  async permanentRanges(haEntityId, kind, fromMs, toMs) {
    return this.permanent
      .filter(
        (p) => p.haEntityId === haEntityId && p.kind === kind && p.rangeStartMs < toMs && p.rangeEndMs > fromMs
      )
      .map((p) => ({ startMs: p.rangeStartMs, endMs: p.rangeEndMs }));
  }
}

/** Worker rows with a synthetic 45-minute gap: buckets 0..9 and 18..35. */
function existingRowsWithGap() {
  const rows = [];
  for (let i = 0; i <= 9; i++) rows.push(i * BUCKET);
  for (let i = 18; i <= 35; i++) rows.push(i * BUCKET);
  return rows;
}

test("source-down is never permanent: a typed fetch error across many runs stays source-down", async () => {
  const existing = existingRowsWithGap();
  const db = new FakeDb();
  const log = new FakeLog();
  let fetchCalls = 0;
  const fetchHistory = async () => {
    fetchCalls += 1;
    return { error: { code: "fetch-error", message: "HA unreachable" } };
  };

  // Twelve consecutive runs, all with the source down. Attempt counting
  // would have promoted the range after five; the honest mechanism retries.
  const RUNS = 12;
  for (let run = 0; run < RUNS; run++) {
    const { results } = await runBackfill({
      entities: [counterEntity],
      sinceMs: 0,
      now: () => WINDOW,
      loadRows: async () => existing,
      fetchHistory,
      dbWriter: db,
      log,
    });
    assert.equal(results.length, 1);
    assert.equal(results[0].outcome, "source-down", `run ${run}: the gap stays a retry case`);
    assert.equal(results[0].rowsWritten, 0, `run ${run}: nothing is written while the source is down`);
    assert.equal(results[0].rowsMissing, 8, `run ${run}: the gap's buckets stay unwritten`);
  }

  assert.equal(log.rows.length, RUNS, "every run writes its own log row");
  assert.ok(
    log.rows.every((r) => r.outcome === "source-down"),
    "every logged outcome is source-down"
  );
  assert.ok(log.rows.every((r) => r.rowsMissing === 8), "every log row books the 8 missing buckets");
  assert.equal(log.permanent.length, 0, "no permanent log row is ever written from retry counts");
  assert.equal(db.deltas.size, 0, "no data rows were written");
  assert.equal(fetchCalls, RUNS, "each run retried the fetch naturally");
});

/** Fake HA history: the counter reports every 5 minutes across the window. */
function fullCounterHistory() {
  const states = [];
  for (let t = 0; t < WINDOW; t += BUCKET) {
    states.push({
      entity_id: counterEntity.haEntityId,
      state: String(100 + (t / BUCKET) * 0.5),
      last_updated_ts: (t + 1000) / 1000,
    });
  }
  return states;
}

test("partial run under the row budget: rows_written + rows_missing equals the detected missing buckets", async () => {
  const existing = existingRowsWithGap(); // 8 detected missing buckets (45-minute gap)
  const db = new FakeDb();
  const log = new FakeLog();

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    maxRowsPerRun: 4, // cuts the replay mid-gap
    loadRows: async () => existing,
    fetchHistory: async (entityId, startMs, endMs) => ({
      states: fullCounterHistory().filter(
        (s) => s.last_updated_ts * 1000 >= startMs && s.last_updated_ts * 1000 < endMs
      ),
    }),
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(r.outcome, "partial");
  // 4 states replayed: the left padding state is the baseline (no row),
  // so the replay writes buckets 10, 11 and (via the closing tick) 12.
  assert.equal(r.rowsWritten, 3);
  assert.equal(r.rowsMissing, 5);
  assert.equal(
    r.rowsWritten + r.rowsMissing,
    8,
    "written + missing equals the detected missing buckets"
  );
  assert.equal(r.detail.budget_exhausted_mid_gap, true, "the structured detail carries the flag");
  assert.equal(typeof r.detail.summary, "string");

  const entry = log.rows[0];
  assert.equal(entry.outcome, "partial");
  assert.equal(entry.runKind, "cli", "provenance: manual CLI run");
  assert.equal(entry.source, "ha-history", "provenance: plugin source");
  assert.equal(entry.rowsWritten, 3);
  assert.equal(entry.rowsMissing, 5);
  assert.equal(entry.rowsWritten + entry.rowsMissing, 8, "the log row books honestly");
});

// ---------------------------------------------------------------------------
// End to end: price repair with stubbed sources and a fake writer

const priceEntity = {
  haEntityId: "sensor.zonneplan_current_electricity_price",
  name: "Current electricity price",
  domain: "price",
  unit: "EUR/kWh",
  kind: "price",
};

const gasPriceEntity = {
  haEntityId: "sensor.zonneplan_gas_price_daily",
  name: "Gas price daily",
  domain: "price",
  unit: "EUR/m3",
  kind: "gas_price",
};

const PRICE_FROM = Date.parse("2026-02-10T00:00:00Z");
const PRICE_TO = Date.parse("2026-02-10T12:00:00Z");
const h = (n) => PRICE_FROM + n * HOUR;

/** Trivial stubs for the sample/counter plumbing (not used by these tests). */
const unusedStubs = {
  loadRows: async () => [],
  fetchHistory: async () => ({ states: [] }),
};

test("price repair end to end: forecast first, history fallback, honest bookkeeping", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  const existingHours = [h(0), h(1), h(2), h(3), h(4), h(5)]; // hours 6..11 missing

  let historyCalls = [];
  const { results } = await runBackfill({
    entities: [priceEntity],
    sinceMs: PRICE_FROM,
    now: () => PRICE_TO,
    timeZone: "Europe/Amsterdam",
    ...unusedStubs,
    loadPriceHours: async () => existingHours,
    // Forecast covers hours 6, 10, 11 (and 6 must win over history).
    fetchPriceForecast: async () => ({
      hours: [
        { startMs: h(6), priceEur: 0.6 },
        { startMs: h(10), priceEur: 0.3 },
        { startMs: h(11), priceEur: 0.31 },
      ],
    }),
    // History covers 6 (stale, must not win), 7 and 9. Hour 8 stays uncovered.
    fetchPriceHistory: async (fromMs, toMs) => {
      historyCalls.push([fromMs, toMs]);
      return {
        hours: [
          { startMs: h(6), priceEur: 0.26 },
          { startMs: h(7), priceEur: 0.21 },
          { startMs: h(9), priceEur: 0.23 },
        ],
      };
    },
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(r.kind, "price");
  assert.equal(r.outcome, "partial", "hour 8 is uncovered and recent: retried next run");
  assert.equal(r.rowsWritten, 5);
  assert.equal(r.rowsMissing, 1, "written + missing equals the 6 detected missing hours");
  assert.equal(r.source, "zonneplan-forecast", "the forecast supplied the hours");

  // Forecast priority: hour 6 has the forecast price, not the history one.
  assert.equal(db.prices.get(priceEntity.haEntityId + "@" + h(6)).price, 0.6);
  assert.equal(db.prices.get(priceEntity.haEntityId + "@" + h(6)).source, "zonneplan-backfill");
  assert.equal(db.prices.get(priceEntity.haEntityId + "@" + h(7)).price, 0.21);
  assert.equal(db.prices.get(priceEntity.haEntityId + "@" + h(9)).price, 0.23);
  assert.equal(db.prices.get(priceEntity.haEntityId + "@" + h(10)).price, 0.3);
  assert.equal(db.prices.get(priceEntity.haEntityId + "@" + h(11)).price, 0.31);
  assert.equal(db.prices.has(priceEntity.haEntityId + "@" + h(8)), false, "hour 8 stays missing");
  assert.equal(db.prices.size, 5, "no existing hour was touched");

  // The fallback is only fetched for the hours the forecast does not cover.
  assert.deepEqual(historyCalls, [[h(7), h(10)]]);

  const entry = log.rows[0];
  assert.equal(entry.kind, "price");
  assert.equal(entry.outcome, "partial");
  assert.equal(entry.runKind, "cli");
  assert.equal(entry.source, "zonneplan-forecast");
  assert.equal(entry.detail.hours_from_forecast, 3);
  assert.equal(entry.detail.hours_from_history, 2);
  assert.equal(entry.detail.hours_uncovered_recent, 1);
  assert.equal(entry.rangeStartMs, h(6));
  assert.equal(entry.rangeEndMs, h(12));
});

test("price repair end to end: both sources down is outcome source-down, never permanent", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  const existingHours = [h(0), h(1), h(2), h(3), h(4), h(5)];

  const { results } = await runBackfill({
    entities: [priceEntity],
    sinceMs: PRICE_FROM,
    now: () => PRICE_TO,
    recorderRetentionDays: 0, // even so: a down source never confirms inability
    ...unusedStubs,
    loadPriceHours: async () => existingHours,
    fetchPriceForecast: async () => ({ error: { code: "fetch-error", message: "HA down" } }),
    fetchPriceHistory: async () => ({ error: { code: "http-503", message: "HA down" } }),
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(r.outcome, "source-down");
  assert.equal(r.rowsWritten, 0);
  assert.equal(r.rowsMissing, 6);
  assert.equal(db.prices.size, 0);
  assert.equal(log.permanent.length, 0, "a merely down source is never registered permanent");
  assert.equal(log.rows[0].detail.forecast_error, "fetch-error");
  assert.equal(log.rows[0].detail.history_error, "http-503");
});

test("price repair end to end: uncovered hours older than the recorder retention are registered permanent, which stops the re-scan", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  const existingHours = [h(0), h(1), h(2), h(3), h(4), h(5)];

  // Sources answer but have nothing: the hours are confirmed uncoverable.
  // With retention 0 they are all older than the retention window.
  let forecastCalls = 0;
  let historyCalls = 0;
  const run = () =>
    runBackfill({
      entities: [priceEntity],
      sinceMs: PRICE_FROM,
      now: () => PRICE_TO,
      recorderRetentionDays: 0,
      ...unusedStubs,
      loadPriceHours: async () => existingHours,
      fetchPriceForecast: async () => {
        forecastCalls += 1;
        return { hours: [] };
      },
      fetchPriceHistory: async () => {
        historyCalls += 1;
        return { hours: [] };
      },
      dbWriter: db,
      log,
    });

  const run1 = await run();
  const r1 = run1.results[0];
  assert.equal(r1.outcome, "permanent", "nothing written, all hours registered permanent");
  assert.equal(r1.rowsWritten, 0);
  assert.equal(r1.rowsMissing, 6);
  assert.equal(log.permanent.length, 1, "one contiguous permanent range");
  assert.deepEqual(
    { startMs: log.permanent[0].rangeStartMs, endMs: log.permanent[0].rangeEndMs },
    { startMs: h(6), endMs: h(12) },
    "the range covers exactly the uncovered hours"
  );
  assert.equal(log.permanent[0].kind, "price");
  assert.equal(log.permanent[0].source, "zonneplan-history", "the fallback plugin confirmed");
  assert.equal(forecastCalls, 1);
  assert.equal(historyCalls, 1);

  // Second run: the permanent range excludes the hours from detection, so no
  // source is consulted at all and the run reports permanent, nothing done.
  const run2 = await run();
  const r2 = run2.results[0];
  assert.equal(r2.outcome, "permanent");
  assert.equal(r2.detail.hours_in_permanent_ranges, 6);
  assert.equal(r2.rowsWritten, 0);
  assert.equal(forecastCalls, 1, "the permanent range stops the re-scan");
  assert.equal(historyCalls, 1, "the permanent range stops the re-scan");
});

// ---------------------------------------------------------------------------
// End to end: gas price repair with a stubbed source and a fake writer

test("gas price repair end to end: missing days recovered, uncovered recent day retried", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  const tz = "Europe/Amsterdam";
  const from = Date.parse("2026-10-12T00:00:00Z"); // 02:00 local on 12 October
  const to = Date.parse("2026-10-15T00:00:00Z"); // 02:00 local on 15 October

  let gasCalls = [];
  const { results } = await runBackfill({
    entities: [gasPriceEntity],
    sinceMs: from,
    now: () => to,
    timeZone: tz,
    ...unusedStubs,
    loadGasDays: async () => ["2026-10-12", "2026-10-15"], // 13 and 14 October missing
    fetchGasHistory: async (cfg, startMs, endMs) => {
      gasCalls.push([cfg.haEntityId, startMs, endMs]);
      return { days: [{ day: "2026-10-13", priceEur: 1.90 }] };
    },
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(r.kind, "gas_price");
  assert.equal(r.outcome, "partial", "14 October is uncovered and recent");
  assert.equal(r.rowsWritten, 1);
  assert.equal(r.rowsMissing, 1);
  assert.equal(r.source, "zonneplan-history");

  assert.deepEqual(
    { price: db.gasDays.get("2026-10-13").price, source: db.gasDays.get("2026-10-13").source },
    { price: 1.90, source: "zonneplan-backfill" }
  );
  assert.equal(db.gasDays.has("2026-10-14"), false, "the uncovered day stays missing");
  // The history fetch covers the local days 13 and 14 October.
  assert.deepEqual(gasCalls, [
    [gasPriceEntity.haEntityId, Date.parse("2026-10-12T22:00:00Z"), Date.parse("2026-10-14T22:00:00Z")],
  ], "the entity id comes from config.json and the range is local-day aligned");

  const entry = log.rows[0];
  assert.equal(entry.kind, "gas_price");
  assert.equal(entry.outcome, "partial");
  assert.equal(entry.source, "zonneplan-history");
  assert.equal(entry.detail.days_from_history, 1);
  assert.equal(entry.detail.days_uncovered_recent, 1);
});
