// Honesty-rule pinning tests for the backfill mechanism (node --test).
//
// These six tests pin the honesty rules of docs/backfill-design.md,
// section 9, end to end through the real orchestrator (runBackfill) with
// injected fakes: no database, no Home Assistant, no I/O.
//
// 1. fabricated value: a source with no data for a gap writes nothing and
//    reports source-down, never invented rows.
// 2. ingest wins: a replay that overlaps existing ingest rows writes
//    nothing new for them (ON CONFLICT DO NOTHING path).
// 3. baseline: a counter replay writes no delta for the first bucket of
//    the range (the IngestEngine baseline rule).
// 4. labelling: every backfill write carries the source label.
// 5. silence: an entity with no rows in the lookback window is no-gap,
//    with no repair attempt at all.
// 6. blast radius: max rows per run caps writes and yields outcome partial.

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

const priceEntity = {
  haEntityId: "sensor.zonneplan_current_electricity_price",
  name: "Current electricity price",
  domain: "price",
  unit: "EUR/kWh",
  kind: "price",
};

/** Fake DbWriter: counts inserts, ON CONFLICT DO NOTHING returns 0. */
class FakeDb {
  constructor() {
    this.deltas = new Map(); // key -> {delta, source}
    this.prices = new Map(); // key -> {price, source}
    this.measurements = new Map();
    this.deltaWrites = []; // every write attempt, conflicts included
  }
  writeEnergyDelta(meta, tsMs, deltaKwh, source = "ingest") {
    const key = meta.haEntityId + "@" + tsMs;
    this.deltaWrites.push({ key, delta: deltaKwh, source });
    if (this.deltas.has(key)) return Promise.resolve(0); // ON CONFLICT DO NOTHING
    this.deltas.set(key, { delta: deltaKwh, source });
    return Promise.resolve(1);
  }
  writePrice(meta, tsMs, priceEur) {
    const key = meta.haEntityId + "@" + tsMs;
    if (this.prices.has(key)) return Promise.resolve(0);
    this.prices.set(key, { price: priceEur, source: meta.source ?? "zonneplan" });
    return Promise.resolve(1);
  }
  writeMeasurement(meta, tsMs, value) {
    const key = meta.haEntityId + "@" + tsMs;
    if (this.measurements.has(key)) return Promise.resolve(0);
    this.measurements.set(key, value);
    return Promise.resolve(1);
  }
}

/** Fake log store. */
class FakeLog {
  constructor() {
    this.rows = [];
  }
  async insert(entry) {
    this.rows.push(entry);
  }
  async markPermanent() {}
  async isPermanent() {
    return false;
  }
  async permanentRanges() {
    return [];
  }
}

/** Worker rows with a synthetic 45-minute gap: buckets 0..9 and 18..35. */
function existingRowsWithGap() {
  const rows = [];
  for (let i = 0; i <= 9; i++) rows.push(i * BUCKET);
  for (let i = 18; i <= 35; i++) rows.push(i * BUCKET);
  return rows;
}

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

/** fetchHistory stub that answers with the states inside the asked range. */
const fetchGapHistory = async (entityId, startMs, endMs) => ({
  states: fullCounterHistory().filter(
    (s) => s.last_updated_ts * 1000 >= startMs && s.last_updated_ts * 1000 < endMs
  ),
});

// ---------------------------------------------------------------------------
// 1. Fabricated value

test("honesty: a source returning no data for a gap writes nothing and reports source-down, never invented rows", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  let fetchCalls = 0;

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => existingRowsWithGap(),
    fetchHistory: async () => {
      fetchCalls += 1;
      return { states: [] }; // the recorder answers, but has nothing
    },
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(fetchCalls, 1, "the gap was fetched");
  assert.ok(
    r.outcome === "source-down" || r.outcome === "permanent",
    `the outcome is a no-data outcome, got ${r.outcome}`
  );
  assert.equal(r.outcome, "source-down", "one configured entity: corroboration is impossible, retry");
  assert.equal(r.rowsWritten, 0);
  assert.equal(r.rowsMissing, 8, "the 8 missing buckets stay unwritten and booked");
  assert.equal(db.deltas.size, 0, "no delta row was invented");
  assert.equal(db.measurements.size, 0, "no measurement row was invented");
  assert.equal(db.deltaWrites.length, 0, "not a single write was attempted");
  assert.equal(log.rows.length, 1);
  assert.equal(log.rows[0].outcome, "source-down");
  assert.equal(log.rows[0].rowsWritten, 0);
});

// ---------------------------------------------------------------------------
// 2. Ingest wins (ON CONFLICT DO NOTHING)

test("honesty: replaying a range that overlaps existing ingest rows writes nothing new for them", async () => {
  const db = new FakeDb();
  const log = new FakeLog();

  // The worker wrote bucket 12 after detection (a live-ingest race): the
  // replay hits an existing ingest row and must lose against it.
  const existingKey = counterEntity.haEntityId + "@" + 12 * BUCKET;
  db.deltas.set(existingKey, { delta: 1.234, source: "ingest" });

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => existingRowsWithGap(), // detection still sees bucket 12 as missing
    fetchHistory: fetchGapHistory,
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(r.outcome, "repaired");
  assert.equal(r.rowsWritten, 7, "only the 7 truly missing buckets are counted as written");
  assert.equal(r.rowsMissing, 1, "the conflicting bucket stays missing in the bookkeeping");
  assert.equal(
    r.rowsWritten + r.rowsMissing,
    8,
    "written + missing equals the detected missing buckets"
  );

  // The existing ingest row is untouched: same value, same label, not rewritten.
  assert.deepEqual(db.deltas.get(existingKey), { delta: 1.234, source: "ingest" });
  assert.equal(
    db.deltaWrites.filter((w) => w.key === existingKey).length,
    1,
    "the replay attempted the write exactly once and ON CONFLICT DO NOTHING discarded it"
  );
  assert.ok(
    db.deltaWrites.every((w) => w.source === "backfill-ha-history"),
    "attempted writes carry the backfill label; the surviving row keeps its ingest label"
  );
  assert.equal(log.rows[0].rowsWritten, 7, "the log row books only the real inserts");
});

// ---------------------------------------------------------------------------
// 3. Baseline rule

test("honesty: counter replay over a repaired range writes no delta for the first bucket of the range", async () => {
  const db = new FakeDb();
  const log = new FakeLog();

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => existingRowsWithGap(),
    fetchHistory: fetchGapHistory,
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(r.outcome, "repaired");
  assert.equal(r.rowsWritten, 8, "exactly the 8 missing buckets, never the padding");

  // The replay's first bucket is the left padding (bucket 9, the reading
  // before the gap): the engine takes it as the baseline and writes no
  // delta for it. Its row already exists and stays untouched.
  assert.equal(db.deltas.has(counterEntity.haEntityId + "@" + 9 * BUCKET), false);
  assert.equal(
    db.deltaWrites.some((w) => w.key === counterEntity.haEntityId + "@" + 9 * BUCKET),
    false,
    "the baseline bucket is never even attempted"
  );
  // The right padding bucket (18) already exists too: no write for it either.
  assert.equal(db.deltas.has(counterEntity.haEntityId + "@" + 18 * BUCKET), false);

  // The first repaired delta derives from the reading before the gap
  // (value at bucket 10 minus value at bucket 9), not from zero, not from
  // an invented baseline, and no energy was shifted into it.
  const first = db.deltas.get(counterEntity.haEntityId + "@" + 10 * BUCKET);
  assert.equal(first.delta, 0.5); // 105.0 - 104.5 from the fixture states
  const written = [...db.deltas.keys()]
    .map((k) => Number(k.split("@")[1]))
    .sort((a, b) => a - b);
  assert.deepEqual(
    written,
    Array.from({ length: 8 }, (_, i) => (10 + i) * BUCKET),
    "exactly the gap buckets 10..17, in the engine's own semantics"
  );
});

// ---------------------------------------------------------------------------
// 4. Source labelling

test("honesty: every backfill write carries the source label", async () => {
  const db = new FakeDb();
  const log = new FakeLog();

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => existingRowsWithGap(),
    fetchHistory: fetchGapHistory,
    dbWriter: db,
    log,
  });

  assert.equal(results[0].rowsWritten, 8);
  assert.equal(db.deltas.size, 8);
  assert.ok(
    [...db.deltas.values()].every((v) => v.source === "backfill-ha-history"),
    "every written energy_delta row is labelled backfill-ha-history"
  );
  assert.ok(
    db.deltaWrites.every((w) => w.source === "backfill-ha-history"),
    "no write ever goes out under the default ingest label"
  );
  assert.equal(log.rows[0].source, "ha-history", "the log row carries the plugin provenance");

  // The price path labels its writes too (zonneplan-backfill).
  const priceFrom = Date.parse("2026-02-10T00:00:00Z");
  const priceTo = Date.parse("2026-02-10T12:00:00Z");
  const h = (n) => priceFrom + n * HOUR;
  const db2 = new FakeDb();
  const log2 = new FakeLog();
  await runBackfill({
    entities: [priceEntity],
    sinceMs: priceFrom,
    now: () => priceTo,
    loadRows: async () => [],
    fetchHistory: async () => ({ states: [] }),
    loadPriceHours: async () => [h(0), h(1), h(2), h(3), h(4), h(5)], // hours 6..11 missing
    fetchPriceForecast: async () => ({ hours: [{ startMs: h(6), priceEur: 0.32 }] }),
    fetchPriceHistory: async () => ({ hours: [{ startMs: h(7), priceEur: 0.21 }] }),
    dbWriter: db2,
    log: log2,
  });
  assert.ok(db2.prices.size > 0);
  assert.ok(
    [...db2.prices.values()].every((v) => v.source === "zonneplan-backfill"),
    "every written price row is labelled zonneplan-backfill"
  );
});

// ---------------------------------------------------------------------------
// 5. Silence

test("honesty: an entity with no rows in the lookback window is no-gap, with no repair attempt", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  let fetchCalls = 0;

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => [], // no rows at all in the lookback window
    fetchHistory: async () => {
      fetchCalls += 1;
      return { states: [] };
    },
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(r.outcome, "no-gap");
  assert.equal(r.rowsWritten, 0);
  assert.equal(r.rowsMissing, 0);
  assert.equal(fetchCalls, 0, "silence is not data loss: no source is consulted");
  assert.equal(db.deltas.size, 0, "nothing is written for an entity that never reported");
  assert.equal(log.rows.length, 1);
  assert.equal(log.rows[0].outcome, "no-gap");
  assert.equal(log.rows[0].rowsWritten, 0);
});

// ---------------------------------------------------------------------------
// 6. Blast radius

test("honesty: max rows per run caps the writes and yields outcome partial, the next run continues", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  const existing = existingRowsWithGap();

  // Run 1: a 4-state budget cuts the replay mid-gap.
  const run1 = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    maxRowsPerRun: 4,
    loadRows: async () => existing,
    fetchHistory: fetchGapHistory,
    dbWriter: db,
    log,
  });

  const r1 = run1.results[0];
  assert.equal(r1.outcome, "partial", "a capped run is honest about being incomplete");
  assert.ok(r1.rowsWritten <= 4, "the run never writes more than its budget");
  assert.equal(r1.rowsWritten, 3, "the baseline state writes no row: 3 of the 4 budgeted states");
  assert.equal(r1.rowsMissing, 5);
  assert.equal(r1.rowsWritten + r1.rowsMissing, 8, "written + missing equals the detected buckets");
  assert.equal(r1.detail.budget_exhausted_mid_gap, true);
  assert.equal(log.rows[0].outcome, "partial");
  assert.equal(db.deltas.size, 3);

  // Run 2: the loader sees the rows run 1 wrote, the run continues where
  // run 1 stopped and finishes the gap within the same honesty rules.
  const run2 = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => {
      const written = [...db.deltas.keys()].map((k) => Number(k.split("@")[1]));
      return [...existing, ...written].sort((a, b) => a - b);
    },
    fetchHistory: fetchGapHistory,
    dbWriter: db,
    log,
  });

  const r2 = run2.results[0];
  assert.equal(r2.outcome, "repaired", "the remaining gap is finished on the next run");
  assert.equal(r2.rowsWritten, 5);
  assert.equal(r2.rowsMissing, 0);
  assert.equal(db.deltas.size, 8, "after both runs exactly the 8 missing buckets exist");
});
