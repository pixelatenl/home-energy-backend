// Manual verification of the backfill mechanism (see docs/backfill-design.md,
// section 11). This script does NOT touch a production database or Home
// Assistant: it constructs a synthetic gap over a fake in-memory writer with
// ON CONFLICT DO NOTHING semantics, serves a fake HA history, and runs the
// real orchestrator (runBackfill with the real IngestEngine and the real
// BackfillWriter). It then asserts:
//   - exactly the missing buckets are filled, with the source label
//     'backfill-ha-history' and correct deltas;
//   - existing 'ingest' rows are untouched;
//   - the backfill_log rows carry the correct outcomes (repaired, no-gap via
//     corroboration, source-down, partial under the row budget);
//   - dry-run writes nothing at all.
//
// Run from the ingest directory:  node scripts/verify-backfill.mjs
// Exit code 0 when every scenario passes.

import assert from "node:assert/strict";
import { BUCKET_MS } from "../src/buckets.mjs";
import { runBackfill } from "../src/backfill/repair.mjs";
import { BackfillWriter } from "../src/backfill/writer.mjs";

const BUCKET = BUCKET_MS;
const HOUR = 6 * 12 * BUCKET; // 1 hour in buckets, kept explicit for readability
const WINDOW = 4 * HOUR;

/** The entity under repair: a cumulative grid meter on 5-minute cadence. */
const counterEntity = {
  haEntityId: "sensor.grid_consumption_total",
  name: "Grid consumption total",
  domain: "grid",
  unit: "kWh",
  kind: "counter",
};

/** A second configured entity, used for the corroboration scenarios. */
const otherEntity = {
  haEntityId: "sensor.house_power",
  name: "House power",
  domain: "grid",
  unit: "kW",
  kind: "sample",
};

/**
 * Fake DbWriter with the real DbWriter contract: writes are keyed like the
 * database's unique index, ON CONFLICT DO NOTHING returns 0, an insert
 * returns 1. The fake is prefilled with the rows the live worker wrote.
 */
class FakeDb {
  constructor() {
    this.deltas = new Map(); // ts -> {delta, source}
    this.measurements = new Map(); // ts -> value
    this.ids = new Map([[counterEntity.haEntityId, 1], [otherEntity.haEntityId, 2]]);
  }
  writeEnergyDelta(meta, tsMs, deltaKwh, source = "ingest") {
    const key = meta.haEntityId + "@" + tsMs;
    if (this.deltas.has(key)) return Promise.resolve(0); // ON CONFLICT DO NOTHING
    this.deltas.set(key, { delta: deltaKwh, source });
    return Promise.resolve(1);
  }
  writeMeasurement(meta, tsMs, value) {
    const key = meta.haEntityId + "@" + tsMs;
    if (this.measurements.has(key)) return Promise.resolve(0);
    this.measurements.set(key, value);
    return Promise.resolve(1);
  }
  writePrice() {
    return Promise.resolve(0);
  }
  writeGasPrice() {
    return Promise.resolve(0);
  }
}

/** Fake log store: records inserts and permanent-registry answers. */
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
  async isPermanent() {
    return false;
  }
}

/**
 * Fake HA recorder history: states for the counter every 5 minutes across the
 * whole window, values 100 + 0.5 * bucketIndex. The outage being repaired is
 * in the EMS database, not in the recorder: exactly the backfill scenario.
 */
function fakeCounterHistory() {
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

function fetchHistoryReturning(statesByEntity) {
  return async (entityId, startMs, endMs) => {
    const states = (statesByEntity[entityId] ?? []).filter(
      (s) => s.last_updated_ts * 1000 >= startMs && s.last_updated_ts * 1000 < endMs
    );
    return { states };
  };
}

/** Prefill the fake database the way the live worker did: rows 0..9 and 18..35. */
function prefillWorkerRows(db) {
  const rows = [];
  for (let i = 0; i <= 9; i++) rows.push(i * BUCKET);
  for (let i = 18; i <= 35; i++) rows.push(i * BUCKET);
  for (const ts of rows) {
    db.deltas.set(counterEntity.haEntityId + "@" + ts, {
      delta: 0.5,
      source: "ingest",
    });
  }
  return rows;
}

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
    console.log(`PASS  ${name}`);
  } catch (e) {
    results.push({ name, ok: false, error: e.message });
    console.error(`FAIL  ${name}`);
    console.error(`      ${e.message}`);
  }
}

// ---------------------------------------------------------------------------
// Scenario 1: repair fills exactly the missing buckets of a synthetic gap.
// The worker wrote rows 0..9 and 18..35; buckets 10..17 (45 minutes) are
// missing. The repair must add exactly those 8 buckets.
{
  const db = new FakeDb();
  const existing = prefillWorkerRows(db);
  const log = new FakeLog();
  const run = await runBackfill({
    entities: [counterEntity],
    lookbackDays: 1,
    maxRowsPerRun: 5000,
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => existing,
    fetchHistory: fetchHistoryReturning({ [counterEntity.haEntityId]: fakeCounterHistory() }),
    dbWriter: db,
    log,
  });

  const r = run.results[0];
  check("scenario 1: outcome repaired with exactly 8 rows written", () => {
    assert.equal(r.outcome, "repaired");
    assert.equal(r.rowsWritten, 8);
  });
  check("scenario 1: exactly the missing buckets 10..17 are filled", () => {
    for (let i = 10; i <= 17; i++) {
      const row = db.deltas.get(counterEntity.haEntityId + "@" + i * BUCKET);
      assert.ok(row, `bucket ${i} must be filled`);
      assert.equal(row.source, "backfill-ha-history", "source label");
      assert.ok(Math.abs(row.delta - 0.5) < 1e-9, `delta of bucket ${i} must be 0.5`);
    }
    assert.equal(db.deltas.size, existing.length + 8, "no other rows added");
  });
  check("scenario 1: ingest rows are untouched", () => {
    for (const ts of existing) {
      const row = db.deltas.get(counterEntity.haEntityId + "@" + ts);
      assert.equal(row.source, "ingest", `row at ${ts} keeps its ingest label`);
      assert.ok(Math.abs(row.delta - 0.5) < 1e-9);
    }
  });
  check("scenario 1: one backfill_log row with outcome repaired", () => {
    assert.equal(log.rows.length, 1);
    assert.equal(log.rows[0].outcome, "repaired");
    assert.equal(log.rows[0].kind, "counter");
    assert.equal(log.rows[0].haEntityId, counterEntity.haEntityId);
    assert.equal(log.rows[0].rowsWritten, 8);
    assert.equal(log.rows[0].rangeStartMs, 10 * BUCKET, "range starts at the first missing bucket");
    assert.equal(log.rows[0].rangeEndMs, 18 * BUCKET, "range ends at the first existing bucket after the gap");
  });
}

// ---------------------------------------------------------------------------
// Scenario 2: the entity was silent (no states), but other configured
// entities do have states in the same range: corroboration says no-gap and
// nothing is written.
{
  const db = new FakeDb();
  const existing = prefillWorkerRows(db);
  const log = new FakeLog();
  const history = fakeCounterHistory().map((s) => ({ ...s, entity_id: otherEntity.haEntityId, state: "1.5" }));
  const run = await runBackfill({
    entities: [counterEntity, otherEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async (cfg) => (cfg.kind === "counter" ? existing : []),
    fetchHistory: fetchHistoryReturning({ [otherEntity.haEntityId]: history }),
    dbWriter: db,
    log,
  });

  check("scenario 2: corroborated silence is outcome no-gap, nothing written", () => {
    assert.equal(run.results[0].outcome, "no-gap");
    assert.equal(run.results[0].rowsWritten, 0);
    assert.equal(db.deltas.size, existing.length);
    assert.equal(log.rows.length, 2, "one log row per configured entity");
    assert.equal(log.rows[0].outcome, "no-gap");
  });
}

// ---------------------------------------------------------------------------
// Scenario 3: no configured entity has states in the gap range: the recorder
// was down, outcome source-down (to be retried on the next run).
{
  const db = new FakeDb();
  const existing = prefillWorkerRows(db);
  const log = new FakeLog();
  const run = await runBackfill({
    entities: [counterEntity, otherEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async (cfg) => (cfg.kind === "counter" ? existing : []),
    fetchHistory: fetchHistoryReturning({}),
    dbWriter: db,
    log,
  });

  check("scenario 3: recorder down is outcome source-down, nothing written", () => {
    assert.equal(run.results[0].outcome, "source-down");
    assert.equal(db.deltas.size, existing.length);
    assert.equal(log.rows[0].outcome, "source-down");
  });
}

// ---------------------------------------------------------------------------
// Scenario 4: the row budget stops the repair mid-gap: outcome partial.
{
  const db = new FakeDb();
  const existing = prefillWorkerRows(db);
  const log = new FakeLog();
  const run = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    maxRowsPerRun: 4,
    loadRows: async () => existing,
    fetchHistory: fetchHistoryReturning({ [counterEntity.haEntityId]: fakeCounterHistory() }),
    dbWriter: db,
    log,
  });

  check("scenario 4: a small row budget yields outcome partial and a bounded blast radius", () => {
    assert.equal(run.results[0].outcome, "partial");
    // 4 states replayed: the left padding state is the baseline (no row),
    // so the replay writes buckets 10, 11 and (via the closing tick) 12.
    assert.equal(run.results[0].rowsWritten, 3);
    assert.equal(db.deltas.size, existing.length + 3, "only the budgeted rows were added");
    assert.ok(/budget exhausted/.test(log.rows[0].detail), "the detail explains the budget stop");
  });
}

// ---------------------------------------------------------------------------
// Scenario 5: dry-run detects and fetches but writes nothing at all.
{
  const db = new FakeDb();
  const existing = prefillWorkerRows(db);
  const log = new FakeLog();
  const run = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    dryRun: true,
    loadRows: async () => existing,
    fetchHistory: fetchHistoryReturning({ [counterEntity.haEntityId]: fakeCounterHistory() }),
    dbWriter: db,
    log,
  });

  check("scenario 5: dry-run writes no rows and no log entries", () => {
    assert.equal(run.results[0].outcome, "repaired"); // would have repaired
    assert.equal(run.results[0].rowsWritten, 0);
    assert.equal(run.results[0].statesReplayed, 10, "states would have been replayed");
    assert.equal(db.deltas.size, existing.length, "no data rows written");
    assert.equal(log.rows.length, 0, "no backfill_log rows written");
  });
}

// ---------------------------------------------------------------------------
// Scenario 6: the BackfillWriter labels the raw writer with the backfill
// source and counts inserts (engine writer interface).
{
  const db = new FakeDb();
  const bf = new BackfillWriter(db);
  const written1 = await (async () => {
    bf.writeEnergyDelta(counterEntity, BUCKET, 0.5);
    bf.writeEnergyDelta(counterEntity, BUCKET, 0.9); // conflict: existing wins
    return bf.flush();
  })();
  check("scenario 6: BackfillWriter injects the source label and counts inserts", () => {
    assert.equal(written1, 1);
    assert.equal(db.deltas.get(counterEntity.haEntityId + "@" + BUCKET).source, "backfill-ha-history");
  });
}

const failed = results.filter((r) => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exitCode = failed.length === 0 ? 0 : 1;
