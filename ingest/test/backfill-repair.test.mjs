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
const WINDOW = 4 * 60 * 60 * 1000;

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
  }
  writeEnergyDelta(meta, tsMs, deltaKwh, source = "ingest") {
    const key = meta.haEntityId + "@" + tsMs;
    if (this.deltas.has(key)) return Promise.resolve(0);
    this.deltas.set(key, { delta: deltaKwh, source });
    return Promise.resolve(1);
  }
  writeMeasurement() {
    return Promise.resolve(0);
  }
}

/** Fake log store: records every insert and every permanent registration. */
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
