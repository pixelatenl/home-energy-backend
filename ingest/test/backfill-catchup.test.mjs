// Unit tests for the class 2 catch-up detection (catchup.mjs pure detector
// plus its read-only integration in runBackfill). Run with: node --test test/
//
// The detector pins (docs/backfill-design.md, section 7.3): a delta row is
// attribution-suspect when |delta_kwh| exceeds the factor times the median
// |delta| of the same entity over the trailing 7 days, with a minimum of 5
// samples in the median window; without enough samples, never flag. The
// integration is report only: no writes to energy_delta, no modification of
// existing rows, no redistribution.

import test from "node:test";
import assert from "node:assert/strict";
import {
  flagAttributionSuspects,
  medianOf,
  CATCHUP_WINDOW_MS,
  DEFAULT_CATCHUP_FACTOR,
  MIN_CATCHUP_SAMPLES,
} from "../src/backfill/catchup.mjs";
import { runBackfill } from "../src/backfill/repair.mjs";

const BUCKET = 5 * 60 * 1000;
const HOUR = 60 * 60 * 1000;
const DAY = 24 * HOUR;
const WINDOW = 4 * HOUR;

const counterEntity = {
  haEntityId: "sensor.grid_consumption_total",
  name: "Grid consumption total",
  domain: "grid",
  unit: "kWh",
  kind: "counter",
};

// ---------------------------------------------------------------------------
// Pure detector

test("medianOf: odd count takes the middle, even count the average of the middle two", () => {
  assert.equal(medianOf([3, 1, 2]), 2);
  assert.equal(medianOf([4, 1, 3, 2]), 2.5);
  assert.equal(medianOf([7]), 7);
});

test("catch-up jump is flagged with its median context", () => {
  const deltas = [];
  for (let i = 0; i < 6; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: 0.5 });
  deltas.push({ tsMs: 6 * BUCKET, deltaKwh: 20 }); // offline meter reports one cumulative jump

  const suspects = flagAttributionSuspects(deltas);
  assert.equal(suspects.length, 1);
  assert.equal(suspects[0].tsMs, 6 * BUCKET);
  assert.equal(suspects[0].deltaKwh, 20);
  assert.equal(suspects[0].medianAbsDelta, 0.5);
  assert.equal(suspects[0].windowSamples, 6);

  // The row after the jump is not flagged: the median is robust to one spike.
  deltas.push({ tsMs: 7 * BUCKET, deltaKwh: 0.5 });
  assert.equal(flagAttributionSuspects(deltas).length, 1);
});

test("threshold is strict: exactly factor times the median is not suspect", () => {
  const deltas = [];
  for (let i = 0; i < 5; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: 1 });
  deltas.push({ tsMs: 5 * BUCKET, deltaKwh: DEFAULT_CATCHUP_FACTOR }); // == 5 * median 1

  assert.equal(flagAttributionSuspects(deltas).length, 0, "exceeds means strictly greater");
  deltas[5].deltaKwh = DEFAULT_CATCHUP_FACTOR * 1.0001;
  assert.equal(flagAttributionSuspects(deltas).length, 1);
});

test("fewer than the minimum samples in the median window: never flag", () => {
  const deltas = [];
  for (let i = 0; i < MIN_CATCHUP_SAMPLES - 1; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: 0.5 });
  deltas.push({ tsMs: (MIN_CATCHUP_SAMPLES - 1) * BUCKET, deltaKwh: 1000 });

  assert.equal(flagAttributionSuspects(deltas).length, 0, "no verdict beats a false alarm");
  // Exactly the minimum does flag when the jump exceeds the threshold.
  deltas.push({ tsMs: (MIN_CATCHUP_SAMPLES - 1) * BUCKET + BUCKET, deltaKwh: 1000 });
  assert.equal(flagAttributionSuspects(deltas).length, 1);
});

test("the median window is the trailing 7 days, boundary included, row itself excluded", () => {
  const T = 100 * DAY;
  const inside = [
    { tsMs: T - CATCHUP_WINDOW_MS, deltaKwh: 0.4 }, // exactly 7 days back: inside
    { tsMs: T - 3 * DAY, deltaKwh: 0.4 },
    { tsMs: T - 2 * DAY, deltaKwh: 0.4 },
    { tsMs: T - DAY, deltaKwh: 0.4 },
    { tsMs: T - BUCKET, deltaKwh: 0.4 },
  ];
  const jump = { tsMs: T, deltaKwh: 8 }; // > 5 * 0.4 = 2

  const flagged = flagAttributionSuspects([...inside, jump]);
  assert.equal(flagged.length, 1, "five samples in the window: the jump is flagged");
  assert.equal(flagged[0].windowSamples, 5);

  // Move the oldest sample one bucket out of the window: only four remain.
  const outside = inside.map((d, i) => (i === 0 ? { ...d, tsMs: d.tsMs - BUCKET } : d));
  assert.equal(flagAttributionSuspects([...outside, jump]).length, 0, "four samples: never flag");
});

test("a zero median (a silent week) makes every non-zero delta suspect", () => {
  const deltas = [];
  for (let i = 0; i < 5; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: 0 });
  deltas.push({ tsMs: 5 * BUCKET, deltaKwh: 0.1 });

  const suspects = flagAttributionSuspects(deltas);
  assert.equal(suspects.length, 1);
  assert.equal(suspects[0].medianAbsDelta, 0);
  // A zero delta after a silent week is not suspect: it exceeds nothing.
  deltas[5].deltaKwh = 0;
  assert.equal(flagAttributionSuspects(deltas).length, 0);
});

test("input order does not matter; the suspects come out ascending", () => {
  const deltas = [];
  for (let i = 0; i < 5; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: 0.5 });
  deltas.push({ tsMs: 5 * BUCKET, deltaKwh: 20 });
  deltas.push({ tsMs: 6 * BUCKET, deltaKwh: 0.5 });
  deltas.push({ tsMs: 7 * BUCKET, deltaKwh: 21 });
  for (let i = 0; i < 6; i++) deltas.push({ tsMs: (8 + i) * BUCKET, deltaKwh: 0.5 });

  const shuffled = [...deltas].sort(() => Math.random() - 0.5);
  const suspects = flagAttributionSuspects(shuffled);
  assert.deepEqual(suspects.map((s) => s.tsMs), [5 * BUCKET, 7 * BUCKET]);
});

// ---------------------------------------------------------------------------
// Integration: report only, inside the repair run

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

/** Fake log store. */
class FakeLog {
  constructor() {
    this.rows = [];
  }
  async insert(entry) {
    this.rows.push(entry);
  }
  async markPermanent(entry) {
    this.rows.push(entry);
  }
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

const fetchGapHistory = async (entityId, startMs, endMs) => ({
  states: fullCounterHistory().filter(
    (s) => s.last_updated_ts * 1000 >= startMs && s.last_updated_ts * 1000 < endMs
  ),
});

test("integration: suspect deltas in the lookback window become one attribution-suspect log row, nothing else", async () => {
  const db = new FakeDb();
  const log = new FakeLog();

  // The suspect row already exists in energy_delta (written by the worker).
  db.deltas.set(counterEntity.haEntityId + "@" + 5 * BUCKET, { delta: 30, source: "ingest" });
  // Delta rows as the SQL loader would return them: uniform 0.5, one jump.
  const deltas = [];
  for (let i = 0; i <= 9; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: i === 5 ? 30 : 0.5 });
  for (let i = 18; i <= 35; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: 0.5 });

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => existingRowsWithGap(),
    loadDeltas: async () => deltas,
    fetchHistory: fetchGapHistory,
    dbWriter: db,
    log,
  });

  // The repair itself is untouched: the gap is repaired, 8 rows labelled.
  const repair = results[0];
  assert.equal(repair.outcome, "repaired");
  assert.equal(repair.rowsWritten, 8);

  // The catch-up scan adds its own report result.
  assert.equal(results.length, 2, "one repair result plus one catch-up report");
  const catchup = results[1];
  assert.equal(catchup.outcome, "attribution-suspect");
  assert.equal(catchup.kind, "counter");
  assert.equal(catchup.source, "none", "report only: no plugin supplied rows");
  assert.equal(catchup.rowsWritten, 0, "no rows are written for a suspect");
  assert.equal(catchup.rowsMissing, 1);
  assert.equal(catchup.rangeStartMs, 5 * BUCKET);
  assert.equal(catchup.rangeEndMs, 6 * BUCKET);
  assert.equal(catchup.detail.suspects, 1);
  assert.equal(catchup.detail.suspect_rows[0].delta_kwh, 30);
  assert.equal(catchup.detail.suspect_rows[0].median_abs_delta, 0.5);
  assert.equal(catchup.detail.suspect_rows[0].window_samples, 5);

  // The log carries both rows; the catch-up row books zero writes.
  assert.equal(log.rows.length, 2);
  const entry = log.rows[1];
  assert.equal(entry.outcome, "attribution-suspect");
  assert.equal(entry.source, "none");
  assert.equal(entry.rowsWritten, 0);
  assert.equal(entry.rowsMissing, 1);
  assert.equal(entry.runKind, "cli");

  // Strictly read-only on the data tables: the suspect row is untouched
  // (still the ingest label and its original value) and the only new rows
  // are the repair's gap buckets.
  assert.deepEqual(db.deltas.get(counterEntity.haEntityId + "@" + 5 * BUCKET), {
    delta: 30,
    source: "ingest",
  });
  assert.equal(db.deltas.size, 9, "the pre-existing suspect row plus 8 repaired gap rows");
  for (const key of db.deltas.keys()) {
    const ts = Number(key.split("@")[1]);
    assert.ok(
      ts === 5 * BUCKET || (ts >= 10 * BUCKET && ts < 18 * BUCKET),
      "no row outside the repaired gap was written"
    );
  }
});

test("integration: suspects before the lookback window are context, never reported", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  // A jump at bucket 5, five quiet samples before it: flagged by the pure
  // detector, but the window starts at bucket 10.
  const deltas = [];
  for (let i = 0; i <= 5; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: i === 5 ? 30 : 0.5 });

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 10 * BUCKET,
    now: () => WINDOW,
    loadRows: async () => existingRowsWithGap(),
    loadDeltas: async () => deltas,
    fetchHistory: fetchGapHistory,
    dbWriter: db,
    log,
  });

  assert.equal(results.length, 1, "no catch-up report for rows outside the window");
  assert.equal(results[0].outcome, "no-gap");
  assert.equal(log.rows.length, 1);
  assert.ok(log.rows.every((r) => r.outcome !== "attribution-suspect"));
});

test("integration: no suspects means no extra result and no extra log row", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  const deltas = [];
  for (let i = 0; i <= 9; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: 0.5 });

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => existingRowsWithGap(),
    loadDeltas: async () => deltas,
    fetchHistory: fetchGapHistory,
    dbWriter: db,
    log,
  });

  assert.equal(results.length, 1, "silence is not news: no attribution-suspect result");
  assert.equal(log.rows.length, 1);
  assert.equal(log.rows[0].outcome, "repaired");
});

test("integration: dry-run reports the suspects but writes no log row at all", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  const deltas = [];
  for (let i = 0; i <= 9; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: i === 5 ? 30 : 0.5 });

  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => existingRowsWithGap(),
    loadDeltas: async () => deltas,
    fetchHistory: fetchGapHistory,
    dbWriter: db,
    log,
    dryRun: true,
  });

  assert.equal(results[1]?.outcome, "attribution-suspect", "detection still reports in dry-run");
  assert.equal(log.rows.length, 0, "dry-run performs no writes, backfill_log included");
  assert.equal(db.deltas.size, 0);
});

test("integration: the factor comes from the option (EMS_BACKFILL_CATCHUP_FACTOR)", async () => {
  const db = new FakeDb();
  const log = new FakeLog();
  // 0.5 kWh buckets, one 2.0 kWh jump: suspect at factor 3, normal at factor 5.
  const deltas = [];
  for (let i = 0; i <= 9; i++) deltas.push({ tsMs: i * BUCKET, deltaKwh: i === 5 ? 2.0 : 0.5 });

  const run = (catchupFactor) =>
    runBackfill({
      entities: [counterEntity],
      sinceMs: 0,
      now: () => WINDOW,
      catchupFactor,
      loadRows: async () => existingRowsWithGap(),
      loadDeltas: async () => deltas,
      fetchHistory: fetchGapHistory,
      dbWriter: db,
      log,
    });

  assert.equal((await run(5)).results[1], undefined, "2.0 <= 5 x 0.5: not suspect");
  const tight = await run(3);
  assert.equal(tight.results[1].outcome, "attribution-suspect", "2.0 > 3 x 0.5: suspect");
  assert.equal(tight.results[1].detail.catchup_factor, 3);
});
