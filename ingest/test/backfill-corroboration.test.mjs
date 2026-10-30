// Corroboration registration tests: corroborated entity silence registers
// the gap range through the permanent-range registry with source
// 'corroboration' (docs/backfill-design.md, rule 7), so later runs skip it.
// Pinned here, end to end through the real orchestrator with fakes:
//
// - a silent gap (only padding states in the fetch) corroborated by another
//   entity is outcome no-gap and registered exactly once;
// - a follow-up run skips the registered range without any fetch;
// - without corroboration the gap stays source-down, is never registered
//   and is fetched again on the next run;
// - a gap with in-gap states still replays exactly as before.

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

const otherEntity = {
  haEntityId: "sensor.house_power",
  name: "House power",
  domain: "grid",
  unit: "kW",
  kind: "sample",
};

/** Fake DbWriter: counts inserts, ON CONFLICT DO NOTHING returns 0. */
class FakeDb {
  constructor() {
    this.deltas = new Map(); // key -> {delta, source}
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
      .filter((p) => p.haEntityId === haEntityId && p.kind === kind && p.rangeStartMs < toMs && p.rangeEndMs > fromMs)
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

/** A full grid of rows: the entity has no gaps of its own. */
function fullRows() {
  const rows = [];
  for (let i = 0; i <= 35; i++) rows.push(i * BUCKET);
  return rows;
}

/** One recorded state, at the given bucket index. */
function st(entityId, bucketIndex, value) {
  return {
    entity_id: entityId,
    state: String(value),
    last_updated_ts: (bucketIndex * BUCKET + 1000) / 1000,
  };
}

/** fetchHistory stub: serves the states of each entity inside the range. */
function fetchHistoryReturning(statesByEntity) {
  return async (entityId, startMs, endMs) => ({
    states: (statesByEntity[entityId] ?? []).filter(
      (s) => s.last_updated_ts * 1000 >= startMs && s.last_updated_ts * 1000 < endMs
    ),
  });
}

/** Count fetches per entity, optionally failing hard (never called). */
function countingFetch(fetchFn) {
  const counts = new Map();
  const wrapped = async (entityId, startMs, endMs) => {
    counts.set(entityId, (counts.get(entityId) ?? 0) + 1);
    return fetchFn(entityId, startMs, endMs);
  };
  wrapped.counts = counts;
  return wrapped;
}

/** The standard run over the two entities with the given fetch and log. */
function run({ statesByEntity, log, fetchFn }) {
  return runBackfill({
    entities: [counterEntity, otherEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async (cfg) => (cfg.kind === "counter" ? existingRowsWithGap() : fullRows()),
    fetchHistory: fetchFn ?? fetchHistoryReturning(statesByEntity ?? {}),
    dbWriter: new FakeDb(),
    log,
  });
}

test("corroboration: a silent gap (only padding states) is no-gap and registered once with source corroboration", async () => {
  const log = new FakeLog();
  const db = new FakeDb();

  // The counter's fetch answers with padding states only (the readings
  // before and after the gap, which the recorder always has); the other
  // entity does have states inside the gap: the recorder was up.
  const { results } = await runBackfill({
    entities: [counterEntity, otherEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async (cfg) => (cfg.kind === "counter" ? existingRowsWithGap() : fullRows()),
    fetchHistory: fetchHistoryReturning({
      [counterEntity.haEntityId]: [st(counterEntity.haEntityId, 9, 104.5), st(counterEntity.haEntityId, 18, 109)],
      [otherEntity.haEntityId]: [st(otherEntity.haEntityId, 12, 1.5)],
    }),
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(r.outcome, "no-gap", "corroborated silence is not data loss");
  assert.equal(r.rowsWritten, 0, "the padding states trigger no replay");
  assert.equal(r.detail.gaps_silent, 1);
  assert.equal(r.detail.gaps_silent_registered, 1);
  assert.ok(
    r.detail.summary.includes("registered"),
    "the summary mentions the registration"
  );
  assert.equal(db.deltas.size, 0, "nothing is written for a silent gap");

  // Registered exactly once, with the exact gap range and provenance.
  assert.equal(log.permanent.length, 1);
  const entry = log.permanent[0];
  assert.equal(entry.haEntityId, counterEntity.haEntityId);
  assert.equal(entry.kind, "counter");
  assert.equal(entry.source, "corroboration");
  assert.equal(entry.runKind, "cli");
  assert.equal(entry.rangeStartMs, 10 * BUCKET);
  assert.equal(entry.rangeEndMs, 18 * BUCKET);
  assert.equal(entry.rowsMissing, 8);
  assert.equal(
    entry.detail.summary,
    "corroborated entity silence: no numeric states in the gap, other entities have states; nothing to recover"
  );

  // The other entity has no gaps of its own and no registration.
  assert.equal(results[1].outcome, "no-gap");
  assert.equal(log.rows.length, 2, "one repair log row per entity");
});

test("corroboration: a follow-up run skips the registered range without any fetch", async () => {
  const log = new FakeLog();
  const fetch = countingFetch(
    fetchHistoryReturning({
      [counterEntity.haEntityId]: [st(counterEntity.haEntityId, 9, 104.5), st(counterEntity.haEntityId, 18, 109)],
      [otherEntity.haEntityId]: [st(otherEntity.haEntityId, 12, 1.5)],
    })
  );

  const first = await run({ log, fetchFn: fetch });
  assert.equal(first.results[0].outcome, "no-gap");
  assert.equal(log.permanent.length, 1);
  assert.equal(fetch.counts.get(counterEntity.haEntityId), 1, "run 1 fetched the gap");

  // Second run: the registered range is permanent, so the gap is skipped
  // before any fetch, and no new registration happens.
  const second = await run({ log, fetchFn: fetch });
  const r = second.results[0];
  assert.equal(r.outcome, "permanent");
  assert.equal(r.rowsWritten, 0);
  assert.equal(r.detail.gaps_permanent, 1, "the gap counts as in a permanent range");
  assert.equal(
    fetch.counts.get(counterEntity.haEntityId),
    1,
    "the registered range is never fetched again"
  );
  assert.equal(log.permanent.length, 1, "no second registration");
  assert.equal(log.rows.filter((x) => x.outcome === "permanent").length, 1);
});

test("corroboration: without corroboration the gap stays source-down, is never registered and is refetched", async () => {
  const log = new FakeLog();
  const fetch = countingFetch(
    fetchHistoryReturning({
      [counterEntity.haEntityId]: [st(counterEntity.haEntityId, 9, 104.5), st(counterEntity.haEntityId, 18, 109)],
      // No other entity has states in the gap range either: the recorder
      // may have been down, which confirms nothing.
      [otherEntity.haEntityId]: [],
    })
  );

  const first = await run({ log, fetchFn: fetch });
  const r1 = first.results[0];
  assert.equal(r1.outcome, "source-down");
  assert.equal(r1.rowsWritten, 0);
  assert.equal(r1.detail.gaps_source_down, 1);
  assert.equal(r1.detail.gaps_silent_registered, 0);
  assert.equal(log.permanent.length, 0, "a source outage never registers a range");
  assert.equal(fetch.counts.get(counterEntity.haEntityId), 1);

  // The follow-up run retries naturally: the gap is fetched again.
  const second = await run({ log, fetchFn: fetch });
  assert.equal(second.results[0].outcome, "source-down");
  assert.equal(fetch.counts.get(counterEntity.haEntityId), 2, "the unconfirmed gap is refetched");
  assert.equal(log.permanent.length, 0);
});

test("corroboration: a gap with in-gap states replays exactly as before", async () => {
  const log = new FakeLog();
  const db = new FakeDb();

  // Padding states plus two states inside the gap (buckets 12 and 15): the
  // replay writes only the state-containing buckets; the silent buckets
  // inside the gap get no row (the engine never invents rows).
  const { results } = await runBackfill({
    entities: [counterEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async () => existingRowsWithGap(),
    fetchHistory: fetchHistoryReturning({
      [counterEntity.haEntityId]: [
        st(counterEntity.haEntityId, 9, 104.5),
        st(counterEntity.haEntityId, 12, 106),
        st(counterEntity.haEntityId, 15, 107.5),
        st(counterEntity.haEntityId, 18, 109),
      ],
    }),
    dbWriter: db,
    log,
  });

  const r = results[0];
  assert.equal(r.outcome, "repaired");
  assert.equal(r.rowsWritten, 2, "only the state-containing buckets are written");
  assert.equal(r.rowsMissing, 6, "the silent buckets inside the gap stay unwritten and booked");
  assert.equal(r.detail.gaps_repaired, 1);
  assert.equal(r.detail.gaps_silent, 0);
  assert.equal(r.detail.gaps_silent_registered, 0);

  const key = (i) => counterEntity.haEntityId + "@" + i * BUCKET;
  assert.deepEqual(db.deltas.get(key(12)), { delta: 1.5, source: "backfill-ha-history" });
  assert.deepEqual(db.deltas.get(key(15)), { delta: 1.5, source: "backfill-ha-history" });
  assert.equal(db.deltas.size, 2, "the padding buckets (9, 18) and the silent buckets get no row");
  assert.equal(log.permanent.length, 0, "a recovered gap is never registered");
});

test("corroboration: dry-run corroborates but performs no registration", async () => {
  const log = new FakeLog();

  const { results } = await runBackfill({
    entities: [counterEntity, otherEntity],
    sinceMs: 0,
    now: () => WINDOW,
    loadRows: async (cfg) => (cfg.kind === "counter" ? existingRowsWithGap() : fullRows()),
    fetchHistory: fetchHistoryReturning({
      [counterEntity.haEntityId]: [st(counterEntity.haEntityId, 9, 104.5), st(counterEntity.haEntityId, 18, 109)],
      [otherEntity.haEntityId]: [st(otherEntity.haEntityId, 12, 1.5)],
    }),
    dbWriter: new FakeDb(),
    log,
    dryRun: true,
  });

  const r = results[0];
  assert.equal(r.outcome, "no-gap", "the corroboration itself is only detection");
  assert.equal(r.detail.gaps_silent_registered, 1, "the run reports what it would register");
  assert.equal(log.permanent.length, 0, "dry-run writes nothing, registry rows included");
  assert.equal(log.rows.length, 0);
});
