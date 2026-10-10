// Unit tests for the IngestEngine (pure logic, no I/O).
// Run with: node --test test/engine.test.mjs (or: npm test)
//
// Timestamps are milliseconds since epoch. Bucket size is 5 minutes.
// The tests use epoch 0 (Thursday 00:00 UTC) as their base;
// bucketOf(0) = 0, so t=0, t=100000 and so on fall in the same first bucket.

import test from "node:test";
import assert from "node:assert/strict";
import { IngestEngine } from "../src/engine.mjs";

const BUCKET = 5 * 60 * 1000;
const HOUR = 60 * 60 * 1000;

function makeWriter() {
  const calls = { measurement: [], delta: [], price: [] };
  return {
    calls,
    writeMeasurement: (meta, ts, value) => calls.measurement.push({ meta, ts, value }),
    writeEnergyDelta: (meta, ts, delta) => calls.delta.push({ meta, ts, delta }),
    writePrice: (meta, ts, price) => calls.price.push({ meta, ts, price }),
  };
}

function makeEngine(entities, writer, now = () => 0) {
  const engine = new IngestEngine(writer, { now });
  engine.registerEntities(entities);
  return engine;
}

const sampleEntity = {
  haEntityId: "sensor.house_power",
  name: "House power",
  domain: "grid",
  unit: "kW",
  kind: "sample",
};
const counterEntity = {
  haEntityId: "sensor.grid_consumption_total",
  name: "Grid consumption total",
  domain: "grid",
  unit: "kWh",
  kind: "counter",
};
const priceEntity = {
  haEntityId: "sensor.current_electricity_price",
  name: "Current electricity price",
  domain: "price",
  unit: "EUR/kWh",
  kind: "price",
};

test("sample: the last value of a bucket is only written when the bucket closes", () => {
  const w = makeWriter();
  const e = makeEngine([sampleEntity], w);
  e.onState("sensor.house_power", 1.5, 0); // baseline in bucket 0
  e.onState("sensor.house_power", 2.0, 100_000); // same bucket
  assert.equal(w.calls.measurement.length, 0, "bucket 0 has not closed yet");
  e.onState("sensor.house_power", 2.5, BUCKET); // new bucket -> bucket 0 closes
  assert.deepEqual(w.calls.measurement, [
    { meta: sampleEntity, ts: 0, value: 2.0 },
  ], "the last value of bucket 0 (2.0) is written with the bucket start as ts");
});

test("counter: delta is the difference between the last readings of consecutive buckets", () => {
  const w = makeWriter();
  const e = makeEngine([counterEntity], w);
  e.onState("sensor.grid_consumption_total", 100.0, 0); // baseline bucket 0, no delta
  e.onState("sensor.grid_consumption_total", 102.5, 60_000); // last reading in bucket 0
  e.onState("sensor.grid_consumption_total", 103.5, BUCKET); // opens bucket 1
  assert.equal(w.calls.delta.length, 0, "bucket 0 is the baseline: no delta possible");
  e.onState("sensor.grid_consumption_total", 106.0, 2 * BUCKET); // opens bucket 2, closes bucket 1
  assert.deepEqual(w.calls.delta, [
    { meta: counterEntity, ts: BUCKET, delta: 1.0 },
  ], "delta of bucket 1 = 103.5 - 102.5 = 1.0");
});

test("counter: a meter reset yields delta 0; afterwards the delta continues from the new reading", () => {
  const w = makeWriter();
  const e = makeEngine([counterEntity], w);
  e.onState("sensor.grid_consumption_total", 103.5, 0); // baseline
  e.onState("sensor.grid_consumption_total", 2.0, BUCKET); // reset; bucket 0 closes: delta negative -> 0
  assert.deepEqual(w.calls.delta, [], "bucket 0 is the baseline: nothing is written");
  e.onState("sensor.grid_consumption_total", 3.0, 2 * BUCKET); // bucket 1 closes
  assert.deepEqual(w.calls.delta, [
    { meta: counterEntity, ts: BUCKET, delta: 0 },
  ], "the delta across the reset becomes 0, not negative");
  e.onState("sensor.grid_consumption_total", 4.5, 3 * BUCKET); // bucket 2 closes
  assert.deepEqual(w.calls.delta[1], { meta: counterEntity, ts: 2 * BUCKET, delta: 1.0 },
    "after the reset the delta simply continues: bucket 2 = 3.0 - 2.0 = 1.0");
});

test("price: every change is written at the start of the hour", () => {
  const w = makeWriter();
  const e = makeEngine([priceEntity], w);
  e.onState("sensor.current_electricity_price", 0.21, 30 * 60_000); // 00:30
  e.onState("sensor.current_electricity_price", 0.19, 90 * 60_000); // 01:30
  assert.deepEqual(w.calls.price, [
    { meta: priceEntity, ts: 0, price: 0.21 },
    { meta: priceEntity, ts: HOUR, price: 0.19 },
  ], "the timestamp is the start of the hour of the observation");
});

test("tick closes a silent bucket exactly once", () => {
  const w = makeWriter();
  let clock = 0;
  const e = makeEngine([counterEntity], w, () => clock);
  e.onState("sensor.grid_consumption_total", 10.0, 0); // baseline bucket 0
  e.onState("sensor.grid_consumption_total", 11.0, 100_000); // last reading bucket 0
  clock = 10 * 60 * 1000; // 10 minutes later: bucket 0 closes silently
  e.tick();
  assert.deepEqual(w.calls.delta, [], "bucket 0 is the baseline: no delta");
  clock = 15 * 60 * 1000; // buckets 1 and 2 remain empty (no measurements)
  e.tick();
  assert.deepEqual(w.calls.delta, [], "silent buckets without measurements get no row");
  e.onState("sensor.grid_consumption_total", 12.0, clock); // first measurement in bucket 3 (15 min)
  clock = 25 * 60 * 1000; // bucket 3 closes silently
  e.tick();
  assert.deepEqual(w.calls.delta, [
    { meta: counterEntity, ts: 3 * BUCKET, delta: 1.0 },
  ], "delta across the silent period = 12.0 - 11.0 = 1.0, and nothing was written twice");
});

test("tick writes nothing while the current bucket has not yet passed", () => {
  const w = makeWriter();
  const e = makeEngine([sampleEntity], w, () => 0);
  e.onState("sensor.house_power", 1.0, 0);
  e.tick(); // clock is at 0: bucket 0 is current
  assert.equal(w.calls.measurement.length, 0, "the active bucket is not closed");
});

test("non-numeric and unknown values are ignored", () => {
  const w = makeWriter();
  const e = makeEngine([sampleEntity], w);
  e.onState("sensor.house_power", Number.NaN, 0);
  e.onState("sensor.unknown_entity", 1.0, 0);
  e.tick();
  assert.deepEqual(w.calls.measurement, [], "nothing written");
});

test("lagging timestamps do not cause duplicate or incorrect writes", () => {
  const w = makeWriter();
  const e = makeEngine([sampleEntity], w, () => 2 * BUCKET);
  e.onState("sensor.house_power", 1.0, 0); // baseline bucket 0
  e.onState("sensor.house_power", 2.0, BUCKET); // bucket 1, closes bucket 0 with value 1.0
  e.onState("sensor.house_power", 1.8, BUCKET - 1000); // back into bucket 0: ignore
  e.tick(); // clock in bucket 2: bucket 1 closes with 2.0
  assert.deepEqual(w.calls.measurement, [
    { meta: sampleEntity, ts: 0, value: 1.0 },
    { meta: sampleEntity, ts: BUCKET, value: 2.0 },
  ], "bucket 0 was correctly closed with 1.0; the lagging measurement does not change that");
});
