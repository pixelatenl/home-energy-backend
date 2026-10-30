// Unit tests for the pure backfill detectors (no I/O, no clock).
// Run with: node --test test/ (or: npm test)
//
// Timestamps are milliseconds since epoch, on the worker's UTC grid: 5-minute
// buckets (BUCKET), hours (HOUR). The tests use epoch 0 as their base, and
// one fixture uses a real DST fall-back date to pin the hourly grid.

import test from "node:test";
import assert from "node:assert/strict";
import {
  detectPriceGaps,
  detectGasPriceGaps,
  detectBucketGaps,
  medianIntervalBuckets,
} from "../src/backfill/detect.mjs";

const BUCKET = 5 * 60 * 1000;
const HOUR = 60 * 60 * 1000;

/** Row fixtures: bucket starts with 5-minute cadence, one run skipped. */
function fiveMinuteRows(count, skips = []) {
  const rows = [];
  for (let i = 0; i < count; i++) {
    if (!skips.includes(i)) rows.push(i * BUCKET);
  }
  return rows;
}

test("price: a run of consecutive missing hours is one gap", () => {
  const hours = [0, 1 * HOUR, 2 * HOUR, 5 * HOUR, 6 * HOUR]; // 03:00 and 04:00 missing
  const gaps = detectPriceGaps(hours, 0, 7 * HOUR);
  assert.deepEqual(gaps, [{ startMs: 3 * HOUR, endMs: 5 * HOUR }]);
});

test("price: a full hourly grid has no gaps", () => {
  const hours = Array.from({ length: 24 }, (_, i) => i * HOUR);
  assert.deepEqual(detectPriceGaps(hours, 0, 24 * HOUR), []);
});

test("price: the DST fall-back boundary does not disturb the UTC hour grid", () => {
  // 26 October 2025, Europe/Amsterdam: clocks go back from 03:00 CEST to
  // 02:00 CET at 01:00Z. The local day is 25 hours and the repeated local
  // hour exists twice as distinct UTC hours, so the UTC grid simply has all
  // 48 hours of this two-day window. Remove 05:00Z, 06:00Z and 07:00Z on the
  // transition day: exactly those three hours must come back as one run.
  const from = Date.parse("2025-10-25T00:00:00Z");
  const to = from + 48 * HOUR;
  const hours = [];
  for (let t = from; t < to; t += HOUR) {
    const hourOfDay = (t - Date.parse("2025-10-26T00:00:00Z")) / HOUR;
    if (hourOfDay >= 5 && hourOfDay <= 7) continue; // missing 05:00Z, 06:00Z, 07:00Z
    hours.push(t);
  }
  assert.equal(hours.length, 45, "45 of the 48 UTC hours are present");
  assert.deepEqual(detectPriceGaps(hours, from, to), [
    { startMs: Date.parse("2025-10-26T05:00:00Z"), endMs: Date.parse("2025-10-26T08:00:00Z") },
  ], "the repeated local hour and the 25-hour day leave the UTC grid intact");
});

test("gas_price: missing local days on the daily grid (bounds inclusive)", () => {
  const days = ["2026-01-01", "2026-01-02", "2026-01-04", "2026-01-05"];
  assert.deepEqual(detectGasPriceGaps(days, "2026-01-01", "2026-01-05"), ["2026-01-03"]);
});

test("gas_price: a full daily grid has no gaps", () => {
  const days = ["2026-01-01", "2026-01-02", "2026-01-03"];
  assert.deepEqual(detectGasPriceGaps(days, "2026-01-01", "2026-01-03"), []);
});

test("buckets: a bounded 45-minute outage in a 5-minute cadence stream is a repairable gap", () => {
  // Rows every 5 minutes; buckets 10..17 are missing (rows 9 and 18 bound
  // the run): a span of 9 buckets with 8 missing buckets.
  const rows = fiveMinuteRows(31, [10, 11, 12, 13, 14, 15, 16, 17]);
  const det = detectBucketGaps(rows, 0, 31 * BUCKET);
  assert.equal(det.medianIntervalBuckets, 1);
  assert.equal(det.repairable.length, 1);
  assert.deepEqual(det.repairable[0], {
    startMs: 10 * BUCKET,
    endMs: 18 * BUCKET,
    spanBuckets: 9,
    missingBuckets: 8,
  });
  assert.equal(det.reportable.length, 0);
});

test("buckets: the absolute minimum of 6 buckets: 25 minutes is not a gap, 30 minutes is", () => {
  // Median 1: a run is a gap when its span is longer than 3 buckets and at
  // least 6 buckets. Span 5 (25 minutes) stays silent, span 6 (30 minutes) counts.
  const short = fiveMinuteRows(20, [8, 9, 10, 11]); // rows 7 and 12: span 5, 4 missing
  const detShort = detectBucketGaps(short, 0, 20 * BUCKET);
  assert.equal(detShort.repairable.length, 0, "span 5 is below the absolute minimum of 6");

  const exact = fiveMinuteRows(20, [8, 9, 10, 11, 12]); // rows 7 and 13: span 6, 5 missing
  const detExact = detectBucketGaps(exact, 0, 20 * BUCKET);
  assert.equal(detExact.repairable.length, 1);
  assert.equal(detExact.repairable[0].spanBuckets, 6);
  assert.equal(detExact.repairable[0].missingBuckets, 5);
});

test("buckets: an unbounded run (no later row) is never repairable, only reportable", () => {
  // Rows every 5 minutes for the first hour, then silence until the end of
  // the 3-hour window: entity silence, not a data loss to repair.
  const rows = fiveMinuteRows(12);
  const det = detectBucketGaps(rows, 0, 3 * HOUR);
  assert.equal(det.repairable.length, 0, "an unbounded run is never a gap for repair");
  assert.equal(det.reportable.length, 1);
  assert.equal(det.reportable[0].reason, "unbounded");
  assert.equal(det.reportable[0].startMs, 12 * BUCKET);
});

test("buckets: total silence (no rows at all) produces zero gaps", () => {
  const det = detectBucketGaps([], 0, 3 * HOUR);
  assert.deepEqual(det.repairable, []);
  assert.deepEqual(det.reportable, []);
});

test("buckets: a run with no earlier row is ignored entirely", () => {
  // The window starts inside the silence: without a left bound this cannot
  // be distinguished from "not ingested yet". Only the trailing unbounded
  // run is reportable; the leading silence appears nowhere.
  const rows = fiveMinuteRows(12).map((t) => t + HOUR); // rows from 01:00 to 02:00
  const det = detectBucketGaps(rows, 0, 3 * HOUR);
  assert.equal(det.repairable.length, 0);
  const reasons = det.reportable.map((r) => r.reason);
  assert.deepEqual(reasons, ["unbounded"], "no leading run is reported");
  assert.equal(det.reportable[0].startMs, 2 * HOUR);
});

test("buckets: a PV-counter-like entity at 15-minute spacing produces zero gaps", () => {
  // Inverter counter reporting every 15 minutes (median interval 3 buckets):
  // the normal cadence is not a gap, and even one missed report (span 6
  // buckets, 30 minutes) stays under the 3 x median threshold.
  const rows = [];
  for (let i = 0; i < 24; i++) rows.push(i * 3 * BUCKET); // every 15 minutes
  let det = detectBucketGaps(rows, 0, 72 * BUCKET);
  assert.equal(det.medianIntervalBuckets, 3);
  assert.equal(det.repairable.length, 0);
  assert.equal(det.reportable.length, 0);

  const withMissedReport = rows.filter((_, i) => i !== 10); // one report missing
  det = detectBucketGaps(withMissedReport, 0, 72 * BUCKET);
  assert.equal(det.repairable.length, 0, "span 6 is not longer than 3 x median 3");
  assert.equal(det.reportable.length, 0);
});

test("buckets: a weather-like entity at 1-hour spacing produces zero gaps", () => {
  // Outdoor temperature reporting hourly (median interval 12 buckets): the
  // slow cadence itself must never look like data loss.
  const rows = [];
  for (let i = 0; i < 12; i++) rows.push(i * 12 * BUCKET); // every hour
  const det = detectBucketGaps(rows, 0, 12 * HOUR);
  assert.equal(det.medianIntervalBuckets, 12);
  assert.equal(det.repairable.length, 0);
  assert.equal(det.reportable.length, 0, "the window ends right after the last row");
});

test("buckets: a gap longer than maxGapBuckets is reportable but not repairable", () => {
  // Four hours missing in a 5-minute stream: span 48 > maxGapBuckets 36.
  const rows = fiveMinuteRows(60, Array.from({ length: 47 }, (_, i) => i + 6)); // rows 5 and 53 bound the run
  const det = detectBucketGaps(rows, 0, 60 * BUCKET, { maxGapBuckets: 36 });
  assert.equal(det.repairable.length, 0);
  assert.equal(det.reportable.length, 1);
  assert.equal(det.reportable[0].reason, "too-long");
  assert.equal(det.reportable[0].spanBuckets, 48);
  assert.equal(det.reportable[0].missingBuckets, 47);
});

test("buckets: two separate outages produce two repairable gaps", () => {
  const rows = fiveMinuteRows(40, [8, 9, 10, 11, 12, 13, 14, 15, 24, 25, 26, 27, 28, 29, 30, 31]);
  const det = detectBucketGaps(rows, 0, 40 * BUCKET);
  assert.equal(det.repairable.length, 2);
  assert.equal(det.repairable[0].startMs, 8 * BUCKET);
  assert.equal(det.repairable[0].endMs, 16 * BUCKET);
  assert.equal(det.repairable[1].startMs, 24 * BUCKET);
  assert.equal(det.repairable[1].endMs, 32 * BUCKET);
});

test("buckets: rows are deduplicated on the bucket grid and kept inside the window", () => {
  const inside = fiveMinuteRows(10);
  const rows = [...inside, ...inside, -5 * BUCKET, 100 * HOUR]; // duplicates, one before, one after
  const det = detectBucketGaps(rows, 0, 10 * BUCKET);
  assert.equal(det.medianIntervalBuckets, 1);
  assert.equal(det.repairable.length, 0);
  assert.equal(det.reportable.length, 0, "the row at 100h is outside the window and ignored");
});

test("medianIntervalBuckets: pure helper with unsorted input and a minimum of 1", () => {
  assert.equal(medianIntervalBuckets([]), 1, "no rows: no evidence, minimum 1");
  assert.equal(medianIntervalBuckets([100]), 1, "one row: no interval, minimum 1");
  assert.equal(medianIntervalBuckets([0, BUCKET, 2 * BUCKET]), 1); // 5-minute cadence
  assert.equal(medianIntervalBuckets([2 * 3 * BUCKET, 0, 3 * BUCKET]), 3); // unsorted 15-minute cadence
  assert.equal(medianIntervalBuckets([0, BUCKET, 3 * BUCKET]), 1.5); // intervals [1, 2]: even count averages
});
