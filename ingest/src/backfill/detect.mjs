// Pure gap detectors for the backfill mechanism (see docs/backfill-design.md,
// section 6). No I/O and no clock: the caller passes the existing rows and
// the detection window, so the detectors are fully testable and the clock is
// injectable through the window bounds.
//
// All timestamps are milliseconds since epoch, on the same UTC grid the
// worker stores on (buckets.mjs): 5-minute buckets, hours, and local calendar
// days for gas prices.

import { BUCKET_MS, HOUR_MS, DAY_MS, bucketOf, hourOf } from "../buckets.mjs";

/**
 * Median interval between consecutive rows, expressed in 5-minute buckets,
 * with an absolute minimum of 1. This is the entity's observed cadence:
 * a PV counter reporting every 15 minutes has median 3, a weather sensor
 * reporting hourly has median 12. Pure: the input is neither sorted nor
 * deduplicated here, both happen inside.
 *
 * @param {Array<number>} rows - bucket starts (milliseconds since epoch)
 * @returns {number} median interval in buckets, at least 1
 */
export function medianIntervalBuckets(rows) {
  if (!Array.isArray(rows) || rows.length < 2) return 1;
  const sorted = [...new Set(rows.map((r) => bucketOf(Number(r))))].sort((a, b) => a - b);
  const intervals = [];
  for (let i = 1; i < sorted.length; i++) {
    const span = Math.round((sorted[i] - sorted[i - 1]) / BUCKET_MS);
    intervals.push(Math.max(1, span));
  }
  if (intervals.length === 0) return 1;
  intervals.sort((a, b) => a - b);
  const mid = Math.floor(intervals.length / 2);
  const median =
    intervals.length % 2 === 1 ? intervals[mid] : (intervals[mid - 1] + intervals[mid]) / 2;
  return Math.max(1, median);
}

/**
 * Missing hours on the hourly grid, for kind "price".
 *
 * @param {Array<number>} existingHours - hour starts already in the price table
 * @param {number} fromMs - window start
 * @param {number} toMs - window end (the hour containing it is excluded)
 * @returns {Array<{startMs: number, endMs: number}>} runs of consecutive
 *   missing hours, endMs exclusive. DST transitions do not disturb the grid:
 *   prices live on UTC hours and the Amsterdam offset is whole hours, so the
 *   repeated local hour at fall-back is two distinct UTC hours.
 */
export function detectPriceGaps(existingHours, fromMs, toMs) {
  const present = new Set((existingHours ?? []).map((h) => hourOf(Number(h))));
  const runs = [];
  for (let t = hourOf(Number(fromMs)); t < hourOf(Number(toMs)); t += HOUR_MS) {
    if (present.has(t)) continue;
    const last = runs[runs.length - 1];
    if (last !== undefined && last.endMs === t) {
      last.endMs = t + HOUR_MS;
    } else {
      runs.push({ startMs: t, endMs: t + HOUR_MS });
    }
  }
  return runs;
}

/**
 * Missing local days on the daily grid, for kind "gas_price".
 *
 * @param {Array<string>} existingDays - local days ("YYYY-MM-DD") already in the gas_price table
 * @param {string} fromDate - first expected local day (inclusive)
 * @param {string} toDate - last expected local day (inclusive)
 * @returns {Array<string>} the missing local day strings, ascending
 */
export function detectGasPriceGaps(existingDays, fromDate, toDate) {
  const present = new Set((existingDays ?? []).map(String));
  const missing = [];
  const fromMs = Date.parse(`${fromDate}T00:00:00Z`);
  const toMs = Date.parse(`${toDate}T00:00:00Z`);
  if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || fromMs > toMs) return missing;
  for (let t = fromMs; t <= toMs; t += DAY_MS) {
    const day = new Date(t).toISOString().slice(0, 10);
    if (!present.has(day)) missing.push(day);
  }
  return missing;
}

/**
 * Cadence-aware gap detection for kinds "sample" and "counter".
 *
 * A missing run between two consecutive rows with span L buckets (the
 * interval between the neighbours, in 5-minute buckets) counts as a gap only
 * when L > 3 x the median interval AND L >= 6 (absolute minimum).
 *
 * Repairable gaps additionally satisfy L <= opts.maxGapBuckets (default 36)
 * and are bounded on BOTH sides by existing rows. Bounded runs that are too
 * long are reportable but not repairable (reason "too-long"). Runs with no
 * later row (entity silence up to the end of the window) are never gaps for
 * repair, only reportable (reason "unbounded", and only when they exceed the
 * cadence threshold). Runs with no earlier row are ignored entirely: without
 * a left bound a silence cannot be distinguished from "not ingested yet".
 *
 * @param {Array<number>} rows - bucket starts already in the database
 * @param {number} fromMs - window start
 * @param {number} toMs - window end
 * @param {object} [opts]
 * @param {number} [opts.maxGapBuckets=36] - maximum span of a repairable gap
 * @returns {{medianIntervalBuckets: number,
 *   repairable: Array<{startMs: number, endMs: number, spanBuckets: number, missingBuckets: number}>,
 *   reportable: Array<{startMs: number, endMs: number, spanBuckets: number, missingBuckets: number, reason: string}>}}
 *   startMs is the first missing bucket, endMs the first existing bucket
 *   after the run (for unbounded runs: the window end).
 */
export function detectBucketGaps(rows, fromMs, toMs, opts = {}) {
  const maxGapBuckets = opts.maxGapBuckets ?? 36;
  const from = bucketOf(Number(fromMs));
  const to = bucketOf(Number(toMs));
  const result = { medianIntervalBuckets: 1, repairable: [], reportable: [] };

  const sorted = [...new Set((rows ?? []).map((r) => bucketOf(Number(r))))]
    .filter((t) => t >= from && t < to)
    .sort((a, b) => a - b);
  result.medianIntervalBuckets = medianIntervalBuckets(sorted);

  const median = result.medianIntervalBuckets;
  const isGap = (spanBuckets) => spanBuckets > 3 * median && spanBuckets >= 6;

  const run = (leftRow, rightRow) => {
    const spanBuckets = Math.round((rightRow - leftRow) / BUCKET_MS);
    return {
      startMs: leftRow + BUCKET_MS,
      endMs: rightRow,
      spanBuckets,
      missingBuckets: spanBuckets - 1,
    };
  };

  // Bounded runs: between two consecutive existing rows.
  for (let i = 1; i < sorted.length; i++) {
    const spanBuckets = Math.round((sorted[i] - sorted[i - 1]) / BUCKET_MS);
    if (!isGap(spanBuckets)) continue;
    const r = run(sorted[i - 1], sorted[i]);
    if (spanBuckets <= maxGapBuckets) {
      result.repairable.push(r);
    } else {
      result.reportable.push({ ...r, reason: "too-long" });
    }
  }

  // Trailing run: no later row, never a gap for repair, only reportable.
  const lastRow = sorted[sorted.length - 1];
  if (lastRow !== undefined && to > lastRow) {
    const spanBuckets = Math.round((to - lastRow) / BUCKET_MS);
    if (isGap(spanBuckets)) {
      result.reportable.push({ ...run(lastRow, to), reason: "unbounded" });
    }
  }

  return result;
}
