// Class 2 catch-up detection, pure part (see docs/backfill-design.md,
// section 7.3). No I/O and no clock: the caller passes the delta rows of one
// entity, the detector decides which of them are attribution-suspect.
//
// Class 2 data loss leaves rows present but suspect: a meter that was
// offline for hours reports a cumulative jump on its first reading
// afterwards, and the worker dutifully derives one 5-minute delta that in
// reality contains the energy of the whole offline period. The energy is
// real, but it cannot be attributed to the buckets it belongs to.
//
// Rule: a delta row is attribution-suspect when |delta_kwh| exceeds
// EMS_BACKFILL_CATCHUP_FACTOR (default 5) times the median |delta| of the
// same entity over the trailing 7 days before the row, with a minimum of
// 5 samples in that median window; without enough samples the row is
// never flagged (no verdict beats a false alarm).
//
// The detector only reports. It writes no rows, modifies nothing and
// never redistributes energy over the covered buckets; the repair run
// turns its output into an outcome 'attribution-suspect' backfill_log row.

import { DAY_MS } from "../buckets.mjs";

/** Median window: the trailing 7 days before each row. Fixed by design. */
export const CATCHUP_WINDOW_MS = 7 * DAY_MS;

/** EMS_BACKFILL_CATCHUP_FACTOR, default 5. */
export const DEFAULT_CATCHUP_FACTOR = 5;

/** Minimum samples in the median window; below it a row is never flagged. */
export const MIN_CATCHUP_SAMPLES = 5;

/**
 * Median of a non-empty array of numbers (average of the two middle values
 * when the count is even).
 *
 * @param {Array<number>} values
 * @returns {number}
 */
export function medianOf(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

/**
 * Flag the attribution-suspect delta rows of one entity.
 *
 * For every row the median |delta| is taken over the entity's rows strictly
 * before it and within opts.windowMs (the trailing 7 days, the boundary
 * included); the row itself is never part of its own median window. A zero
 * median (for example a week of zero deltas) makes every non-zero delta
 * suspect: that is exactly the catch-up pattern.
 *
 * @param {Array<{tsMs: number, deltaKwh: number}>} deltas - one entity, any
 *   order; duplicate timestamps cannot exist (unique index in the database)
 * @param {object} [opts]
 * @param {number} [opts.factor=DEFAULT_CATCHUP_FACTOR] - EMS_BACKFILL_CATCHUP_FACTOR
 * @param {number} [opts.windowMs=CATCHUP_WINDOW_MS] - median window length
 * @param {number} [opts.minSamples=MIN_CATCHUP_SAMPLES] - minimum samples in
 *   the median window; below it the row is never flagged
 * @returns {Array<{tsMs: number, deltaKwh: number, medianAbsDelta: number,
 *   windowSamples: number}>} the suspects, ascending by timestamp
 */
export function flagAttributionSuspects(deltas, opts = {}) {
  const factor = Number.isFinite(opts.factor) && opts.factor > 0 ? opts.factor : DEFAULT_CATCHUP_FACTOR;
  const windowMs = Number.isFinite(opts.windowMs) && opts.windowMs > 0 ? opts.windowMs : CATCHUP_WINDOW_MS;
  const minSamples = Number.isFinite(opts.minSamples) && opts.minSamples >= 0
    ? Math.floor(opts.minSamples)
    : MIN_CATCHUP_SAMPLES;

  const rows = (deltas ?? [])
    .filter((d) => d !== null && d !== undefined && Number.isFinite(d.tsMs) && Number.isFinite(d.deltaKwh))
    .sort((a, b) => a.tsMs - b.tsMs);

  const suspects = [];
  for (let i = 0; i < rows.length; i++) {
    const window = [];
    for (let j = i - 1; j >= 0 && rows[i].tsMs - rows[j].tsMs <= windowMs; j--) {
      window.push(Math.abs(rows[j].deltaKwh));
    }
    if (window.length < minSamples) continue; // never flag without enough samples
    const medianAbsDelta = medianOf(window);
    if (Math.abs(rows[i].deltaKwh) > factor * medianAbsDelta) {
      suspects.push({
        tsMs: rows[i].tsMs,
        deltaKwh: rows[i].deltaKwh,
        medianAbsDelta,
        windowSamples: window.length,
      });
    }
  }
  return suspects;
}
