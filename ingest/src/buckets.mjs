// Helper functions for time intervals.
// All times are milliseconds since epoch (UTC); buckets are rounded down
// to the bucket start, in line with the database design (revision 2).

export const BUCKET_MS = 5 * 60 * 1000; // 5 minutes
export const HOUR_MS = 60 * 60 * 1000;

export function bucketOf(tsMs) {
  return Math.floor(tsMs / BUCKET_MS) * BUCKET_MS;
}

export function hourOf(tsMs) {
  return Math.floor(tsMs / HOUR_MS) * HOUR_MS;
}
