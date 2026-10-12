// Helper functions for time intervals.
// All times are milliseconds since epoch (UTC); buckets are rounded down
// to the bucket start, in line with the database design (revision 2).

export const BUCKET_MS = 5 * 60 * 1000; // 5 minutes
export const HOUR_MS = 60 * 60 * 1000;
export const DAY_MS = 24 * 60 * 60 * 1000;

export function bucketOf(tsMs) {
  return Math.floor(tsMs / BUCKET_MS) * BUCKET_MS;
}

export function hourOf(tsMs) {
  return Math.floor(tsMs / HOUR_MS) * HOUR_MS;
}

// Local calendar day (YYYY-MM-DD) of a timestamp in the given IANA time zone.
// Used for daily facts such as the gas price, which belongs to the local day
// (Europe/Amsterdam in production), not to the UTC day of the server clock.
export function dayOf(tsMs, timeZone = "UTC") {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(tsMs));
}
