// Gas price history source for the backfill mechanism (see
// docs/backfill-design.md, section 7): the daily gas price entity (kind
// gas_price in ingest/config.json, for example
// sensor.zonneplan_gas_price_daily) is recovered from the recorder history.
// Every state observation is a candidate price for the LOCAL calendar day
// (same dayOf semantics as the engine) it was observed in, and the first
// observation per local day wins; a later change on the same day cannot
// corrupt the day price, and an evening pre-publication of the next day's
// price belongs to the evening's day, never to the current day's price.
//
// The entity id comes from the configuration (kind gas_price); it is not
// hardcoded a second time here. Fail-soft: fetch errors return a typed
// {error} object, never throw past the plugin boundary.

import { dayOf, DAY_MS } from "../../buckets.mjs";
import { fetchEntityHistory } from "./ha-history.mjs";

/**
 * UTC instant of local midnight of a local calendar day ("YYYY-MM-DD") in
 * the given IANA time zone. Local midnight of 25 October 2026 in
 * Europe/Amsterdam is 22:00Z on 24 October, for example.
 */
export function localDayStartMs(day, timeZone = "Europe/Amsterdam") {
  const guess = Date.parse(`${day}T00:00:00Z`);
  if (!Number.isFinite(guess)) {
    throw new Error(`localDayStartMs: not a YYYY-MM-DD day: ${day}`);
  }
  // The offset at the guessed instant differs by at most a few hours from
  // the offset at local midnight of the same day; the candidate lands within
  // an hour of the true boundary, and the dayOf check plus the one-hour
  // window make the search exact (Amsterdam transitions happen at 02:00/03:00
  // local, never at midnight).
  const offset = tzOffsetMs(guess, timeZone);
  for (const candidate of [guess - offset, guess - offset - 3600000, guess - offset + 3600000]) {
    if (dayOf(candidate, timeZone) === day && dayOf(candidate - 1, timeZone) !== day) {
      return candidate;
    }
  }
  throw new Error(`localDayStartMs: could not find local midnight of ${day} in ${timeZone}`);
}

/** The local day after a "YYYY-MM-DD" day string. */
export function nextDay(day) {
  return new Date(Date.parse(`${day}T00:00:00Z`) + DAY_MS).toISOString().slice(0, 10);
}

/** Offset of a time zone from UTC at the given instant, in milliseconds. */
function tzOffsetMs(ts, timeZone) {
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  const parts = Object.fromEntries(
    dtf.formatToParts(new Date(ts)).map((p) => [p.type, p.value])
  );
  const asUtc = Date.UTC(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    Number(parts.hour) % 24,
    Number(parts.minute),
    Number(parts.second)
  );
  return asUtc - ts;
}

/**
 * Pure mapping: normalized history states to daily gas prices. The first
 * observation per local day wins (engine semantics: the first write of the
 * day wins). Non-numeric states are ignored, as the live engine ignores them.
 *
 * @param {Array<{entity_id: string, state: string, last_updated_ts: number}>} states
 * @param {string} [timeZone] - IANA time zone of the local calendar day
 * @returns {Array<{day: string, priceEur: number}>} ascending by day
 */
export function dailyPricesFromStates(states, timeZone = "Europe/Amsterdam") {
  /** @type {Map<string, {ts: number, priceEur: number}>} day -> first observation */
  const byDay = new Map();
  for (const s of states ?? []) {
    const priceEur = parseFloat(s.state);
    if (!Number.isFinite(priceEur)) continue;
    const ts = s.last_updated_ts * 1000;
    const day = dayOf(ts, timeZone);
    const existing = byDay.get(day);
    if (existing === undefined || ts < existing.ts) {
      byDay.set(day, { ts, priceEur }); // the first observation of the day wins
    }
  }
  return [...byDay.entries()]
    .sort((a, b) => (a[0] < b[0] ? -1 : 1))
    .map(([day, v]) => ({ day, priceEur: v.priceEur }));
}

/**
 * Fetch the gas price entity's recorder history for a range and map it to
 * daily prices. The caller derives the range from local-day boundaries.
 *
 * @param {object} options - as for fetchEntityHistory (httpBase, token,
 *   entityId, startMs, endMs, fetchFn, timeoutMs), plus timeZone
 * @returns {Promise<{days: Array<{day: string, priceEur: number}>} |
 *   {error: {code: string, message: string}}>} never throws
 */
export async function fetchGasPriceHistory(options) {
  const { timeZone = "Europe/Amsterdam", ...fetchOptions } = options;
  const result = await fetchEntityHistory(fetchOptions);
  if (result.error) return result;
  return { days: dailyPricesFromStates(result.states, timeZone) };
}
