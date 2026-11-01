// Price history fallback source for the backfill mechanism (see
// docs/backfill-design.md, section 7): hours outside the forecast's rolling
// window are recovered from the recorder history of the configured hourly
// price entity (kind "price" in config.json, e.g.
// sensor.zonneplan_current_electricity_price). The worker itself writes
// prices from this entity on every state change; it publishes one state per
// hour, around 13 minutes past the hour boundary, and its state is the
// hourly price in display units. Every state observation is mapped to the
// start of its hour; when several observations land in the same hour (an
// in-hour revision), the LAST one before the hour boundary wins, matching
// the engine's writePrice-on-change upsert semantics (the live worker
// upserts on every state change, so the last change of the hour is the
// stored price).
//
// Fail-soft: fetch errors return a typed {error} object, never throw past
// the plugin boundary.

import { hourOf } from "../../buckets.mjs";
import { fetchEntityHistory } from "./ha-history.mjs";

/**
 * Pure mapping: normalized history states to hourly prices. The state value
 * is the price in EUR/kWh (the entity state is in display units; only the
 * forecast attribute uses the 1e-7 unit scale). Non-numeric states are
 * ignored, as the live engine ignores them.
 *
 * @param {Array<{entity_id: string, state: string, last_updated_ts: number}>} states
 * @returns {Array<{startMs: number, priceEur: number}>} ascending by hour
 */
export function hourlyPricesFromStates(states) {
  /** @type {Map<number, {ts: number, priceEur: number}>} hour -> last observation */
  const byHour = new Map();
  for (const s of states ?? []) {
    const priceEur = parseFloat(s.state);
    if (!Number.isFinite(priceEur)) continue;
    const ts = s.last_updated_ts * 1000;
    const hour = hourOf(ts);
    const existing = byHour.get(hour);
    if (existing === undefined || ts >= existing.ts) {
      byHour.set(hour, { ts, priceEur }); // the last observation before the boundary wins
    }
  }
  return [...byHour.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([startMs, v]) => ({ startMs, priceEur: v.priceEur }));
}

/**
 * Fetch the configured hourly price entity's recorder history for a range
 * and map it to hourly prices.
 *
 * @param {object} options - as for fetchEntityHistory (httpBase, token,
 *   entityId, startMs, endMs, fetchFn, timeoutMs)
 * @returns {Promise<{hours: Array<{startMs: number, priceEur: number}>} |
 *   {error: {code: string, message: string}}>} never throws
 */
export async function fetchPriceEntityHistory(options) {
  const result = await fetchEntityHistory(options);
  if (result.error) return result;
  return { hours: hourlyPricesFromStates(result.states) };
}
