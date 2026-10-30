// Price forecast source for the backfill mechanism (see
// docs/backfill-design.md, section 7): the Zonneplan tariff entity
// sensor.zonneplan_current_quarter_hourly_electricity_tariff carries a
// rolling forecast attribute with 15-minute blocks. This module turns the
// attribute into hourly prices: the blocks of an hour are averaged (up to 4
// per hour) and the amount is converted from 1e-7 EUR/kWh to EUR/kWh.
//
// Only hours the attribute actually covers are provided; the rolling window
// start is honest, hours before it fall through to the history fallback
// source (price-history.mjs). Fail-soft: fetch errors return a typed
// {error} object, never throw past the plugin boundary.

import { hourOf } from "../../buckets.mjs";

/** The Home Assistant entity that carries the quarter-hourly forecast. */
export const TARIFF_FORECAST_ENTITY_ID = "sensor.zonneplan_current_quarter_hourly_electricity_tariff";

/** Zonneplan amounts are in 1e-7 EUR/kWh. */
const UNIT_SCALE = 1e7;

const DEFAULT_TIMEOUT_MS = 30000;

/**
 * Pure parser: a raw HA state (with attributes.forecast) to hourly prices.
 * An unavailable or unknown entity, or a missing forecast attribute, yields
 * no hours (the fallback source takes over). Blocks are grouped by the hour
 * of their start_date; an hour is provided when the forecast covers at least
 * one of its blocks, with the average of the blocks it actually covers.
 *
 * @param {object} rawState - HA state object
 * @returns {Array<{startMs: number, priceEur: number}>} ascending by hour
 */
export function parseForecastHours(rawState) {
  if (rawState === null || typeof rawState !== "object") return [];
  if (rawState.state === "unavailable" || rawState.state === "unknown") return [];
  const forecast = rawState.attributes?.forecast;
  if (!Array.isArray(forecast)) return [];

  /** @type {Map<number, number[]>} hour start -> block amounts */
  const byHour = new Map();
  for (const block of forecast) {
    if (block === null || typeof block !== "object") continue;
    const startMs = Date.parse(block.start_date);
    const amount = block.price_tax_included?.amount;
    if (!Number.isFinite(startMs) || !Number.isFinite(amount)) continue;
    const hour = hourOf(startMs);
    if (!byHour.has(hour)) byHour.set(hour, []);
    byHour.get(hour).push(amount);
  }

  return [...byHour.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([startMs, amounts]) => ({
      startMs,
      priceEur:
        amounts.reduce((sum, a) => sum + a, 0) / amounts.length / UNIT_SCALE,
    }));
}

/**
 * Fetch the tariff entity over the REST states API and parse its forecast
 * into hourly prices. One GET per run: the forecast is a rolling window.
 *
 * @param {object} options
 * @param {string} options.httpBase - for example http://localhost:8123
 * @param {string} options.token - long-lived access token (Bearer)
 * @param {string} [options.entityId] - defaults to TARIFF_FORECAST_ENTITY_ID
 * @param {Function} [options.fetchFn] - injectable fetch (tests)
 * @param {number} [options.timeoutMs=30000]
 * @returns {Promise<{hours: Array<{startMs: number, priceEur: number}>} |
 *   {error: {code: string, message: string}}>} never throws
 */
export async function fetchPriceForecast(options) {
  const {
    httpBase,
    token,
    entityId = TARIFF_FORECAST_ENTITY_ID,
    fetchFn = fetch,
    timeoutMs = DEFAULT_TIMEOUT_MS,
  } = options;
  const url = `${httpBase.replace(/\/$/, "")}/api/states/${encodeURIComponent(entityId)}`;
  try {
    const res = await fetchFn(url, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) {
      return {
        error: {
          code: `http-${res.status}`,
          message: `HA states request for ${entityId} failed with HTTP ${res.status}`,
        },
      };
    }
    const state = await res.json();
    return { hours: parseForecastHours(state) };
  } catch (e) {
    return {
      error: {
        code: "fetch-error",
        message: `HA states request for ${entityId} failed: ${e?.message ?? String(e)}`,
      },
    };
  }
}
