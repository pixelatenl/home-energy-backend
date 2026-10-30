// Unit tests for the backfill source plugins (pure parsing and mapping, no
// I/O). Run with: node --test test/ (or: npm test)
//
// Fixtures mirror the real Zonneplan attribute shapes: the forecast blocks
// carry amounts in 1e-7 EUR/kWh, the tariff entity states carry prices in
// EUR, and the gas price entity states carry EUR per m3.

import test from "node:test";
import assert from "node:assert/strict";
import { parseForecastHours } from "../src/backfill/sources/price-forecast.mjs";
import { fetchEntityHistory } from "../src/backfill/sources/ha-history.mjs";
import { hourlyPricesFromStates } from "../src/backfill/sources/price-history.mjs";
import {
  dailyPricesFromStates,
  localDayStartMs,
  nextDay,
} from "../src/backfill/sources/gas-price-history.mjs";
import { dayOf } from "../src/buckets.mjs";

const HOUR = 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// Forecast parser (price-forecast.mjs)

/** A 15-minute forecast block in the Zonneplan attribute shape. */
function block(startISO, amount) {
  const start = Date.parse(startISO);
  return {
    start_date: startISO,
    end_date: new Date(start + 15 * 60 * 1000).toISOString(),
    price_tax_included: { amount },
  };
}

function forecastState(blocks, state = "0.1496877") {
  return {
    entity_id: "sensor.zonneplan_current_quarter_hourly_electricity_tariff",
    state,
    attributes: { forecast: blocks },
  };
}

test("forecast: a single 15-min block pins the exact 1e-7 conversion", () => {
  const hours = parseForecastHours(forecastState([block("2026-02-10T15:00:00Z", 1496877)]));
  assert.deepEqual(hours, [{ startMs: Date.parse("2026-02-10T15:00:00Z"), priceEur: 0.1496877 }]);
});

test("forecast: up to four 15-min blocks of an hour are averaged", () => {
  const hours = parseForecastHours(
    forecastState([
      block("2026-02-10T15:00:00Z", 1496877),
      block("2026-02-10T15:15:00Z", 1496877),
      block("2026-02-10T15:30:00Z", 1500000),
      block("2026-02-10T15:45:00Z", 1500000),
    ])
  );
  assert.deepEqual(hours, [
    { startMs: Date.parse("2026-02-10T15:00:00Z"), priceEur: 0.14984385 },
  ], "(1496877 + 1496877 + 1500000 + 1500000) / 4 / 1e7");
});

test("forecast: blocks are grouped by hour, ascending, one price per hour", () => {
  const hours = parseForecastHours(
    forecastState([
      block("2026-02-10T16:15:00Z", 1500000),
      block("2026-02-10T15:00:00Z", 1496877),
      block("2026-02-10T16:00:00Z", 1500000),
      block("2026-02-10T15:45:00Z", 1496877),
    ])
  );
  assert.deepEqual(hours, [
    { startMs: Date.parse("2026-02-10T15:00:00Z"), priceEur: 0.1496877 },
    { startMs: Date.parse("2026-02-10T16:00:00Z"), priceEur: 0.15 },
  ], "unsorted input blocks are grouped per hour");
});

test("forecast: partial hour coverage is averaged over the blocks it has", () => {
  // The rolling window starts mid-hour: only 15:30 and 15:45 exist.
  const hours = parseForecastHours(
    forecastState([
      block("2026-02-10T15:30:00Z", 1400000),
      block("2026-02-10T15:45:00Z", 1600000),
      block("2026-02-10T16:00:00Z", 1700000),
    ])
  );
  assert.deepEqual(hours, [
    { startMs: Date.parse("2026-02-10T15:00:00Z"), priceEur: 0.15 },
    { startMs: Date.parse("2026-02-10T16:00:00Z"), priceEur: 0.17 },
  ], "the partially covered hour is provided with the average of its 2 blocks");
});

test("forecast: an unavailable entity or a missing attribute yields no hours", () => {
  assert.deepEqual(parseForecastHours({ state: "unavailable", attributes: {} }), []);
  assert.deepEqual(parseForecastHours({ state: "unknown", attributes: { forecast: [block("2026-02-10T15:00:00Z", 1496877)] } }), []);
  assert.deepEqual(parseForecastHours({ state: "0.1", attributes: {} }), []);
  assert.deepEqual(parseForecastHours(null), []);
});

// ---------------------------------------------------------------------------
// Price history mapping (price-history.mjs)

function stateAt(iso, value) {
  return {
    entity_id: "sensor.zonneplan_current_quarter_hourly_electricity_tariff",
    state: String(value),
    last_updated_ts: Date.parse(iso) / 1000,
  };
}

test("price history: the last observation before the hour boundary wins", () => {
  // The live engine upserts on every state change, so the last change of the
  // hour is the stored price; the backfill mapping must match.
  const hours = hourlyPricesFromStates([
    stateAt("2026-02-10T15:04:00Z", 0.20),
    stateAt("2026-02-10T15:37:00Z", 0.22),
    stateAt("2026-02-10T15:58:00Z", 0.21),
    stateAt("2026-02-10T16:20:00Z", 0.25),
  ]);
  assert.deepEqual(hours, [
    { startMs: Date.parse("2026-02-10T15:00:00Z"), priceEur: 0.21 },
    { startMs: Date.parse("2026-02-10T16:00:00Z"), priceEur: 0.25 },
  ], "hour 15 gets the 15:58 observation (0.21), not the earlier ones");
});

test("price history: non-numeric states are ignored", () => {
  const hours = hourlyPricesFromStates([
    stateAt("2026-02-10T15:04:00Z", "unavailable"),
    stateAt("2026-02-10T15:30:00Z", 0.31),
    stateAt("2026-02-10T15:59:00Z", "unknown"),
  ]);
  assert.deepEqual(hours, [
    { startMs: Date.parse("2026-02-10T15:00:00Z"), priceEur: 0.31 },
  ]);
});

// ---------------------------------------------------------------------------
// Gas price day mapping (gas-price-history.mjs)

test("gas history: the first observation per local day wins", () => {
  const tz = "Europe/Amsterdam";
  const days = dailyPricesFromStates(
    [
      stateAt("2026-10-12T06:00:00+02:00", 1.80), // later change on 12 October: ignored
      stateAt("2026-10-12T00:10:00+02:00", 1.7589), // first observation of 12 October
      stateAt("2026-10-13T00:05:00+02:00", 1.90), // first observation of 13 October
    ],
    tz
  );
  assert.deepEqual(days, [
    { day: "2026-10-12", priceEur: 1.7589 },
    { day: "2026-10-13", priceEur: 1.90 },
  ]);
});

test("gas history: an evening pre-publication of the next day must not leak into the current day", () => {
  const tz = "Europe/Amsterdam";
  const days = dailyPricesFromStates(
    [
      stateAt("2026-10-12T00:10:00+02:00", 1.7589), // day price of 12 October
      stateAt("2026-10-12T23:00:00+02:00", 1.90), // evening: the NEXT day's price, observed during day 12
      stateAt("2026-10-13T00:05:00+02:00", 1.90), // morning: the same price becomes day 13's first observation
    ],
    tz
  );
  assert.deepEqual(days, [
    { day: "2026-10-12", priceEur: 1.7589 },
    { day: "2026-10-13", priceEur: 1.90 },
  ], "the evening observation belongs to day 12 but arrives too late: first write of the day wins");
});

test("gas history: the local day boundary follows the time zone, not UTC", () => {
  const tz = "Europe/Amsterdam";
  const days = dailyPricesFromStates(
    [
      stateAt("2026-10-12T22:30:00Z", 1.7589), // 00:30 local on 13 October
      stateAt("2026-10-12T21:30:00Z", 1.60), // 23:30 local on 12 October
    ],
    tz
  );
  assert.deepEqual(days, [
    { day: "2026-10-12", priceEur: 1.60 },
    { day: "2026-10-13", priceEur: 1.7589 },
  ]);
});

// ---------------------------------------------------------------------------
// Local day helpers

test("localDayStartMs: local midnight expressed in UTC, across DST", () => {
  const tz = "Europe/Amsterdam";
  // Summer (CEST, UTC+2): local midnight of 12 October 2026 is 22:00Z on 11 October.
  assert.equal(localDayStartMs("2026-10-12", tz), Date.parse("2026-10-11T22:00:00Z"));
  // Winter (CET, UTC+1): local midnight of 12 November 2026 is 23:00Z on 11 November.
  assert.equal(localDayStartMs("2026-11-12", tz), Date.parse("2026-11-11T23:00:00Z"));
  // The 25-hour fall-back day (25 October 2026) starts at 22:00Z on 24 October.
  assert.equal(localDayStartMs("2026-10-25", tz), Date.parse("2026-10-24T22:00:00Z"));
  // The helper is the inverse of dayOf at the boundary.
  assert.equal(dayOf(localDayStartMs("2026-10-25", tz) - 1, tz), "2026-10-24");
  assert.equal(dayOf(localDayStartMs("2026-10-25", tz), tz), "2026-10-25");
});

test("nextDay: the local day after a day string", () => {
  assert.equal(nextDay("2026-10-12"), "2026-10-13");
  assert.equal(nextDay("2026-10-31"), "2026-11-01");
  assert.equal(nextDay("2026-12-31"), "2027-01-01");
});

// ---------------------------------------------------------------------------
// HA history client (ha-history.mjs), fetch faked: no network

test("fetchEntityHistory: the start is the single path segment, the end time is the end_time query parameter", async () => {
  const calls = [];
  const fetchFn = async (url) => {
    calls.push(url);
    return {
      ok: true,
      status: 200,
      json: async () => [[
        { entity_id: "sensor.grid_consumption_total", state: "104.5", last_updated_ts: 1770000000 },
      ]],
    };
  };

  const result = await fetchEntityHistory({
    httpBase: "http://localhost:8123/", // trailing slash must be stripped
    token: "secret",
    entityId: "sensor.grid_consumption_total",
    startMs: Date.parse("2026-02-10T00:00:00Z"),
    endMs: Date.parse("2026-02-10T12:00:00Z"),
    fetchFn,
  });

  // HA serves history at GET /api/history/period/<start> with the end time
  // as the end_time query parameter; the end time in the path matches no
  // route (live-verified 404).
  assert.equal(
    calls[0],
    "http://localhost:8123/api/history/period/2026-02-10T00:00:00.000Z" +
      "?end_time=2026-02-10T12%3A00%3A00.000Z" +
      "&filter_entity_id=sensor.grid_consumption_total"
  );
  assert.equal(calls.length, 1, "exactly one request");

  // The response is normalized to the replay shape.
  assert.deepEqual(result.states, [
    { entity_id: "sensor.grid_consumption_total", state: "104.5", last_updated_ts: 1770000000 },
  ]);
});
