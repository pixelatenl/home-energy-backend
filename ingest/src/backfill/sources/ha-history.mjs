// HA REST history client for the backfill mechanism (see
// docs/backfill-design.md, section 5). The recorder keeps the raw state
// history and serves it over /api/history/period; this module fetches one
// entity's states for a range and normalizes them to what the orchestrator
// replays: {entity_id, state, last_updated_ts} with last_updated_ts in
// epoch seconds (the same field the worker's WebSocket states carry).
//
// Fail-soft by contract: every failure (network, HTTP status, bad payload)
// returns a typed {error: {code, message}} object. This module never throws
// past the plugin boundary, so a down Home Assistant degrades a repair run
// to the outcome "source-down" instead of crashing it.

const DEFAULT_TIMEOUT_MS = 30000;

/**
 * Derive the Home Assistant HTTP base from the WebSocket URL:
 * ws:// -> http://, wss:// -> https://, path dropped.
 * For example ws://localhost:8123/api/websocket -> http://localhost:8123.
 */
export function haHttpBaseFromWsUrl(wsUrl) {
  const u = new URL(wsUrl);
  const proto = u.protocol === "wss:" ? "https:" : u.protocol === "ws:" ? "http:" : u.protocol;
  return `${proto}//${u.host}`;
}

/**
 * Fetch the recorded states of one entity over the REST history API.
 *
 * @param {object} options
 * @param {string} options.httpBase - for example http://localhost:8123
 * @param {string} options.token - long-lived access token (Bearer)
 * @param {string} options.entityId - for example sensor.grid_consumption_total
 * @param {number} options.startMs - range start (inclusive), epoch milliseconds
 * @param {number} options.endMs - range end (exclusive), epoch milliseconds
 * @param {Function} [options.fetchFn] - injectable fetch (tests, verification)
 * @param {number} [options.timeoutMs=30000]
 * @returns {Promise<{states: Array<{entity_id: string, state: string, last_updated_ts: number}>} |
 *   {error: {code: string, message: string}}>} never throws
 */
export async function fetchEntityHistory(options) {
  const {
    httpBase,
    token,
    entityId,
    startMs,
    endMs,
    fetchFn = fetch,
    timeoutMs = DEFAULT_TIMEOUT_MS,
  } = options;
  const startISO = new Date(startMs).toISOString();
  const endISO = new Date(endMs).toISOString();
  const url = `${httpBase.replace(/\/$/, "")}/api/history/period/${startISO}/${endISO}?filter_entity_id=${encodeURIComponent(entityId)}`;
  try {
    const res = await fetchFn(url, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) {
      return {
        error: {
          code: `http-${res.status}`,
          message: `HA history request for ${entityId} failed with HTTP ${res.status}`,
        },
      };
    }
    const body = await res.json();
    // The history API answers with one array of states per requested entity.
    const raw = Array.isArray(body) ? (body[0] ?? []) : [];
    const states = raw
      .map(normalizeState)
      .filter((s) => s !== null)
      .sort((a, b) => a.last_updated_ts - b.last_updated_ts);
    return { states };
  } catch (e) {
    return {
      error: {
        code: "fetch-error",
        message: `HA history request for ${entityId} failed: ${e?.message ?? String(e)}`,
      },
    };
  }
}

/** Normalize a raw HA state into the replay shape; null if unusable. */
function normalizeState(s) {
  if (s === null || typeof s !== "object") return null;
  let ts = s.last_updated_ts;
  if (typeof ts !== "number" && s.last_updated !== undefined) {
    ts = Date.parse(s.last_updated) / 1000; // older HA versions use ISO strings
  }
  if (!Number.isFinite(ts)) return null;
  return { entity_id: s.entity_id, state: s.state, last_updated_ts: ts };
}
