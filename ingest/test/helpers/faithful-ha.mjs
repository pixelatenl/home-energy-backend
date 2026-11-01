// A faithful fake of the HA REST history endpoint
// (GET /api/history/period/<start>) for the backfill regression pins. It
// emulates the API contract documented in the HA REST docs and implemented
// in components/history/__init__.py: by default the response contains, IN
// ADDITION to the recorded states inside the requested window, one synthetic
// state — the state current at the period start, with last_changed and
// last_updated clamped to exactly the requested start instant. When (and only
// when) the request URL carries the bare presence-based query parameter
// skip_initial_state, that synthetic state is left out.
//
// The caller provides the entity's full recorded timeline (states, ascending
// by ms) and an optional initialState carried by the synthetic state when no
// recorded state exists at or before the requested start.

/**
 * Build a fetchFn faking the HA history API for one entity's timeline.
 *
 * @param {object} options
 * @param {Array<{ms: number, state: string}>} options.states - recorded
 *   timeline, ascending by ms
 * @param {string} [options.initialState] - state current before the first
 *   recorded state (the synthetic start state carries it when nothing older
 *   is recorded); omit to return no synthetic state in that case
 * @returns {Function} fetchFn(url) matching the fetchEntityHistory contract
 */
export function faithfulHaHistoryFetchFn({ states, initialState }) {
  return async (url) => {
    const u = new URL(url);
    const startMs = Date.parse(u.pathname.slice("/api/history/period/".length));
    const endMs = Date.parse(u.searchParams.get("end_time"));
    const entityId = u.searchParams.get("filter_entity_id");
    const out = [];
    if (!u.searchParams.has("skip_initial_state")) {
      // The clamped start-of-period state: the state current at the start,
      // stamped exactly at the start instant.
      let current;
      for (const s of states) {
        if (s.ms <= startMs) current = s.state;
      }
      if (current === undefined) current = initialState;
      if (current !== undefined) {
        out.push({ entity_id: entityId, state: current, last_updated_ts: startMs / 1000 });
      }
    }
    for (const s of states) {
      if (s.ms >= startMs && s.ms < endMs) {
        out.push({ entity_id: entityId, state: s.state, last_updated_ts: s.ms / 1000 });
      }
    }
    return { ok: true, status: 200, json: async () => [out] };
  };
}
