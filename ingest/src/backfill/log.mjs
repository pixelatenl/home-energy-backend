// backfill_log store: inserts plus the permanent-range registry
// (see docs/backfill-design.md, sections 3 and 8).
//
// Every repair run inserts exactly one row per processed entity and kind,
// with the honest outcome. Rows with outcome 'permanent' double as the
// permanent-range registry: repair runs skip gap ranges that such a row
// fully covers. Source-down is never permanent: repair runs only read the
// registry here; permanent registration happens when a source is confirmed
// unable to cover a range, which the next build block wires.

/**
 * Create the log store bound to a pg pool. The pool is owned by the caller.
 *
 * @param {object} pool - pg.Pool (connection details from the PG* environment)
 */
export function createBackfillLog(pool) {
  const iso = (ms) => new Date(ms).toISOString();

  return {
    /**
     * Insert one run result. entry: {haEntityId, kind, outcome,
     * rangeStartMs, rangeEndMs, rowsWritten, detail}.
     */
    async insert(entry) {
      await pool.query(
        `INSERT INTO backfill_log
           (ha_entity_id, kind, outcome, range_start, range_end, rows_written, detail)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [
          entry.haEntityId,
          entry.kind,
          entry.outcome,
          iso(entry.rangeStartMs),
          iso(entry.rangeEndMs),
          entry.rowsWritten ?? 0,
          entry.detail ?? null,
        ]
      );
    },

    /** Mark a range permanent: no more repair attempts for gaps it covers. */
    async markPermanent(entry) {
      await pool.query(
        `INSERT INTO backfill_log
           (ha_entity_id, kind, outcome, range_start, range_end, rows_written, detail)
         VALUES ($1, $2, 'permanent', $3, $4, 0, $5)`,
        [
          entry.haEntityId,
          entry.kind,
          iso(entry.rangeStartMs),
          iso(entry.rangeEndMs),
          entry.detail ?? null,
        ]
      );
    },

    /**
     * Is the given range permanent? True only when a permanent row fully
     * covers it; partial overlap does not skip a repairable gap.
     */
    async isPermanent(haEntityId, kind, rangeStartMs, rangeEndMs) {
      const res = await pool.query(
        `SELECT 1 FROM backfill_log
          WHERE ha_entity_id = $1 AND kind = $2 AND outcome = 'permanent'
            AND range_start <= $3 AND range_end >= $4
          LIMIT 1`,
        [haEntityId, kind, iso(rangeStartMs), iso(rangeEndMs)]
      );
      return (res.rowCount ?? 0) > 0;
    },

    /** The last limit log rows, newest first (CLI command "report"). */
    async recent(limit = 20) {
      const res = await pool.query(
        `SELECT id, ha_entity_id, kind, outcome, range_start, range_end,
                rows_written, detail, created_at
         FROM backfill_log
         ORDER BY id DESC
         LIMIT $1`,
        [Math.max(1, Number(limit) || 20)]
      );
      return res.rows;
    },
  };
}
