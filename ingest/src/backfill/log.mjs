// backfill_log store: inserts plus the permanent-range registry
// (see docs/backfill-design.md, sections 3 and 8).
//
// Every repair run inserts exactly one row per processed entity and kind,
// with the honest outcome and honest bookkeeping: rows_written (rows really
// inserted) next to rows_missing (detected missing buckets this run did not
// write). Provenance is part of every row: run_kind (auto | cli) and source
// (which plugin supplied the rows). detail is a JSONB object with a
// human-readable summary field plus the counters.
//
// Rows with outcome 'permanent' double as the permanent-range registry:
// repair runs skip gap ranges that such a row fully covers. Source-down is
// never permanent: repair runs only read the registry here; permanent
// registration happens when a source is confirmed unable to cover a range,
// which the next build block wires.

/**
 * Create the log store bound to a pg pool. The pool is owned by the caller.
 *
 * @param {object} pool - pg.Pool (connection details from the PG* environment)
 */
export function createBackfillLog(pool) {
  const iso = (ms) => new Date(ms).toISOString();

  return {
    /**
     * Insert one run result. entry: {haEntityId, kind, runKind, source,
     * outcome, rangeStartMs, rangeEndMs, rowsWritten, rowsMissing, detail}.
     * detail is a plain object (JSONB) or null.
     */
    async insert(entry) {
      await pool.query(
        `INSERT INTO backfill_log
           (ha_entity_id, kind, run_kind, source, outcome, range_start, range_end,
            rows_written, rows_missing, detail)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [
          entry.haEntityId,
          entry.kind,
          entry.runKind ?? "cli",
          entry.source ?? "none",
          entry.outcome,
          iso(entry.rangeStartMs),
          iso(entry.rangeEndMs),
          entry.rowsWritten ?? 0,
          entry.rowsMissing ?? 0,
          entry.detail ?? null,
        ]
      );
    },

    /**
     * Mark a range permanent: no more repair attempts for gaps it covers.
     * Carries the same provenance fields as insert.
     */
    async markPermanent(entry) {
      await pool.query(
        `INSERT INTO backfill_log
           (ha_entity_id, kind, run_kind, source, outcome, range_start, range_end,
            rows_written, rows_missing, detail)
         VALUES ($1, $2, $3, $4, 'permanent', $5, $6, 0, $7, $8)`,
        [
          entry.haEntityId,
          entry.kind,
          entry.runKind ?? "cli",
          entry.source ?? "none",
          iso(entry.rangeStartMs),
          iso(entry.rangeEndMs),
          entry.rowsMissing ?? 0,
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

    /**
     * Permanent ranges overlapping the given window (ascending), for
     * detection: gaps inside them are excluded before repair. Overlap, not
     * full cover: the caller decides per hour/day/bucket whether a range
     * fully covers the item.
     */
    async permanentRanges(haEntityId, kind, fromMs, toMs) {
      const res = await pool.query(
        `SELECT range_start, range_end FROM backfill_log
          WHERE ha_entity_id = $1 AND kind = $2 AND outcome = 'permanent'
            AND range_start < $4 AND range_end > $3
          ORDER BY range_start`,
        [haEntityId, kind, iso(fromMs), iso(toMs)]
      );
      return res.rows.map((r) => ({
        startMs: new Date(r.range_start).getTime(),
        endMs: new Date(r.range_end).getTime(),
      }));
    },

    /** The last limit log rows, newest first (CLI command "report"). */
    async recent(limit = 20) {
      const res = await pool.query(
        `SELECT id, ha_entity_id, kind, run_kind, source, outcome, range_start, range_end,
                rows_written, rows_missing, detail, created_at
         FROM backfill_log
         ORDER BY id DESC
         LIMIT $1`,
        [Math.max(1, Number(limit) || 20)]
      );
      return res.rows;
    },
  };
}
