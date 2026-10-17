-- Migration 002: energy_daily aligned to local calendar days (Europe/Amsterdam).
--
-- Run once on an already deployed database (init.sql only runs automatically
-- on the first container start). Idempotent: safe to run more than once.
--
--   docker exec -i ems-timescaledb psql -U ems -d energy < deploy/migrations/002_energy_daily_local_day.sql
--
-- Run with plain psql (autocommit per statement). Do NOT use
-- -1 / --single-transaction and do not wrap the file in BEGIN/COMMIT:
-- refresh_continuous_aggregate cannot run inside a transaction block, so it
-- must stay a separate top-level statement. Every statement is idempotent,
-- so a failed run can simply be repeated once the cause is fixed.
--
-- What this changes and why:
--   The old energy_daily used time_bucket('1 day', ts), which buckets over
--   UTC days. In Europe/Amsterdam (UTC+2 in summer, UTC+1 in winter) the
--   local hours 00:00-01:59 (summer) respectively 00:00-00:59 (winter)
--   therefore landed in the previous UTC bucket, and the daily totals did
--   not line up with the Home Assistant Energy dashboard. The new aggregate
--   uses the timezone-aware form time_bucket('1 day', ts, 'Europe/Amsterdam'):
--   each bucket is one local calendar day, matching gas_price.day. The
--   hourly aggregates are untouched: the Amsterdam offset is always a whole
--   number of hours, so UTC hour buckets coincide with local hours.
--
-- DST correctness (verified on paper and, read-only, against the deployed
-- TimescaleDB 2.30.2):
--   * Fall-back, local day 2026-10-25 (03:00 CEST -> 02:00 CET at 01:00Z):
--     the day spans [2026-10-24 22:00Z, 2026-10-25 23:00Z) = 25 hours. The
--     repeated local hour 02:00-02:59 exists twice as distinct UTC instants
--     and both are assigned to the same local day. Because ts is a
--     timestamptz (an absolute instant), nothing is counted twice and no
--     energy is lost at the transition.
--   * Spring-forward, local day 2026-03-29 (02:00 CET -> 03:00 CEST at
--     01:00Z): the day spans [2026-03-28 23:00Z, 2026-03-29 22:00Z) =
--     23 hours. The skipped local hour 02:00-02:59 has no instants at all.
--   * The bucket start remains a timestamptz: local midnight expressed in
--     UTC, so 22:00Z in summer and 23:00Z in winter.
--
-- Refresh policy of the new aggregate (unchanged from the old one except that
-- the start offset is now checked against the DST shift):
--   start_offset 3 days, end_offset 1 hour, schedule_interval 1 hour,
--   materialized_only true (the default for new continuous aggregates on
--   this instance; the previous aggregate, created the same way, also had
--   materialized_only true). The refresh window [now - 3 days, now - 1 hour]
--   always fully contains the most recent complete local day, which can be
--   up to 25 hours long and whose bucket boundary shifts by one hour at a
--   DST transition; 3 days covers that with margin and also re-aggregates
--   late-arriving deltas. The hourly schedule keeps re-materializing the
--   trailing partial local day, exactly as before.
--
-- energy_daily has no retention policy (kept forever), so there is nothing
-- to restore after the drop. The old aggregate and its policy are removed
-- first; energy_hourly and measurement_hourly are deliberately untouched.

SELECT remove_continuous_aggregate_policy('energy_daily', if_exists => TRUE);

DROP MATERIALIZED VIEW IF EXISTS energy_daily;

CREATE MATERIALIZED VIEW IF NOT EXISTS energy_daily
WITH (timescaledb.continuous) AS
SELECT
    entity_id,
    time_bucket('1 day', ts, 'Europe/Amsterdam') AS bucket,
    sum(delta_kwh) AS kwh
FROM energy_delta
GROUP BY entity_id, bucket
WITH NO DATA;

-- Rebuild from the full raw energy_delta history. NULL, NULL refreshes the
-- entire range. This is the statement that may not run inside a transaction
-- block (see the header of this file).
CALL refresh_continuous_aggregate('energy_daily', NULL, NULL);

SELECT add_continuous_aggregate_policy('energy_daily',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE);

-- Verify:
-- SELECT view_definition FROM timescaledb_information.continuous_aggregates
--  WHERE view_name = 'energy_daily';
-- SELECT d.bucket, e.ha_entity_id, d.kwh FROM energy_daily d
--   JOIN entity e ON e.id = d.entity_id
--  ORDER BY d.bucket DESC, e.id LIMIT 20;
-- Bucket starts are now 22:00Z (CEST) / 23:00Z (CET) = local midnight, and
-- each bucket sums exactly one local calendar day.
