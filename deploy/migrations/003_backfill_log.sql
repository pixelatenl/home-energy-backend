-- Migration 003: backfill_log table (backfill phase).
--
-- Run once on an already deployed database (init.sql only runs automatically
-- on the first container start). Idempotent: safe to run more than once.
--
--   docker exec -i ems-timescaledb psql -U ems -d energy < deploy/migrations/003_backfill_log.sql
--
-- No existing table or policy is modified; this migration is purely additive.
-- Plain psql execution (autocommit per statement), no transaction wrapper,
-- in line with migrations 001 and 002.
--
-- backfill_log is the audit trail of the backfill mechanism (see
-- docs/backfill-design.md, sections 3 and 8): one row per entity and kind
-- per repair run, with the honest outcome. The outcome value 'permanent'
-- doubles as the permanent-range registry: repair runs skip gap ranges that
-- a permanent row fully covers, so futile retries stop.

CREATE TABLE IF NOT EXISTS backfill_log (
    id            BIGSERIAL PRIMARY KEY,
    ha_entity_id  TEXT NOT NULL,           -- configured entity, also for kinds without an entity table row
    kind          TEXT NOT NULL,           -- sample | counter | price | gas_price
    outcome       TEXT NOT NULL CHECK (outcome IN ('repaired', 'partial', 'source-down', 'permanent', 'no-gap')),
    range_start   TIMESTAMPTZ NOT NULL,    -- union of the entity's repairable gaps this run (window when none)
    range_end     TIMESTAMPTZ NOT NULL,
    rows_written  INTEGER NOT NULL DEFAULT 0,
    detail        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_backfill_log_entity ON backfill_log (ha_entity_id, kind, id DESC);

-- Verify:
-- SELECT id, ha_entity_id, kind, outcome, rows_written, created_at
--   FROM backfill_log ORDER BY id DESC LIMIT 20;
