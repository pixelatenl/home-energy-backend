-- Migration 001: daily gas price table (gas phase, revision 3).
--
-- Run once on an already deployed database (init.sql only runs automatically
-- on the first container start). Idempotent: safe to run more than once.
--
--   docker exec -i ems-timescaledb psql -U ems -d energy < deploy/migrations/001_gas_price.sql
--
-- No existing table or policy is modified; this migration is purely additive.

CREATE TABLE IF NOT EXISTS gas_price (
    day              DATE PRIMARY KEY,
    price_eur_per_m3 DOUBLE PRECISION NOT NULL, -- EUR per m3 including VAT and levies
    source           TEXT NOT NULL DEFAULT 'zonneplan'
);

-- Verify:
-- SELECT * FROM gas_price ORDER BY day DESC LIMIT 5;
