-- Home energy management system: initial database migration (revision 2)
-- PostgreSQL 16 + TimescaleDB. Runs automatically on first container start
-- via docker-entrypoint-initdb.d.
--
-- Design principles:
--  * measurement samples and energy deltas are stored separately;
--    only sum() over energy_delta is meaningful.
--  * timestamps are rounded to 5-minute buckets by the ingest worker;
--    the unique index therefore makes duplicates predictable and idempotent.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. Dimension table: entities
CREATE TABLE IF NOT EXISTS entity (
    id            SMALLSERIAL PRIMARY KEY,
    ha_entity_id  TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    domain        TEXT NOT NULL,          -- grid | pv | battery | plug | weather | price | heat
    device_class  TEXT,
    unit          TEXT,                   -- kW | kWh | W | EUR/kWh | %
    is_counter    BOOLEAN NOT NULL DEFAULT FALSE,  -- cumulative kWh meter: worker derives deltas
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Fact: measurement samples (power, SOC, temperature, percentages)
CREATE TABLE IF NOT EXISTS measurement (
    entity_id  SMALLINT NOT NULL REFERENCES entity(id),
    ts         TIMESTAMPTZ NOT NULL,      -- bucket start (5 min)
    value      DOUBLE PRECISION NOT NULL,
    state      TEXT
);

SELECT create_hypertable('measurement', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS ux_measurement_entity_ts
    ON measurement (entity_id, ts DESC);

ALTER TABLE measurement SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'entity_id'
);
SELECT add_compression_policy('measurement', INTERVAL '7 days',
    if_not_exists => TRUE);

-- 3. Fact: energy deltas per 5-minute interval (kWh)
--    Written by the ingest worker: delta = last reading in bucket
--    minus last reading in the previous bucket. A meter reset (reading
--    drops) is detected and yields delta = 0.
CREATE TABLE IF NOT EXISTS energy_delta (
    entity_id  SMALLINT NOT NULL REFERENCES entity(id),
    ts         TIMESTAMPTZ NOT NULL,      -- bucket start (5 min)
    delta_kwh  DOUBLE PRECISION NOT NULL,
    source     TEXT NOT NULL DEFAULT 'ingest'
);

SELECT create_hypertable('energy_delta', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS ux_energy_delta_entity_ts
    ON energy_delta (entity_id, ts DESC);

ALTER TABLE energy_delta SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'entity_id'
);
SELECT add_compression_policy('energy_delta', INTERVAL '7 days',
    if_not_exists => TRUE);

-- 4. Fact: dynamic hourly tariffs (hypertable)
CREATE TABLE IF NOT EXISTS price (
    ts         TIMESTAMPTZ NOT NULL,      -- hour start
    tariff     TEXT NOT NULL DEFAULT 'normaal',
    price_eur  DOUBLE PRECISION NOT NULL, -- EUR/kWh including VAT and levies
    source     TEXT NOT NULL DEFAULT 'zonneplan',
    PRIMARY KEY (ts, tariff)
);

SELECT create_hypertable('price', 'ts',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE);

-- 5. Continuous aggregates
-- 5a. Power: hourly avg/max/min over measurement samples
CREATE MATERIALIZED VIEW IF NOT EXISTS measurement_hourly
WITH (timescaledb.continuous) AS
SELECT
    entity_id,
    time_bucket('1 hour', ts) AS bucket,
    avg(value) AS avg_value,
    max(value) AS max_value,
    min(value) AS min_value
FROM measurement
GROUP BY entity_id, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('measurement_hourly',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists     => TRUE);

-- 5b. Energy: hourly sums over the deltas (kWh)
CREATE MATERIALIZED VIEW IF NOT EXISTS energy_hourly
WITH (timescaledb.continuous) AS
SELECT
    entity_id,
    time_bucket('1 hour', ts) AS bucket,
    sum(delta_kwh) AS kwh
FROM energy_delta
GROUP BY entity_id, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('energy_hourly',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists     => TRUE);

-- 5c. Energy: daily totals (verification against the HA Energy dashboard)
CREATE MATERIALIZED VIEW IF NOT EXISTS energy_daily
WITH (timescaledb.continuous) AS
SELECT
    entity_id,
    time_bucket('1 day', ts) AS bucket,
    sum(delta_kwh) AS kwh
FROM energy_delta
GROUP BY entity_id, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('energy_daily',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE);

-- 6. Retention policies
SELECT add_retention_policy('measurement', INTERVAL '1 year',
    if_not_exists => TRUE);
SELECT add_retention_policy('energy_delta', INTERVAL '1 year',
    if_not_exists => TRUE);
SELECT add_retention_policy('measurement_hourly', INTERVAL '5 years',
    if_not_exists => TRUE);
SELECT add_retention_policy('energy_hourly', INTERVAL '5 years',
    if_not_exists => TRUE);
-- energy_daily and price: kept forever (no retention policy)

-- 7. Verification after the first ingest run (example)
-- SELECT e.name, d.bucket, d.kwh
-- FROM energy_daily d JOIN entity e ON e.id = d.entity_id
-- ORDER BY d.bucket DESC LIMIT 10;
