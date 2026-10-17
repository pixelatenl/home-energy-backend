# Database phase: PostgreSQL/TimescaleDB

Design of the data layer for the home energy management system, matching the chosen architecture: devices, Home Assistant (collection layer), own backend, own frontend.

## 1. Key architecture decision

The Home Assistant recorder does not support PostgreSQL as a backend (only SQLite, MariaDB/MySQL). The database is therefore not fed through the recorder, but by the own backend, which subscribes to Home Assistant state changes via the WebSocket API and writes the sampled measurements to TimescaleDB. The recorder keeps running on the server as a buffer and presentation layer.

Subscribing over polling: with REST-API polling, every interval in which the backend is not running (restart, network outage) is lost. With a WebSocket subscription the worker receives every fact and samples it itself. Backfilling from the recorder database (states or long-term statistics) remains possible as a safety net for incidents.

Retention consequence: as long as the backend is not running yet, the recorder's default retention (roughly 10 days for states; long-term statistics considerably longer) limits how far back data can be recovered. To be safe, the recorder retention is temporarily raised to about 40 days (purge_keep_days: 40) so no gap occurs between this phase and the backend phase.

```mermaid
flowchart TD
  dev["Devices: P1, inverter, Anker, smart plugs"] --> ha["Home Assistant (collection layer)"]
  ha -->|"WebSocket subscribe on state changes"| be["Own backend (ingest-worker)"]
  be -->|"SQL writes: samples and deltas"| db[("TimescaleDB")]
  be -->|"forecasts"| db
  db --> api["Backend API + websocket"]
  api --> fe["Own frontend"]
```

## 2. Containers

Extension of the existing Docker setup on the server (static LAN address, configured via EMS_DB_BIND_IP):

| Container | Image | Port | Description |
|---|---|---|---|
| timescaledb | timescale/timescaledb:latest-pg16 | 5432 (LAN only) | database with the Timescale extension |
| db-backup | prodrigestivill/postgres-backup-local | none | nightly pg_dump with retention |
| ingest-worker | own image (Node) | none | reads HA, writes to the database |
| adminer/pgadmin | optional | temporary | administration and inspection during development |

Important: never expose the database port to the internet; restrict access to the Docker network and the LAN.

## 3. Data model

Principle: measurement samples and energy deltas are stored as separate facts, plus a dimension table for entities and separate tables for dynamic tariffs. Aggregations (hour, day) are computed by TimescaleDB itself via continuous aggregates, so the dashboard gets fast queries without the backend having to aggregate.

### 3.1 Dimension: entities

```sql
CREATE TABLE entity (
    id            SMALLSERIAL PRIMARY KEY,
    ha_entity_id  TEXT NOT NULL UNIQUE,   -- e.g. sensor.p1_active_power
    name          TEXT NOT NULL,           -- e.g. "House grid consumption"
    domain        TEXT NOT NULL,           -- grid, pv, battery, plug, weather, price, heat, gas, appliance
    device_class  TEXT,
    unit          TEXT,                    -- kW, kWh, W, EUR/kWh, %, m3
    is_counter    BOOLEAN NOT NULL DEFAULT FALSE,  -- true for cumulative meters (worker derives deltas)
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### 3.2 Facts: measurement samples and energy deltas (two hypertables)

Measurement samples (power, SOC, percentages) and energy are different facts: over power values an average is meaningful, over energy only a sum. Cumulative kWh meters do not produce sums but meter readings. The ingest worker therefore computes a delta in kWh per 5-minute interval and writes it to energy_delta; sum(delta_kwh) is then correct per hour, day and month. Meter resets (reading drops) are detected and yield delta 0.

All timestamps are rounded to the 5-minute bucket start, so the unique index (entity_id, ts) makes duplicates predictable and writes idempotent.

Gas entities use the same pipeline: a daily "gas consumption today" sensor with a midnight reset is a counter, its deltas land in energy_delta, and the continuous aggregates sum them like any other entity. For these entities the delta_kwh column holds cubic metres (m3); the unit in the entity table determines the interpretation, and sums must always be taken per entity, never mixed across entities.

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE measurement (          -- samples: kW, W, %, temperature
    entity_id  SMALLINT NOT NULL REFERENCES entity(id),
    ts         TIMESTAMPTZ NOT NULL,  -- bucket start (5 min)
    value      DOUBLE PRECISION NOT NULL,
    state      TEXT
);
SELECT create_hypertable('measurement', 'ts', chunk_time_interval => INTERVAL '1 day');
CREATE UNIQUE INDEX ux_measurement_entity_ts ON measurement (entity_id, ts DESC);

CREATE TABLE energy_delta (        -- energy per 5-minute interval (kWh, or m3 for gas entities)
    entity_id  SMALLINT NOT NULL REFERENCES entity(id),
    ts         TIMESTAMPTZ NOT NULL,  -- bucket start (5 min)
    delta_kwh  DOUBLE PRECISION NOT NULL,
    source     TEXT NOT NULL DEFAULT 'ingest'
);
SELECT create_hypertable('energy_delta', 'ts', chunk_time_interval => INTERVAL '1 day');
CREATE UNIQUE INDEX ux_energy_delta_entity_ts ON energy_delta (entity_id, ts DESC);
```

### 3.3 Fact: dynamic hourly electricity tariffs

```sql
CREATE TABLE price (
    ts        TIMESTAMPTZ NOT NULL,   -- hour start
    tariff    TEXT NOT NULL DEFAULT 'normaal',
    price_eur DOUBLE PRECISION NOT NULL,  -- EUR/kWh (incl. VAT and levies)
    source    TEXT NOT NULL DEFAULT 'zonneplan',
    PRIMARY KEY (ts, tariff)
);

SELECT create_hypertable('price', 'ts', chunk_time_interval => INTERVAL '1 month');
```

### 3.4 Fact: daily gas price

Zonneplan publishes one gas price per day, not per hour. The gas price is therefore stored in its own small table, keyed by local calendar day. It is deliberately separate from the price hypertable: the electricity price pipeline is validated and running, and the gas addition must not touch it.

The ingest worker applies the "first observation of the day wins" rule: the first value seen after local midnight is written for that day, later changes on the same day are ignored. This keeps the day price correct even if the source publishes the next day's price in the evening. The local day is determined in the configured time zone (EMS_TIMEZONE, default Europe/Amsterdam), independent of the server clock.

```sql
CREATE TABLE gas_price (
    day              DATE PRIMARY KEY,    -- local calendar day
    price_eur_per_m3 DOUBLE PRECISION NOT NULL,  -- EUR per m3 (incl. VAT and levies)
    source           TEXT NOT NULL DEFAULT 'zonneplan'
);
```

Plain table, no hypertable: roughly 365 rows per year, kept forever. Cost of gas per day is exact: sum of gas deltas for the day times the day price.

### 3.5 Continuous aggregates

Sums are computed exclusively over energy_delta; over measurement samples only average, maximum and minimum. The full SQL with all policies is in deploy/initdb/init.sql.

Daily-alignment rule: `energy_daily` buckets over local calendar days (Europe/Amsterdam), not UTC days. The timezone-aware `time_bucket('1 day', ts, 'Europe/Amsterdam')` makes every bucket span exactly one local day, so the daily totals line up with the Home Assistant Energy dashboard and with `gas_price.day`. Bucket starts are local midnight expressed in UTC (22:00Z in summer, 23:00Z in winter). At DST transitions a local day is 23 or 25 hours long and the bucket follows that actual interval; the repeated local hour at fall-back consists of distinct UTC instants, and because `ts` is a timestamptz nothing is counted twice. The refresh policy (start_offset 3 days, end_offset 1 hour, schedule_interval 1 hour, materialized_only) always covers the most recent complete local day, including the 25-hour day and the one-hour shift of the bucket boundary at a transition. Already deployed databases get this definition from deploy/migrations/002_energy_daily_local_day.sql, which drops and rebuilds the aggregate from the raw history. The hourly aggregates stay UTC-aligned: the Amsterdam offset is always a whole number of hours, so UTC hour buckets coincide with local hours.

```sql
-- Power: hourly avg/max/min
CREATE MATERIALIZED VIEW measurement_hourly
WITH (timescaledb.continuous) AS
SELECT entity_id, time_bucket('1 hour', ts) AS bucket,
       avg(value) AS avg_value, max(value) AS max_value, min(value) AS min_value
FROM measurement
GROUP BY entity_id, bucket
WITH NO DATA;

-- Energy: hourly and daily sums over the deltas
CREATE MATERIALIZED VIEW energy_hourly
WITH (timescaledb.continuous) AS
SELECT entity_id, time_bucket('1 hour', ts) AS bucket, sum(delta_kwh) AS kwh
FROM energy_delta
GROUP BY entity_id, bucket
WITH NO DATA;

-- Daily sums over the deltas: one local calendar day per bucket
CREATE MATERIALIZED VIEW energy_daily
WITH (timescaledb.continuous) AS
SELECT entity_id, time_bucket('1 day', ts, 'Europe/Amsterdam') AS bucket,
       sum(delta_kwh) AS kwh
FROM energy_delta
GROUP BY entity_id, bucket
WITH NO DATA;
```

## 4. Entities to be ingested

| Source | Entities (domain) | Interval | Type |
|---|---|---|---|
| Zonneplan One (P1) | Grid consumption, grid feed-in (grid) | 5 min | kWh cumulative + kW |
| Zonneplan One | Inverter yield (pv) | 5 min | kWh cumulative |
| Zonneplan official | Current electricity price (price) | hourly | EUR/kWh |
| Zonneplan official | Gas consumption today (gas) | daily sensor, hourly updates from the P1 reader | m3 per day, midnight reset |
| Zonneplan official | Gas price daily (price) | 1 day | EUR/m3 per day |
| Anker SOLIX Max | Charge/discharge power, SOC (battery) | 5 min | kW, % |
| Anker smart plugs | Power and energy per appliance (plug) | 5 min (Modbus provides 5 s, sampled to 5 min) | W, kWh |
| Forecast.Solar | Production forecast (pv) | 1 hour | kWh forecast |
| Evohome (later) | Heat demand per zone (heat) | 15 min | % |
| Daikin (later) | Heat pump consumption (heat) | 5-15 min | kWh, temperature |

Sampling strategy: 5 minutes for everything is more than sufficient for savings analysis and forecasts, and keeps storage small. The smart plugs deliver 5-second updates, but only a 5-minute sample is stored; raw 5-second data remains available in the recorder. Gas arrives with hourly resolution from the P1 reader at best; daily totals are sufficient for the hot water analysis.

## 5. Retention and backups

| Data | Retention |
|---|---|
| measurement and energy_delta (raw, 5 min) | 1 year, then drop policy (aggregates remain) |
| measurement_hourly and energy_hourly | 5 years |
| energy_daily | kept forever |
| price and gas_price | kept forever (small, and needed for correct savings lookback) |

```sql
SELECT add_retention_policy('measurement', INTERVAL '1 year');
SELECT add_retention_policy('energy_delta', INTERVAL '1 year');
SELECT add_retention_policy('measurement_hourly', INTERVAL '5 years');
SELECT add_retention_policy('energy_hourly', INTERVAL '5 years');
```

Backup policy: the entire database lives on a single SSD in a single container; without a backup a disk failure is fatal for the whole energy history. The db-backup container runs a nightly pg_dump with retention of 14 daily and 8 weekly dumps, stored outside the database volume. Because energy_daily, price and gas_price are kept forever, the history can be restored from any dump.

Storage estimate: roughly 25 entities x 288 samples per day x 365 days is about 2.6 million rows per year; with compression that is well under 1 GB per year. The gas_price table adds roughly 365 rows per year.

## 6. Incorporated points of attention

- Recorder retention (roughly 10 days for states) is the weak spot; the solution is temporarily raising purge_keep_days and running the ingest worker early, so the first backend run can backfill up to about 40 days.
- Hourly tariffs arrive automatically from the Zonneplan integration; a price is never entered manually.
- The gas price belongs to the local calendar day (Europe/Amsterdam), not to the UTC day; the worker decides the day via EMS_TIMEZONE, and the first observation of a day wins so early pre-publication cannot corrupt the day price.
- Monthly entities of the official Zonneplan integration are delayed and are ignored for the database; only the direct meters and the price entities are ingested.
- Like all "consumption today" sensors, the gas sensor resets at local midnight; the first interval of each day therefore has no delta (baseline). With hourly gas updates this means the 00:00 to 01:00 hour is structurally missing from the daily total; quantify this in the validation against the Zonneplan app.
- The database runs on the same server as Home Assistant; Docker network, no external access. Any external access via Tailscale then also applies to the database.

## 7. Database phase roadmap

1. Clone the repository on the server; deploy-db.sh runs the compose stack and the init script automatically.
2. Verify the schema (the script prints tables, views and hypertables).
3. Check what the recorder database actually contains: states (roughly 10 days by default) and long-term statistics (longer). Decide whether purge_keep_days is temporarily raised to 40 days.
4. Small ingest test: one entity (the Zonneplan price) via a WebSocket subscription, rounded to 5-minute buckets and written; then extend with the list from section 4.
5. For the first kWh meter: test the delta derivation, including a meter reset (reading drops) to verify it yields delta 0 instead of a negative value.
6. Check the continuous aggregates: energy_hourly and energy_daily are filling.
7. Verification: compare a daily total from energy_daily with the Home Assistant Energy dashboard; the deviation should stay below roughly 1 percent.
8. Only then build the backend phase (forecast model, savings logic) on this foundation.

Migrations for already deployed databases live in deploy/migrations/ and are idempotent; run them once via psql inside the database container.
