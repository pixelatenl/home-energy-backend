# Database (TimescaleDB) for the home energy management system

This folder contains the project's data layer: the TimescaleDB container and the nightly backup, fully self-contained in `deploy/`.

## Files
   File | Contents |
 |---|---|
 | docker-compose.yml | TimescaleDB container plus nightly backup container |
 | deploy-db.sh | Deployment and verification script for the server |
 | initdb/init.sql | Data model: hypertables, continuous aggregates, retention |
 | migrations/ | Schema changes after the initial deploy, applied manually |
 | .env.example | Example of the environment variables |
 | ems-backfill.service | oneshot unit for the backfill repair run |
 | ems-backfill.timer | hourly timer that starts the backfill service |

## Installation on the server

```bash
git pull
cd deploy
./deploy-db.sh

## Backfill repair (hourly timer)

The backfill mechanism repairs gaps in measurement, energy_delta, price and
gas_price from the Home Assistant recorder history and the Zonneplan tariff
forecast. See docs/backfill-design.md for the design. It runs as a oneshot
service started by an hourly timer; it can also be run manually:

    cd ../ingest
    node src/backfill/cli.mjs detect          # report only, no writes
    node src/backfill/cli.mjs repair          # full run
    node src/backfill/cli.mjs repair --dry-run

### 1. Migration

The backfill writes its audit rows into backfill_log. Run migration 003 once
(plain psql, idempotent):

    docker exec -i ems-timescaledb psql -U ems -d energy < migrations/003_backfill_log.sql

### 2. Install the timer

Adjust the user name and paths in ems-backfill.service (same placeholders as
the ingest worker unit), then:

    sudo cp ems-backfill.service ems-backfill.timer /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable --now ems-backfill.timer

The service reads the same ingest/.env as the worker (HA_WS_URL, HA_TOKEN,
PG*), runs `node src/backfill/cli.mjs repair --auto` and exits 0 for every
data outcome, including source-down (retried on the next hourly run).

### 3. Environment knobs (in ingest/.env)

| Variable | Default | Meaning |
|---|---|---|
| EMS_BACKFILL_LOOKBACK_DAYS | 7 | detection window in days (before now) |
| EMS_BACKFILL_MAX_ROWS_PER_RUN | 5000 | maximum replayed states per repair run |
| EMS_BACKFILL_MAX_GAP_BUCKETS | 36 | maximum span in buckets of a repairable gap |
| EMS_BACKFILL_RECORDER_RETENTION_DAYS | 40 | older uncovered hours/days are registered permanent |

Follow the runs with:

    journalctl -u ems-backfill.service -f
    cd ../ingest && node src/backfill/cli.mjs report --limit 20
