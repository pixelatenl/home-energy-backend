# Database (TimescaleDB) for the home energy management system

This folder contains the project's data layer: the TimescaleDB container and the nightly backup, fully self-contained in `deploy/`.

## Files
   File | Contents |
 |---|---|
 | docker-compose.yml | TimescaleDB container |
 | deploy-db.sh | Deployment and verification script for the server |
 | initdb/init.sql | Data model: hypertables, continuous aggregates, retention |
 | migrations/ | Schema changes after the initial deploy, applied manually |
 | .env.example | Example of the environment variables |
 | ems-backfill.service | oneshot unit for the backfill repair run |
 | ems-backfill.timer | hourly timer that starts the backfill service |
 | backup.sh | nightly physical base backup + restore rehearsal script |
 | restore.sh | recovery procedure for restoring a base backup |
 | ems-backup.service | oneshot unit for the nightly backup run |
 | ems-backup.timer | daily 02:30 timer that starts the backup service |

## Installation on the server

```bash
git pull
cd deploy
./deploy-db.sh
```

## Nightly backup (physical base backup + restore rehearsal)

The database is backed up with `pg_basebackup`, not `pg_dump`: the old
db-backup container produced defective dumps (its vanilla pg_dump wrote
hypertable data into raw `_timescaledb_internal` chunk blocks and left
`public.price` and `public.energy_delta` empty). A physical base backup
captures the whole cluster and is correct by construction for TimescaleDB.

`backup.sh` runs nightly at 02:30 (see ems-backup.timer) and, per run:

1. takes a `pg_basebackup` inside the running container over the local
   socket (no network, no credentials),
2. streams it out and compresses it (`gzip -9`), then gates it through an
   integrity check (`gzip -t`),
3. performs a **restore rehearsal**: it boots the fresh archive in a scratch
   container (`ems-backup-rehearsal`) and compares row counts for price,
   gas_price, energy_delta, measurement, entity, backfill_log and
   solar_forecast_daily against the live database. Any mismatch, timeout or
   startup failure fails the run — a backup is only promoted if it has
   actually been restored and verified first,
4. only then promotes the archive into the retention tree and prunes.

Tree layout under `deploy/backups/base/`:

    backups/base/
      daily/    energy-YYYYMMDD.tar.gz   (last 14 kept)
      weekly/   every Monday             (last 8 kept)
      monthly/  1st of the month         (last 6 kept)
      last/     energy-latest.tar.gz     (refreshed every run)
      backup.log                            (timestamped, one line per step)

Failed runs leave no staging artifacts and never damage existing backups.
Follow the runs with `journalctl -u ems-backup.service -f` or
`tail -f backups/base/backup.log`.

Restore (the bad day):

    ./restore.sh backups/base/last/energy-latest.tar.gz /var/tmp/ems-restore

`restore.sh` extracts the archive to the given target directory (refusing a
non-empty target unless `--force`), fixes ownership for the container's
postgres user, and prints the exact follow-up commands: stop the stack, swap
`deploy/data/timescaledb`, `docker compose up -d`, and the verification
queries. It never touches `deploy/data/timescaledb` unless you name it as the
target explicitly.

Legacy `.sql.gz` files from the old db-backup container in
`backups/daily/`, `backups/weekly/`, `backups/monthly/` and `backups/last/`
are **known-defective** (empty price/energy_delta, raw chunk blocks) and are
pending a separate quarantine decision — do not restore from them. The
pre-backfill snapshots `energy_pre_backfill_20261030_1838.dump` and
`ha_v2_pre_backfill_20261030_1846.db` are unrelated to this chain and remain
untouched.

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
