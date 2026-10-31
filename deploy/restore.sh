#!/usr/bin/env bash
#
# Recovery procedure for the EMS TimescaleDB physical base backups.
#
# Usage: restore.sh <archive> <target-data-dir> [--force]
#
# Extracts a backups/base/.../energy-*.tar.gz into <target-data-dir> and makes
# it readable by the postgres user of timescale/timescaledb:latest-pg16. It
# then prints the exact follow-up commands for the bad day. This script only
# writes to the directory explicitly named as <target-data-dir>; it never
# touches deploy/data/timescaledb unless you name it yourself.
#
# shellcheck disable=SC2312

set -Eeuo pipefail

REHEARSAL_IMAGE="timescale/timescaledb:latest-pg16"
DB_USER="ems"
DB_NAME="energy"

die() {
    echo "restore: FAILED: $*" >&2
    exit 1
}

usage() {
    echo "usage: $0 <archive> <target-data-dir> [--force]" >&2
    exit 2
}

FORCE=0
ARGS=()
for arg in "$@"; do
    case "$arg" in
        --force) FORCE=1 ;;
        -h|--help) usage ;;
        *) ARGS+=("$arg") ;;
    esac
done
[ "${#ARGS[@]}" -eq 2 ] || usage

ARCHIVE="${ARGS[0]}"
TARGET_DIR="${ARGS[1]}"

[ -f "$ARCHIVE" ] || die "archive '$ARCHIVE' does not exist"

TARGET_DIR="${TARGET_DIR%/}"
[ -n "$TARGET_DIR" ] || die "target directory must not be empty"
case "$(cd -- "$TARGET_DIR" 2>/dev/null && pwd)" in
    /|/home|/Users|/tmp) die "refusing to use '$TARGET_DIR' as target" ;;
esac

if [ -d "$TARGET_DIR" ] && [ -n "$(ls -A -- "$TARGET_DIR" 2>/dev/null)" ]; then
    if [ "$FORCE" -eq 1 ]; then
        echo "restore: --force given, removing existing contents of $TARGET_DIR"
        find "$TARGET_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    else
        die "target '$TARGET_DIR' exists and is not empty (pass --force to overwrite)"
    fi
fi

mkdir -p -- "$TARGET_DIR"
echo "restore: extracting $ARCHIVE into $TARGET_DIR"
tar -xzf "$ARCHIVE" -C "$TARGET_DIR"
[ -f "$TARGET_DIR/PG_VERSION" ] || die "extraction did not produce a PGDATA (PG_VERSION missing)"

# Extraction is root-owned (or caller-owned); give the container postgres user
# (numeric uid in the image) ownership so it can start on this data directory.
echo "restore: setting postgres ownership for $TARGET_DIR"
docker run --rm \
    -v "$(cd -- "$TARGET_DIR" && pwd)":/var/lib/postgresql/data \
    "$REHEARSAL_IMAGE" chown -R postgres:postgres /var/lib/postgresql/data

STAMP="$(date '+%Y%m%d-%H%M%S')"
cat <<EOF

restore: extraction complete. Data directory ready at:
    $TARGET_DIR

Follow these commands on the server to finish the recovery:

  # 1. Stop the ingest worker and the database stack.
  sudo systemctl stop ems-ingest.service
  cd /home/YOUR_USER/home-energy-backend/deploy
  docker compose stop timescaledb

  # 2. Swap the data directory (keep the broken one for forensics).
  mv data/timescaledb "data/timescaledb.broken-$STAMP"
  mv "$TARGET_DIR" data/timescaledb

  # 3. Start the stack again and wait for the health check.
  docker compose up -d
  watch -n5 'docker inspect --format "{{.State.Health.Status}}" ems-timescaledb'

  # 4. Verification queries.
  docker exec ems-timescaledb pg_isready -U $DB_USER -d $DB_NAME
  docker exec ems-timescaledb psql -U $DB_USER -d $DB_NAME -c "
      SELECT count(*) AS price FROM price;
      SELECT count(*) AS gas_price FROM gas_price;
      SELECT count(*) AS energy_delta FROM energy_delta;
      SELECT count(*) AS measurement FROM measurement;
      SELECT count(*) AS entity FROM entity;
      SELECT count(*) AS backfill_log FROM backfill_log;
      SELECT count(*) AS solar_forecast_daily FROM solar_forecast_daily;"
  docker exec ems-timescaledb psql -U $DB_USER -d $DB_NAME -c "
      SELECT max(time) FROM measurement;"

  # 5. When satisfied, restart the ingest worker.
  sudo systemctl start ems-ingest.service

  # 6. Run a restore rehearsal against the recovered database:
  ./backup.sh

EOF
exit 0
