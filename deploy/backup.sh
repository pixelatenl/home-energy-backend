#!/usr/bin/env bash
#
# Nightly physical backup for the EMS TimescaleDB, with a restore rehearsal.
#
# Replaces the old pg_dump-based db-backup container: pg_dump writes hypertable
# data into raw _timescaledb_internal chunk blocks and leaves public.price and
# public.energy_delta empty, so its .sql.gz files are defective. pg_basebackup
# captures the physical cluster and is correct by construction for TimescaleDB.
#
# Chain per run:
#   1. pg_basebackup inside the live container (local socket, no network).
#   2. Stream out, gzip -9, integrity check (gzip -t).
#   3. Restore rehearsal: boot the archive in a scratch container and compare
#      row counts against the live database. Any mismatch fails the run.
#   4. Only then promote the archive to daily/weekly/monthly/last + retention.
#
# A failed run leaves no staging artifacts behind and never touches existing
# backups. Run from anywhere; all paths resolve relative to this script.
#
# shellcheck disable=SC2312  # command substitution exit codes are handled

set -Eeuo pipefail

# ---------------------------------------------------------------------------
# Configuration (defaults match the current deployment).
# ---------------------------------------------------------------------------
DB_CONTAINER="ems-timescaledb"
DB_USER="ems"
DB_NAME="energy"
DB_TABLES=(price gas_price energy_delta measurement entity backfill_log solar_forecast_daily)

BACKUP_DIR="backups/base"          # relative to the deploy/ directory
DAILY_SUBDIR="daily"
WEEKLY_SUBDIR="weekly"
MONTHLY_SUBDIR="monthly"
LAST_SUBDIR="last"
LOG_FILE="$BACKUP_DIR/backup.log"

KEEP_DAILY=14
KEEP_WEEKLY=8
KEEP_MONTHLY=6

STAGING_DIR="/tmp/ems-basebackup"  # staging dir inside the DB container
REHEARSAL_CONTAINER="ems-backup-rehearsal"
REHEARSAL_IMAGE="timescale/timescaledb:latest-pg16"
REHEARSAL_STARTUP_TIMEOUT=120      # seconds to wait for pg_isready

# ---------------------------------------------------------------------------
# Setup.
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$SCRIPT_DIR"

DAILY_PATH="$BACKUP_DIR/$DAILY_SUBDIR"
WEEKLY_PATH="$BACKUP_DIR/$WEEKLY_SUBDIR"
MONTHLY_PATH="$BACKUP_DIR/$MONTHLY_SUBDIR"
LAST_PATH="$BACKUP_DIR/$LAST_SUBDIR"

TMP_ARCHIVE=""
REHEARSAL_STAGE=""

log() {
    local line
    line="$(date '+%Y-%m-%dT%H:%M:%S%z') $*"
    line="${line//$'\n'/; }"
    echo "$line"
    mkdir -p -- "$BACKUP_DIR"
    echo "$line" >> "$LOG_FILE"
}

size_of() {
    # Size in bytes, portable across GNU and BSD userlands.
    wc -c < "$1" | tr -d '[:space:]'
}

die() {
    log "FAILED: $*"
    exit 1
}

# shellcheck disable=SC2329  # invoked via the EXIT trap
cleanup() {
    # Teardown must be unconditional and tolerant: it runs on success and on
    # every failure path (including a dead DB container).
    docker rm -f "$REHEARSAL_CONTAINER" >/dev/null 2>&1 || true
    docker exec "$DB_CONTAINER" rm -rf "$STAGING_DIR" >/dev/null 2>&1 || true
    if [ -n "$REHEARSAL_STAGE" ] && [ -d "$REHEARSAL_STAGE" ]; then
        rm -rf -- "$REHEARSAL_STAGE" || true
    fi
    if [ -n "$TMP_ARCHIVE" ] && [ -e "$TMP_ARCHIVE" ]; then
        rm -f -- "$TMP_ARCHIVE" || true
    fi
    return 0
}
trap cleanup EXIT

# Single query so all live counts come from one snapshot; captured immediately
# after pg_basebackup returns to keep the live-vs-rehearsal race minimal.
count_query() {
    local t query=""
    for t in "${DB_TABLES[@]}"; do
        query+="SELECT '$t' AS table_name, count(*) AS rows FROM $t UNION ALL "
    done
    echo "SELECT * FROM (${query% UNION ALL }) AS counts ORDER BY table_name"
}

parse_counts() {
    # $1 = "table count" lines, $2 = name of the output associative array.
    local input=$1
    local -n out=$2
    local t c
    out=()
    while read -r t c; do
        [ -n "$t" ] || continue
        out["$t"]=$c
    done <<< "$input"
    [ "${#out[@]}" -gt 0 ] || die "row counts: query returned no rows"
}

row_counts() {
    # $1 = container name. Output: "<table> <count>" lines.
    docker exec "$1" psql -U "$DB_USER" -d "$DB_NAME" -At -F ' ' \
        -c "$(count_query)"
}

prune() {
    # $1 = directory, $2 = number of newest archives to keep.
    local dir=$1 keep=$2 files=() f
    shopt -s nullglob
    files=("$dir"/energy-*.tar.gz)
    shopt -u nullglob
    if (( ${#files[@]} > keep )); then
        mapfile -t files < <(printf '%s\n' "${files[@]}" | sort)
        for f in "${files[@]:0:${#files[@]}-keep}"; do
            rm -f -- "$f"
            log "retention: removed $f"
        done
    fi
}

# ---------------------------------------------------------------------------
# Preflight.
# ---------------------------------------------------------------------------
log "backup: run started"
[ "$(docker inspect -f '{{.State.Running}}' "$DB_CONTAINER" 2>/dev/null || true)" = "true" ] \
    || die "preflight: container $DB_CONTAINER is not running"

mkdir -p -- "$DAILY_PATH" "$WEEKLY_PATH" "$MONTHLY_PATH" "$LAST_PATH"

# ---------------------------------------------------------------------------
# 1. Physical base backup inside the live container (local socket only).
# ---------------------------------------------------------------------------
log "backup: removing previous staging dir $STAGING_DIR in $DB_CONTAINER"
docker exec "$DB_CONTAINER" rm -rf "$STAGING_DIR"

log "backup: running pg_basebackup inside $DB_CONTAINER"
docker exec "$DB_CONTAINER" pg_basebackup \
    -U "$DB_USER" -D "$STAGING_DIR" -Fp -Xs --checkpoint=fast

# Snapshot the live row counts right after the backup finished; the rehearsal
# database will reflect exactly this point in time.
declare -A LIVE_COUNTS REHEARSAL_COUNTS

LIVE_OUT="$(row_counts "$DB_CONTAINER")"
parse_counts "$LIVE_OUT" LIVE_COUNTS
log "backup: live row counts: $LIVE_OUT"

# ---------------------------------------------------------------------------
# 2. Stream the result out and compress on the host.
# ---------------------------------------------------------------------------
TMP_ARCHIVE="$(mktemp "${TMPDIR:-/tmp}/ems-basebackup.XXXXXX")"
log "backup: streaming staging dir to $TMP_ARCHIVE"
docker exec "$DB_CONTAINER" tar -C "$STAGING_DIR" -cf - . \
    | gzip -9 > "$TMP_ARCHIVE"
docker exec "$DB_CONTAINER" rm -rf "$STAGING_DIR"

# ---------------------------------------------------------------------------
# 3. Integrity gate.
# ---------------------------------------------------------------------------
log "backup: verifying gzip integrity of $TMP_ARCHIVE"
gzip -t "$TMP_ARCHIVE"
log "backup: archive size $(size_of "$TMP_ARCHIVE") bytes"

# ---------------------------------------------------------------------------
# 4. Restore rehearsal in a scratch container.
# ---------------------------------------------------------------------------
REHEARSAL_STAGE="$(mktemp -d "$BACKUP_DIR/.rehearsal.XXXXXX")"
log "rehearsal: extracting archive to $REHEARSAL_STAGE"
tar -xzf "$TMP_ARCHIVE" -C "$REHEARSAL_STAGE"

# Extraction is root-owned (or caller-owned); give the container's postgres
# user (numeric uid in timescale/timescaledb:latest-pg16) ownership.
log "rehearsal: fixing ownership for postgres in $REHEARSAL_IMAGE"
docker run --rm \
    -v "$REHEARSAL_STAGE":/var/lib/postgresql/data \
    "$REHEARSAL_IMAGE" chown -R postgres:postgres /var/lib/postgresql/data

log "rehearsal: starting scratch container $REHEARSAL_CONTAINER"
docker run -d --name "$REHEARSAL_CONTAINER" \
    -v "$REHEARSAL_STAGE":/var/lib/postgresql/data \
    "$REHEARSAL_IMAGE" >/dev/null

deadline=$((SECONDS + REHEARSAL_STARTUP_TIMEOUT))
until docker exec "$REHEARSAL_CONTAINER" pg_isready -U "$DB_USER" -d "$DB_NAME" -q \
        2>/dev/null; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$REHEARSAL_CONTAINER" 2>/dev/null || true)" != "true" ]; then
        die "rehearsal: container $REHEARSAL_CONTAINER exited during startup"
    fi
    if (( SECONDS >= deadline )); then
        die "rehearsal: pg_isready did not succeed within ${REHEARSAL_STARTUP_TIMEOUT}s"
    fi
    sleep 2
done
log "rehearsal: database is ready"

REHEARSAL_OUT="$(row_counts "$REHEARSAL_CONTAINER")"
parse_counts "$REHEARSAL_OUT" REHEARSAL_COUNTS

REHEARSAL_OK=1
for table in "${DB_TABLES[@]}"; do
    live_count="${LIVE_COUNTS[$table]:-}"
    rehearsal_count="${REHEARSAL_COUNTS[$table]:-}"
    if [ -n "$live_count" ] && [ "$live_count" = "$rehearsal_count" ]; then
        log "rehearsal: $table OK ($live_count rows)"
    else
        log "rehearsal: $table MISMATCH (live '$live_count' vs rehearsal '$rehearsal_count')"
        REHEARSAL_OK=0
    fi
done
[ "$REHEARSAL_OK" -eq 1 ] || die "rehearsal: row counts do not match the live database"

docker rm -f "$REHEARSAL_CONTAINER" >/dev/null
rm -rf -- "$REHEARSAL_STAGE"
REHEARSAL_STAGE=""

# ---------------------------------------------------------------------------
# 5. Promotion: only a verified archive reaches the retention tree.
# ---------------------------------------------------------------------------
DATE="$(date '+%Y%m%d')"
ARCHIVE_NAME="energy-$DATE.tar.gz"
FINAL_ARCHIVE="$DAILY_PATH/$ARCHIVE_NAME"

log "promotion: moving $TMP_ARCHIVE to $FINAL_ARCHIVE"
mv -f -- "$TMP_ARCHIVE" "$FINAL_ARCHIVE"
TMP_ARCHIVE=""
log "backup: archive stored at $FINAL_ARCHIVE ($(size_of "$FINAL_ARCHIVE") bytes)"

if [ "$(date '+%u')" -eq 1 ]; then
    log "promotion: Monday, copying to $WEEKLY_PATH/$ARCHIVE_NAME"
    cp -f -- "$FINAL_ARCHIVE" "$WEEKLY_PATH/$ARCHIVE_NAME"
fi
if [ "$(date '+%d')" -eq 01 ]; then
    log "promotion: first day of the month, copying to $MONTHLY_PATH/$ARCHIVE_NAME"
    cp -f -- "$FINAL_ARCHIVE" "$MONTHLY_PATH/$ARCHIVE_NAME"
fi

log "promotion: refreshing $LAST_PATH/energy-latest.tar.gz"
cp -f -- "$FINAL_ARCHIVE" "$LAST_PATH/energy-latest.tar.gz"

log "retention: keeping $KEEP_DAILY daily, $KEEP_WEEKLY weekly, $KEEP_MONTHLY monthly"
prune "$DAILY_PATH" "$KEEP_DAILY"
prune "$WEEKLY_PATH" "$KEEP_WEEKLY"
prune "$MONTHLY_PATH" "$KEEP_MONTHLY"

log "backup: PASS"
exit 0
