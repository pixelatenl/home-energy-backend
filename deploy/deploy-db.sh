#!/usr/bin/env bash
# Installs and starts the TimescaleDB stack and verifies the schema.
# Run on the server from the deploy/ directory: ./deploy-db.sh
set -euo pipefail
cd "$(dirname "$0")"

# 1. Create .env with a generated password if it does not exist yet.
if [ ! -f .env ]; then
  if command -v openssl >/dev/null 2>&1; then
    PW=$(openssl rand -base64 24 | tr -d '/+=')
  else
    PW=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
  fi
  BIND_IP=$(hostname -I | awk '{print $1}')
  if [ -z "${BIND_IP}" ]; then
    echo "Could not determine the LAN address; set EMS_DB_BIND_IP manually in .env."
    exit 1
  fi
  {
    echo "EMS_DB_PASSWORD=${PW}"
    echo "EMS_DB_BIND_IP=${BIND_IP}"
  } > .env
  chmod 600 .env
  echo ".env created (password generated, bind address: ${BIND_IP})."
else
  echo ".env already exists; using existing values."
fi

mkdir -p data/timescaledb backups

# 2. Start the containers.
docker compose up -d

# 3. Wait until the database is ready.
echo "Waiting for the database..."
i=0
until docker exec ems-timescaledb pg_isready -U ems -d energy >/dev/null 2>&1; do
  i=$((i+1))
  if [ "$i" -gt 60 ]; then
    echo "Database did not become ready within 120 seconds; check with: docker compose logs timescaledb"
    exit 1
  fi
  sleep 2
done
echo "Database is ready."

# 4. Verify the schema.
echo
echo "== Tables =="
docker exec ems-timescaledb psql -U ems -d energy -c "\dt"
echo "== Materialized views (continuous aggregates) =="
docker exec ems-timescaledb psql -U ems -d energy -c "SELECT view_name FROM timescaledb_information.continuous_aggregates ORDER BY view_name;"
echo "== Hypertables =="
docker exec ems-timescaledb psql -U ems -d energy -c "SELECT hypertable_name FROM timescaledb_information.hypertables;"
echo
echo "Done. Expected: tables entity, measurement, energy_delta, price;"
echo "views measurement_hourly, energy_hourly, energy_daily;"
echo "hypertables measurement, energy_delta, price."
