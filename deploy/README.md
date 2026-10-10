# Database (TimescaleDB) for the home energy management system

This folder contains the project's data layer: the TimescaleDB container and the nightly backup, fully self-contained in `deploy/`.

## Files

| File | Contents |
|---|---|
| docker-compose.yml | TimescaleDB container plus nightly backup container |
| deploy-db.sh | Deployment and verification script for the server |
| initdb/init.sql | Data model: hypertables, continuous aggregates, retention |
| .env.example | Example of the environment variables |

## Installation on the server

```bash
git pull
cd deploy
./deploy-db.sh
```

The script creates `deploy/.env` with a generated password and the server's LAN address, starts both containers, and verifies the created schema. Expected result: the tables entity, measurement, energy_delta and price; the continuous aggregates measurement_hourly, energy_hourly and energy_daily; and three hypertables.

## Important rules

- `deploy/.env` contains the database password and is never committed; add `.env` to the repository's .gitignore if it is not there yet.
- `deploy/data/` (database volume) and `deploy/backups/` also stay out of git.
- The database listens only on the server's LAN address and is never exposed to the internet.

## Design

The design decisions are documented in `docs/database-design.md` in this repository.
