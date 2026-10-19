# Home Energy Backend

This repository (home-energy-backend) contains the backend and data layer of the home energy management system: a self-hosted, locally running platform that collects household consumption, solar production, battery state, heat zones and dynamic tariffs, stores them, and will eventually provide forecasts and savings insights. The dashboard (frontend) lives in the separate repository home-energy-dashboard.

## Current state

Both core components are complete and running in production on the home server:

- The TimescaleDB stack (deploy/): container, data model with hypertables and continuous aggregates, retention policies, nightly backups. Deployed and verified.
- The ingest worker (ingest/): running under systemd with the full entity list - grid import and export, PV production, battery charge/discharge and SOC, three smart plugs, the nine Evohome heat zones and the outdoor temperature, the Zonneplan hourly electricity price and the daily gas price. The full pipeline Home Assistant to TimescaleDB has been validated against the Home Assistant Energy dashboard (deviation within the expected margin; see ingest/README.md for the known baseline artifacts).

For now, the dashboard repository also carries the temporary Express API server (its server/ folder) that reads TimescaleDB directly. The permanent backend API will move into this repository; the SQL is isolated in one file so the migration is a copy operation (see the dashboard README's "Temporary layer" section).

## Architecture

```text
devices -> Home Assistant (collection layer) -> ingest-worker -> TimescaleDB -> backend API -> home-energy-dashboard
```

Home Assistant collects the data (P1 meter, inverter, Anker SOLIX and smart plugs, Evohome zones, Zonneplan tariffs). The ingest-worker subscribes to state changes via the WebSocket API, samples at 5-minute intervals, and writes measurement samples, energy deltas, hourly electricity tariffs and the daily gas price to TimescaleDB. Continuous aggregates maintain hourly and daily totals automatically; the daily aggregate follows Europe/Amsterdam local calendar days.

## Structure

| Folder | Contents |
|---|---|
| deploy/ | TimescaleDB stack: docker-compose, init script, migrations, deployment script, backup container |
| ingest/ | Ingest worker (Node 22): Home Assistant to TimescaleDB, with systemd unit |
| docs/ | Design documentation (database design) |

Planned additions: the permanent backend API with savings calculations and forecasts, taking over the role of the dashboard's temporary server.

## Server deployment (in order)

1. Database: see deploy/README.md; `deploy/deploy-db.sh` starts and verifies the stack. Existing databases apply schema changes via `deploy/migrations/` (never re-run 001).
2. Ingest worker: see ingest/README.md; after `npm install`, configure the token and entities, then run as a systemd service (`ingest/ems-ingest.service`).
3. Verification: compare daily totals from `energy_daily` with the Home Assistant Energy dashboard; the deviation should stay below roughly 1 percent (known artifacts: the midnight reset snippet and the post-restart baseline bucket, both documented in ingest/README.md).

## Rules for this repository

- This repository is public: never commit passwords, tokens, IP addresses or other personal data. All secrets live in .env files that are excluded by .gitignore and generated on the server.
- `.env` files (deploy/ and ingest/) contain passwords and tokens and are never committed.
- `deploy/data/` (database volume), `deploy/backups/` and `ingest/node_modules/` stay out of git.
- The database and the worker listen on the LAN only; no public exposure.

## Roadmap

1. Database phase: TimescaleDB container, data model, retention, backups - done and verified on the server.
2. Ingest worker: running in production under systemd with the full entity list (grid, PV, battery, plugs, heat zones, outdoor temperature, electricity and gas prices).
3. Recorder retention: raised to about 40 days during the transition period.
4. Backend API: savings calculations, forecasts (per roof plane, own forecasting model), AI context via Mistral - takes over from the dashboard's temporary server.
5. Web server that serves the dashboard and exposes the API on the LAN - the dashboard's deploy/ folder already handles this for the current setup; the permanent variant moves here.

## License

The code in this repository is licensed under the MIT license with one addition, see [LICENSE](LICENSE): derivative works of this codebase must credit Pixelate BV as the original author in their documentation.