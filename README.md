# Home Energy Backend

This repository (home-energy-backend) contains the backend and data layer of the home energy management system: a self-hosted, locally running platform that collects household consumption, solar production, battery state and dynamic tariffs, stores them, and will eventually provide forecasts and savings insights. The dashboard (frontend) lives in the separate repository home-energy-dashboard and talks only to this backend's API.

## Architecture

```text
devices -> Home Assistant (collection layer) -> ingest-worker -> TimescaleDB -> backend API -> home-energy-dashboard
```

Home Assistant collects the data (P1 meter, inverter, Anker SOLIX and smart plugs, Zonneplan hourly tariffs). The ingest-worker subscribes to state changes via the WebSocket API, samples at 5-minute intervals, and writes measurement samples, energy deltas and hourly tariffs to TimescaleDB. Continuous aggregates maintain hourly and daily totals automatically.

## Structure

| Folder | Contents |
|---|---|
| deploy/ | TimescaleDB stack: docker-compose, init script, deployment script, backup container |
| ingest/ | Ingest worker (Node 22): Home Assistant to TimescaleDB |
| docs/ | Design documentation (database phase) |

Planned additions: the backend API with savings calculations and forecasts, and the web server that statically serves the built dashboard.

## Server deployment (in order)

1. Database: see deploy/README.md; `deploy/deploy-db.sh` starts and verifies the stack.
2. Ingest worker: see ingest/README.md; after `npm install`, configure the token and entities, then start with `npm start` (systemd unit or container comes later).
3. Verification: compare daily totals from `energy_daily` with the Home Assistant Energy dashboard; the deviation should stay below roughly 1 percent.

## Rules for this repository

- This repository is public: never commit passwords, tokens, IP addresses or other personal data. All secrets live in .env files that are excluded by .gitignore and generated on the server.
- `.env` files (deploy/ and ingest/) contain passwords and tokens and are never committed.
- `deploy/data/` (database volume), `deploy/backups/` and `ingest/node_modules/` stay out of git.
- The database and the worker listen on the LAN only; no public exposure.

## Roadmap

1. Database phase: TimescaleDB container, data model, retention, backups (done once verified on the server).
2. Ingest worker v0.1: first entity is the Zonneplan price; the full entity list follows.
3. Tune the recorder retention (roughly 10 days by default; temporarily raise to about 40 days).
4. Backend API: savings calculations, forecasts (Forecast.Solar per roof plane, own forecasting model), AI context via Mistral.
5. Web server that serves the dashboard and exposes the API and websocket on the LAN.
