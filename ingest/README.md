# Ingest-worker

This worker is the bridge between Home Assistant and the TimescaleDB database. It subscribes to state changes via the WebSocket API, samples into 5-minute buckets and writes:

- measurement samples (power, SOC, percentages) to `measurement`;
- energy deltas per 5 minutes (derived from cumulative meter readings; a meter reset yields delta 0) to `energy_delta`;
- dynamic hourly electricity prices (Zonneplan) to `price`, rounded down to the start of the hour;
- the daily gas price (Zonneplan) to `gas_price`, one row per local calendar day.

## Requirements

- Node.js >= 22 (the worker uses the built-in WebSocket client; the only dependency is `pg`);
- the running TimescaleDB stack from `deploy/`;
- a long-lived access token from Home Assistant.

## Installation

```bash
cd ingest
npm install
cp .env.example .env   # fill in: HA_TOKEN, database details, optionally EMS_TIMEZONE
cp config.example.json config.json
```

You create a long-lived access token in Home Assistant via Profile, Security, at the bottom under Long-lived access tokens.

## Configuring entities

Look up the correct `entity_id` values with the helper script (the search term is optional):

```bash
HA_WS_URL=ws://localhost:8123/api/websocket HA_TOKEN=... npm run list -- zonneplan
```

Then add the entities to `config.json` with one of the four kinds:

```json
{
  "entities": [
    { "haEntityId": "sensor.current_electricity_price", "name": "Current electricity price", "domain": "price", "unit": "EUR/kWh", "kind": "price" },
    { "haEntityId": "sensor.house_power", "name": "House power", "domain": "grid", "unit": "kW", "kind": "sample" },
    { "haEntityId": "sensor.grid_consumption_total", "name": "Grid consumption total", "domain": "grid", "unit": "kWh", "kind": "counter" },
    { "haEntityId": "sensor.zonneplan_gas_price_daily", "name": "Gas price daily", "domain": "price", "unit": "EUR/m3", "kind": "gas_price" }
  ]
}
```

- `kind: "price"` writes to the `price` table (no entity registration needed);
- `kind: "sample"` writes the last value per bucket to `measurement`;
- `kind: "counter"` expects a cumulative reading and writes the difference per bucket to `energy_delta`. For devices, configure both the power entity (sample) and the energy entity (counter);
- `kind: "gas_price"` writes the value to the `gas_price` table, keyed by local calendar day (no entity registration needed). The first observation of a day wins; later changes on the same day are ignored, so an early price for the next day cannot corrupt the current day.

Gas note: a gas consumption entity (for example a daily "consumption today" sensor with a midnight reset) is a normal `counter`. The `delta_kwh` column then holds cubic metres (m3); the unit in the entity configuration determines the interpretation, and sums must always be taken per entity.

## Starting and testing

```bash
npm test          # unit tests of the ingest logic (no HA or database needed)
npm start         # start the worker (Ctrl-C shuts it down cleanly)
```

## Behavior and limitations

- After a restart, the first bucket of every counter is a baseline: no delta is written because the previous reading is unknown. Measurement samples are still recorded.
- Buckets without measurements get no row; gaps are visible as missing intervals (gapfill can be applied in queries).
- Every bucket is written at most once; repeats (restart, reconnect) are idempotent thanks to the unique indexes in the database.
- The gas price uses the time zone from `EMS_TIMEZONE` (default Europe/Amsterdam) to determine the calendar day, independent of the server clock.
- On connection loss or a Home Assistant restart, the worker reconnects with increasing intervals and subscribes again. Before opening the WebSocket it probes Home Assistant's HTTP endpoint with a short timeout, so a container proxy that accepts TCP while Home Assistant is still booting cannot leave the client hanging in the connecting state; the WebSocket connection itself also carries a connect timeout with a safe abort.
- A production deployment runs the worker as a systemd service (see below); containerization may follow in a later phase.

## Structure

| File | Contents |
|---|---|
| src/index.mjs | composition: configuration, connections, ticker |
| src/engine.mjs | pure sampling and delta logic (fully tested) |
| src/ha-client.mjs | WebSocket client with authentication, HTTP probe and reconnect |
| src/db.mjs | writes to TimescaleDB (idempotent) |
| src/buckets.mjs | time intervals (5 minutes, hour, local day) |
| src/list-entities.mjs | helper script to look up entity ids |
| test/engine.test.mjs | unit tests (node --test) |
| ems-ingest.service | systemd unit for production (placeholder user) |

## Running as a systemd service

For a production deployment, run the worker as a systemd service so it
starts at boot and restarts after failures.

1. Adjust the user name and paths in `ems-ingest.service` to your setup.
2. Copy it to `/etc/systemd/system/ems-ingest.service`.
3. Enable and start it:

   sudo systemctl daemon-reload
   sudo systemctl enable --now ems-ingest.service

4. Follow the logs with:

   journalctl -u ems-ingest.service -f

The service reads the same `.env` file as the manual setup, so no
configuration change is needed.