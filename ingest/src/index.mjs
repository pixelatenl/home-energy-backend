// Ingest worker for the Energy Management System.
// Subscribes to state changes in Home Assistant, samples into 5-minute
// buckets and writes measurement samples, energy deltas, hourly electricity
// prices and the daily gas price to TimescaleDB. See deploy/README.md and
// docs/database-design.md.
//
// Requirements: Node.js >= 22, a running TimescaleDB stack (deploy/) and a
// long-lived access token from Home Assistant.

import { readFileSync } from "node:fs";
import { IngestEngine } from "./engine.mjs";
import { HaClient } from "./ha-client.mjs";
import { DbWriter } from "./db.mjs";

const log = (...args) => console.log(new Date().toISOString(), ...args);

// 1. Configuration: entities from ingest/config.json, secrets from the environment.
const config = JSON.parse(readFileSync(new URL("../config.json", import.meta.url), "utf8"));
const entities = config.entities ?? [];
if (entities.length === 0) {
  console.error("config.json does not contain any entities; nothing to do.");
  process.exit(1);
}

const HA_WS_URL = process.env.HA_WS_URL ?? "ws://localhost:8123/api/websocket";
const HA_TOKEN = process.env.HA_TOKEN;
if (!HA_TOKEN) {
  console.error("Set HA_TOKEN in the environment (long-lived access token from Home Assistant).");
  process.exit(1);
}

// Local time zone for daily facts (kind "gas_price"): the gas day price
// belongs to the local calendar day, independent of the server clock.
const EMS_TIMEZONE = process.env.EMS_TIMEZONE ?? "Europe/Amsterdam";

// 2. Set up the engine and the database writer.
const writer = new DbWriter(entities);
await writer.init();
log("Entities registered in the database:", entities.map((e) => e.haEntityId).join(", "));

const engine = new IngestEngine(writer, { timeZone: EMS_TIMEZONE });
engine.registerEntities(entities);

// 3. Translate incoming Home Assistant states into engine calls.
function feed(state) {
  const value = parseFloat(state.state);
  // Non-numeric states (unavailable, unknown, text) are ignored by the engine;
  // the timestamp comes from the measurement itself, not from the clock.
  engine.onState(state.entity_id, value, lastUpdatedMs(state));
}

function lastUpdatedMs(state) {
  const secs = state.last_updated_ts ?? state.last_updated;
  if (typeof secs === "number") return secs * 1000;
  return Date.now();
}

// 4. Connect to Home Assistant: baseline first, then the subscription.
const client = new HaClient({
  url: HA_WS_URL,
  token: HA_TOKEN,
  onStates: (states) => {
    for (const s of states) feed(s);
    log("Baseline states processed:", states.length);
  },
  onStateChanged: (newState) => feed(newState),
  log,
});
client.connect();

// 5. Close silent buckets every minute (entities without fresh events).
setInterval(() => engine.tick(), 60 * 1000);

// 6. Shut down gracefully.
process.on("SIGINT", async () => {
  log("Shutting down...");
  client.stop();
  await writer.close();
  process.exit(0);
});
