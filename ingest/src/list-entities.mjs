// Utility: list all Home Assistant entities to find the correct
// haEntityId values for ingest/config.json.
// Usage: HA_WS_URL=... HA_TOKEN=... node src/list-entities.mjs [search term]
// Without a search term all entities are listed; otherwise only entities
// whose id or name contains the search term (case insensitive).

import { HaClient } from "./ha-client.mjs";

const url = process.env.HA_WS_URL ?? "ws://localhost:8123/api/websocket";
const token = process.env.HA_TOKEN;
if (!token) {
  console.error("Set HA_TOKEN in the environment.");
  process.exit(1);
}
const searchTerm = (process.argv[2] ?? "").toLowerCase();

const client = new HaClient({
  url,
  token,
  onStates: (states) => {
    for (const s of states) {
      const name = s.attributes?.friendly_name ?? "";
      const hay = (s.entity_id + " " + name).toLowerCase();
      if (searchTerm && !hay.includes(searchTerm)) continue;
      const unit = s.attributes?.unit_of_measurement ?? "";
      console.log(`${s.entity_id}  =  ${s.state} ${unit}  (${name})`);
    }
    client.stop();
    process.exit(0);
  },
  onStateChanged: () => {},
  log: () => {},
});
client.connect();
