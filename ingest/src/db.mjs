// DbWriter: writes the results of the IngestEngine to TimescaleDB.
// Connection details and credentials come from the PG* environment variables
// (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE); the pg library reads these itself.
//
// Write rules:
// - entities are registered at startup (upsert on ha_entity_id);
// - measurement and energy_delta are written idempotently
//   (ON CONFLICT DO NOTHING: an already written bucket remains unchanged);
// - price is upserted: a newer price for the same hour wins.

import pg from "pg";

export class DbWriter {
  /** @param {Array} entities - the entities from the configuration */
  constructor(entities) {
    this.pool = new pg.Pool();
    /** @type {Map<string, number>} haEntityId -> entity.id */
    this.ids = new Map();
    this.entities = entities;
  }

  async init() {
    for (const e of this.entities) {
      if (e.kind === "price") continue; // prices are not stored in the entity table
      const res = await this.pool.query(
        `INSERT INTO entity (ha_entity_id, name, domain, unit, is_counter)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (ha_entity_id) DO UPDATE
           SET name = EXCLUDED.name,
               domain = EXCLUDED.domain,
               unit = EXCLUDED.unit,
               is_counter = EXCLUDED.is_counter
         RETURNING id`,
        [e.haEntityId, e.name ?? e.haEntityId, e.domain ?? "other", e.unit ?? null, e.kind === "counter"]
      );
      this.ids.set(e.haEntityId, res.rows[0].id);
    }
  }

  writeMeasurement(meta, tsMs, value) {
    const id = this.ids.get(meta.haEntityId);
    if (id === undefined) return;
    this.pool
      .query(
        `INSERT INTO measurement (entity_id, ts, value)
         VALUES ($1, $2, $3)
         ON CONFLICT DO NOTHING`,
        [id, iso(tsMs), value]
      )
      .catch((e) => console.error("measurement write error:", e.message));
  }

  writeEnergyDelta(meta, tsMs, deltaKwh) {
    const id = this.ids.get(meta.haEntityId);
    if (id === undefined) return;
    this.pool
      .query(
        `INSERT INTO energy_delta (entity_id, ts, delta_kwh, source)
         VALUES ($1, $2, $3, 'ingest')
         ON CONFLICT DO NOTHING`,
        [id, iso(tsMs), deltaKwh]
      )
      .catch((e) => console.error("energy_delta write error:", e.message));
  }

  writePrice(meta, tsMs, priceEur) {
    this.pool
      .query(
        `INSERT INTO price (ts, tariff, price_eur, source)
         VALUES ($1, 'normaal', $2, $3)
         ON CONFLICT (ts, tariff) DO UPDATE
           SET price_eur = EXCLUDED.price_eur`,
        [iso(tsMs), priceEur, meta.source ?? "zonneplan"]
      )
      .catch((e) => console.error("price write error:", e.message));
  }

  async close() {
    await this.pool.end();
  }
}

function iso(tsMs) {
  return new Date(tsMs).toISOString();
}
