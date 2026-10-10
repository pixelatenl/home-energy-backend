// IngestEngine: pure sampling and delta-derivation logic.
// Receives state changes (entityId, numeric value, timestamp) and decides
// what is written to the database. Contains no I/O of its own; the writer
// is injected, which makes the logic testable without Home Assistant or a
// database (see test/engine.test.mjs).
//
// Rules (from the design):
// - measurement samples (kind "sample"): the last value of a closed 5-minute
//   bucket is written as a measurement, with the bucket start as timestamp.
// - energy meters (kind "counter", cumulative kWh readings): per closed bucket,
//   delta = last reading in the bucket minus last reading in the previous
//   bucket, written to energy_delta. A meter reset (reading drops) yields
//   delta 0. The very first bucket after a restart is a baseline: no delta is
//   written for it because the previous reading is unknown.
// - prices (kind "price"): every change is written with the start of the hour
//   as timestamp; a newer price for the same hour wins (upsert).
// - Each bucket is finalized at most once (the "closed" flag); late
//   measurements for an already closed bucket are ignored.

import { bucketOf, hourOf } from "./buckets.mjs";

export class IngestEngine {
  /**
   * @param {object} writer - interface: writeMeasurement(meta, tsMs, value),
   *   writeEnergyDelta(meta, tsMs, deltaKwh), writePrice(meta, tsMs, priceEur)
   * @param {object} [options]
   * @param {() => number} [options.now] - injectable clock for testing
   */
  constructor(writer, options = {}) {
    this.writer = writer;
    this.now = options.now ?? (() => Date.now());
    /** @type {Map<string, {cfg: object, currentBucket: number|null, valueInBucket: number|null, prevEnd: number|null, closed: boolean}>} */
    this.track = new Map();
  }

  /** Register the entities from the configuration. */
  registerEntities(entities) {
    for (const e of entities) {
      if (!e.haEntityId || !e.kind) {
        throw new Error("configuration entity is missing haEntityId or kind: " + JSON.stringify(e));
      }
      this.track.set(e.haEntityId, {
        cfg: e,
        currentBucket: null,
        valueInBucket: null,
        prevEnd: null,
        closed: false,
      });
    }
  }

  /** Process a state change. tsMs is the time of the measurement, not "now". */
  onState(entityId, value, tsMs = this.now()) {
    const t = this.track.get(entityId);
    if (t === undefined) return; // entity is not tracked by the worker
    if (!Number.isFinite(value)) return; // unavailable, unknown, not a number

    if (t.cfg.kind === "price") {
      this.writer.writePrice(t.cfg, hourOf(tsMs), value);
      return;
    }

    const bucket = bucketOf(tsMs);

    if (t.currentBucket === null) {
      // First measurement after startup: baseline, write nothing yet.
      t.currentBucket = bucket;
      t.valueInBucket = value;
      t.closed = false;
      return;
    }

    if (bucket === t.currentBucket) {
      if (!t.closed) t.valueInBucket = value; // late value for a closed bucket: ignore
      return;
    }

    if (bucket > t.currentBucket) {
      if (!t.closed) this.finalizeBucket(t);
      t.currentBucket = bucket;
      t.valueInBucket = value;
      t.closed = false;
      return;
    }

    // bucket < t.currentBucket: a lagging timestamp (clock drift,
    // source restart). Ignored so written buckets are not disturbed.
  }

  /** Call periodically (for example every minute) to close silent buckets. */
  tick() {
    const nowBucket = bucketOf(this.now());
    for (const t of this.track.values()) {
      if (t.cfg.kind === "price") continue;
      if (!t.closed && t.valueInBucket !== null && nowBucket > t.currentBucket) {
        this.finalizeBucket(t);
      }
    }
  }

  /** Close the current bucket of an entity and write the result. */
  finalizeBucket(t) {
    const tsMs = t.currentBucket;
    if (t.cfg.kind === "counter") {
      if (t.prevEnd !== null) {
        let delta = t.valueInBucket - t.prevEnd;
        if (delta < 0) delta = 0; // meter reset: no negative delta
        this.writer.writeEnergyDelta(t.cfg, tsMs, delta);
      }
      t.prevEnd = t.valueInBucket; // this reading is the baseline for the next bucket
    } else if (t.cfg.kind === "sample") {
      this.writer.writeMeasurement(t.cfg, tsMs, t.valueInBucket);
    }
    t.closed = true;
  }
}
