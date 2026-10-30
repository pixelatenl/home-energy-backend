// BackfillWriter: adapter between the IngestEngine and the DbWriter for
// backfill replays (see docs/backfill-design.md, section 2, rule 2).
//
// The engine calls the writer with the live-ingest signature
// (meta, tsMs, value/delta); it has no notion of a source label. This adapter
// injects the backfill source label so that repaired rows in energy_delta
// (and later price/gas_price replays) are labelled 'backfill-ha-history'
// while the live worker keeps writing 'ingest'. It also tracks the pending
// inserts so the orchestrator can flush and count the rows a repair really
// wrote (the DbWriter write methods resolve to the inserted row count).

export class BackfillWriter {
  /**
   * @param {object} dbWriter - DbWriter (or a fake with the same contract)
   * @param {string} [source] - source label for the written rows
   */
  constructor(dbWriter, source = "backfill-ha-history") {
    this.db = dbWriter;
    this.source = source;
    /** @type {Array<Promise<void>>} */
    this.pending = [];
    this.written = 0;
  }

  writeMeasurement(meta, tsMs, value) {
    this.track(this.db.writeMeasurement(meta, tsMs, value));
  }

  writeEnergyDelta(meta, tsMs, deltaKwh) {
    this.track(this.db.writeEnergyDelta(meta, tsMs, deltaKwh, this.source));
  }

  writePrice(meta, tsMs, priceEur) {
    this.track(this.db.writePrice({ ...meta, source: this.source }, tsMs, priceEur));
  }

  writeGasPrice(meta, day, priceEur) {
    this.track(this.db.writeGasPrice({ ...meta, source: this.source }, day, priceEur));
  }

  /** Count one pending insert; write errors are logged by the DbWriter. */
  track(insertPromise) {
    this.pending.push(
      Promise.resolve(insertPromise)
        .then((n) => {
          if (Number.isFinite(n)) this.written += n;
        })
        .catch(() => {
          /* the DbWriter contract resolves to 0 on error; nothing to do */
        })
    );
  }

  /** Wait for all pending inserts and return the number of rows written. */
  async flush() {
    await Promise.all(this.pending);
    this.pending = [];
    return this.written;
  }
}
