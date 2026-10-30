#!/usr/bin/env node
// CLI for the backfill mechanism (see docs/backfill-design.md, section 10).
//
//   node src/backfill/cli.mjs detect [--kind K] [--entity ID] [--since ISO]
//   node src/backfill/cli.mjs repair [--kind K] [--entity ID] [--since ISO] [--dry-run] [--auto]
//   node src/backfill/cli.mjs report [--limit N]
//
// repair is the default command when none is given. --auto is for the hourly
// timer (deploy/ems-backfill.timer): it records run_kind 'auto' and prints a
// journal-friendly one-line summary per entity, nothing else. Exit code 0
// covers every data outcome (including no-gap and source-down); non-zero is
// for program errors (missing config.json or HA_TOKEN, unreachable
// database, bad flags).
//
// Configuration: entities from ingest/config.json (untracked deployment
// configuration, loaded exactly like src/index.mjs loads it), secrets and
// tuning from the environment (the same ingest/.env as the worker):
// HA_WS_URL, HA_TOKEN, PG*, EMS_TIMEZONE, EMS_BACKFILL_LOOKBACK_DAYS,
// EMS_BACKFILL_MAX_ROWS_PER_RUN, EMS_BACKFILL_MAX_GAP_BUCKETS,
// EMS_BACKFILL_RECORDER_RETENTION_DAYS. No new env files.

import { readFileSync } from "node:fs";
import pg from "pg";
import { DbWriter } from "../db.mjs";
import { DAY_MS, dayOf } from "../buckets.mjs";
import { detectBucketGaps, detectPriceGaps, detectGasPriceGaps } from "./detect.mjs";
import { fetchEntityHistory, haHttpBaseFromWsUrl } from "./sources/ha-history.mjs";
import { fetchPriceForecast, TARIFF_FORECAST_ENTITY_ID } from "./sources/price-forecast.mjs";
import { fetchTariffHistory } from "./sources/price-history.mjs";
import { fetchGasPriceHistory } from "./sources/gas-price-history.mjs";
import {
  runBackfill,
  createRowLoader,
  createPriceHoursLoader,
  createGasDaysLoader,
} from "./repair.mjs";
import { createBackfillLog } from "./log.mjs";

const USAGE = `Usage: node src/backfill/cli.mjs <detect|repair|report> [flags]
  detect   report gaps only, no writes
  repair   full repair run (the default command)
  report   show the last N backfill_log rows
Flags:
  --kind K        only entities of this kind (sample | counter | price | gas_price)
  --entity ID     only this ha_entity_id
  --since ISO     window start (ISO 8601), instead of the lookback days
  --dry-run       repair: detect + fetch, no writes at all
  --auto          repair: run from the hourly timer (run_kind auto, one-line summary)
  --limit N       report: how many log rows to show (default 20)`;

function parseArgs(argv) {
  const args = { command: "repair", kind: null, entity: null, since: null, dryRun: false, auto: false, limit: 20 };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dry-run") args.dryRun = true;
    else if (a === "--auto") args.auto = true;
    else if (a === "--kind" || a === "--entity" || a === "--since" || a === "--limit") {
      const v = argv[++i];
      if (v === undefined) fail(`missing value for ${a}\n${USAGE}`);
      if (a === "--kind") args.kind = v;
      else if (a === "--entity") args.entity = v;
      else if (a === "--since") args.since = v;
      else args.limit = Number(v);
    } else if (!a.startsWith("--") && args.command === "repair" && (a === "detect" || a === "repair" || a === "report")) {
      args.command = a;
    } else {
      fail(`unknown argument: ${a}\n${USAGE}`);
    }
  }
  return args;
}

function fail(message, code = 2) {
  console.error(message);
  process.exit(code);
}

function loadConfig() {
  // Loaded exactly like src/index.mjs: readFileSync ../config.json relative to
  // the module (here: src/backfill/ -> ../../config.json = ingest/config.json).
  try {
    return JSON.parse(readFileSync(new URL("../../config.json", import.meta.url), "utf8"));
  } catch {
    fail(
      "ingest/config.json not found or unreadable. It is untracked deployment " +
        "configuration: copy config.example.json to config.json and adjust it.",
      1
    );
  }
}

function windowFrom(args) {
  const lookbackDays = Number(process.env.EMS_BACKFILL_LOOKBACK_DAYS ?? 7) || 7;
  const toMs = Date.now();
  let sinceMs = null;
  if (args.since !== null) {
    sinceMs = Date.parse(args.since);
    if (!Number.isFinite(sinceMs)) fail(`--since is not a valid ISO 8601 timestamp: ${args.since}`);
    if (sinceMs > toMs) fail("--since lies in the future");
  }
  const fromMs = sinceMs ?? toMs - Math.max(1, lookbackDays) * DAY_MS;
  return { fromMs, toMs };
}

const iso = (ms) => new Date(ms).toISOString();

function printBucketGaps(entityId, kind, det) {
  console.log(`  ${entityId} [${kind}]: median cadence ${det.medianIntervalBuckets} bucket(s), ` +
    `${det.repairable.length} repairable, ${det.reportable.length} reportable`);
  for (const g of det.repairable) {
    console.log(`    gap      ${iso(g.startMs)} .. ${iso(g.endMs)} (${g.missingBuckets} missing buckets)`);
  }
  for (const g of det.reportable) {
    console.log(`    ${g.reason.padEnd(8)} ${iso(g.startMs)} .. ${iso(g.endMs)} (${g.missingBuckets} missing buckets)`);
  }
}

async function commandDetect(entities, args) {
  const { fromMs, toMs } = windowFrom(args);
  const timeZone = process.env.EMS_TIMEZONE ?? "Europe/Amsterdam";
  const dbWriter = new DbWriter(entities);
  try {
    await dbWriter.init();
    const pool = dbWriter.pool;
    const loadRows = createRowLoader(pool, dbWriter.ids);

    for (const e of entities) {
      if (e.kind === "sample" || e.kind === "counter") {
        const rows = await loadRows(e, fromMs, toMs);
        printBucketGaps(e.haEntityId, e.kind, detectBucketGaps(rows, fromMs, toMs, {
          maxGapBuckets: Number(process.env.EMS_BACKFILL_MAX_GAP_BUCKETS ?? 36) || 36,
        }));
      } else if (e.kind === "price") {
        const res = await pool.query(
          `SELECT ts FROM price WHERE ts >= $1 AND ts < $2 ORDER BY ts`,
          [iso(fromMs), iso(toMs)]
        );
        const hours = res.rows.map((r) => new Date(r.ts).getTime());
        const runs = detectPriceGaps(hours, fromMs, toMs);
        console.log(`  ${e.haEntityId} [price]: ${runs.length} missing hour run(s) in [${iso(fromMs)}, ${iso(toMs)})`);
        for (const r of runs) console.log(`    gap ${iso(r.startMs)} .. ${iso(r.endMs)}`);
      } else if (e.kind === "gas_price") {
        const fromDay = dayOf(fromMs, timeZone);
        const toDay = dayOf(toMs, timeZone);
        const res = await pool.query(
          `SELECT day FROM gas_price WHERE day >= $1 AND day <= $2 ORDER BY day`,
          [fromDay, toDay]
        );
        const missing = detectGasPriceGaps(res.rows.map((r) => String(r.day)), fromDay, toDay);
        console.log(`  ${e.haEntityId} [gas_price]: ${missing.length} missing day(s) in [${fromDay}, ${toDay}]`);
        for (const d of missing) console.log(`    missing ${d}`);
      }
    }
  } finally {
    await dbWriter.close().catch(() => {});
  }
}

async function commandRepair(entities, args) {
  const HA_WS_URL = process.env.HA_WS_URL ?? "ws://localhost:8123/api/websocket";
  const HA_TOKEN = process.env.HA_TOKEN;
  if (!HA_TOKEN) {
    fail("Set HA_TOKEN in the environment (long-lived access token from Home Assistant).", 1);
  }
  const httpBase = haHttpBaseFromWsUrl(HA_WS_URL);
  const sinceMs = args.since !== null ? Date.parse(args.since) : null;

  const dbWriter = new DbWriter(entities);
  try {
    await dbWriter.init();
    const pool = dbWriter.pool;
    const log = createBackfillLog(pool);
    const fetchHistory = (entityId, startMs, endMs) =>
      fetchEntityHistory({ httpBase, token: HA_TOKEN, entityId, startMs, endMs });

    const { fromMs, toMs, results } = await runBackfill({
      entities,
      lookbackDays: Number(process.env.EMS_BACKFILL_LOOKBACK_DAYS ?? 7) || 7,
      maxRowsPerRun: Number(process.env.EMS_BACKFILL_MAX_ROWS_PER_RUN ?? 5000) || 5000,
      maxGapBuckets: Number(process.env.EMS_BACKFILL_MAX_GAP_BUCKETS ?? 36) || 36,
      recorderRetentionDays: Number(process.env.EMS_BACKFILL_RECORDER_RETENTION_DAYS ?? 40) || 40,
      timeZone: process.env.EMS_TIMEZONE ?? "Europe/Amsterdam",
      source: "backfill-ha-history", // row label in energy_delta
      logSource: "ha-history", // plugin recorded in backfill_log.source
      runKind: args.auto ? "auto" : "cli",
      sinceMs,
      loadRows: createRowLoader(pool, dbWriter.ids),
      fetchHistory,
      dbWriter,
      log,
      loadPriceHours: createPriceHoursLoader(pool),
      fetchPriceForecast: () => fetchPriceForecast({ httpBase, token: HA_TOKEN }),
      fetchPriceHistory: (startMs, endMs) =>
        fetchTariffHistory({ httpBase, token: HA_TOKEN, entityId: TARIFF_FORECAST_ENTITY_ID, startMs, endMs }),
      loadGasDays: createGasDaysLoader(pool),
      fetchGasHistory: (cfg, startMs, endMs) =>
        fetchGasPriceHistory({
          httpBase,
          token: HA_TOKEN,
          entityId: cfg.haEntityId, // from config.json, kind gas_price
          startMs,
          endMs,
          timeZone: process.env.EMS_TIMEZONE ?? "Europe/Amsterdam",
        }),
      dryRun: args.dryRun,
    });

    if (args.auto) {
      // Journal-friendly: exactly one line per entity, nothing else.
      for (const r of results) {
        console.log(
          `backfill ${r.outcome} ${r.kind} ${r.haEntityId}: ` +
            `${r.rowsWritten} row(s) written, ${r.rowsMissing} missing; ${r.detail?.summary ?? ""}`
        );
      }
      return;
    }

    console.log(`Backfill ${args.dryRun ? "dry-run" : "repair"} window [${iso(fromMs)}, ${iso(toMs)}), source backfill-ha-history`);
    if (results.length === 0) {
      console.log("  no configured entities of a repairable kind; nothing to repair");
    }
    for (const r of results) {
      console.log(
        `  ${r.haEntityId} [${r.kind}]: ${r.outcome}, ${r.rowsWritten} row(s) written, ` +
          `${r.rowsMissing} missing bucket(s) not written` +
          (args.dryRun ? `, ${r.statesReplayed} state(s) replayable` : "") +
          `; ${r.detail?.summary ?? ""}`
      );
    }
  } finally {
    await dbWriter.close().catch(() => {});
  }
}

async function commandReport(args) {
  const pool = new pg.Pool();
  try {
    const log = createBackfillLog(pool);
    const rows = await log.recent(args.limit);
    if (rows.length === 0) {
      console.log("backfill_log is empty (has the repair run been executed yet?)");
      return;
    }
    for (const r of rows) {
      const range = `${new Date(r.range_start).toISOString()} .. ${new Date(r.range_end).toISOString()}`;
      const detail = r.detail && typeof r.detail === "object" ? (r.detail.summary ?? JSON.stringify(r.detail)) : r.detail;
      console.log(
        `${r.id}  ${r.created_at.toISOString()}  ${r.ha_entity_id} [${r.kind}] ${r.run_kind}/${r.source} ` +
          `${r.outcome} ${r.rows_written} row(s) written, ${r.rows_missing} missing  ${range}` +
          (detail ? `  (${detail})` : "")
      );
    }
  } finally {
    await pool.end().catch(() => {});
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  let entities = [];
  if (args.command === "detect" || args.command === "repair") {
    const config = loadConfig();
    entities = config.entities ?? [];
    if (args.kind !== null) entities = entities.filter((e) => e.kind === args.kind);
    if (args.entity !== null) entities = entities.filter((e) => e.haEntityId === args.entity);
    if (entities.length === 0) {
      fail("No configured entities match the given --kind/--entity filters.", 1);
    }
  }

  try {
    if (args.command === "detect") await commandDetect(entities, args);
    else if (args.command === "repair") await commandRepair(entities, args);
    else await commandReport(args);
  } catch (e) {
    // pg connection errors can carry an empty message; the code says enough.
    const reason = e?.message || e?.code || String(e);
    console.error(`backfill ${args.command} failed:`, reason);
    process.exitCode = 1;
    return;
  }
  // Every data outcome (repaired, partial, source-down, permanent, no-gap) is
  // a success of the program: exit code 0.
}

main();
