/**
 * Server-only snapshot service for GET /api/snapshot.
 *
 * One consolidated query set per (run, selection): resolve the run, then three
 * bound-parameter queries (totals, per-client latest rows, selected-router series).
 * Concurrent requests for the same key share one in-flight query set, so polling can
 * never stack queries. Last-good data is kept per key and marked stale after 6 s.
 *
 * Every number returned here comes from ROUTER_TELEMETRY. Producer-side counters
 * (append calls, acknowledgements, SDK request IDs) are not visible to the dashboard.
 */

import type { SnapshotParams } from "@/lib/demo-params"
import {
  ACTIVE_WINDOW_SECONDS,
  HISTORY_SECONDS,
  MAX_POINTS_PER_DEVICE,
  REGIONS,
  STALE_AFTER_MS,
  devicesForClients,
  emptySnapshotData,
  type ClientSummary,
  type DashboardSnapshot,
  type Region,
  type RouterPoint,
  type SnapshotData,
} from "@/lib/demo-types"

/** Commit-time window for the latency p50. */
export const LATENCY_WINDOW_SECONDS = 30

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_$]*$/

/**
 * Telemetry table from deployment config (never request input). Each part must be a plain
 * unquoted identifier, so the value is safe to interpolate into SQL text.
 */
export function telemetryTable(env: Record<string, string | undefined> = process.env): string {
  const parts = [
    env.ELASTIC_DEMO_DATABASE ?? "ELASTIC_DEMO_DB",
    env.ELASTIC_DEMO_SCHEMA ?? "FLEET",
    env.ELASTIC_DEMO_TABLE ?? "ROUTER_TELEMETRY",
  ]
  for (const p of parts) {
    if (!IDENT_RE.test(p)) throw new Error(`Invalid identifier in ELASTIC_DEMO_* settings: ${JSON.stringify(p)}`)
  }
  return parts.join(".")
}

export const TELEMETRY_TABLE = telemetryTable()

/** ISO-8601 UTC. EVENT_TS is TIMESTAMP_NTZ holding UTC (see sql/setup.sql). */
const TS_FMT = `'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'`

/** Latest run: newest EVENT_TS in the last 15 minutes. SYSDATE() is UTC NTZ. */
export const RESOLVE_RUN_SQL = `
SELECT RUN_ID
FROM ${TELEMETRY_TABLE}
WHERE EVENT_TS >= DATEADD(minute, -15, SYSDATE())
ORDER BY EVENT_TS DESC
LIMIT 1`

/**
 * Run totals. The rate window is the 10 s ending one second before the newest
 * queryable event (the newest second is usually only partly committed). Its lower
 * bound is exclusive and never earlier than the first event, so a run younger than
 * the window counts the same span it divides by (WINDOW_MS) instead of one extra tick.
 *
 * Latency p50 is commit time (METADATA$ROW_LAST_COMMIT_TIME, needs ROW_TIMESTAMP = TRUE)
 * minus EVENT_TS, over rows committed in the LATENCY_WINDOW_SECONDS before the run's
 * newest commit. Anchoring on the newest commit (not wall clock) keeps it from growing
 * between commits or after the run stops. The run-wide p50 uses every committed row.
 */
export const TOTALS_SQL = `
WITH r AS (
  SELECT EVENT_ID, CLIENT_ID, DEVICE_ID, EVENT_TS, METADATA$ROW_LAST_COMMIT_TIME AS COMMIT_TS
  FROM ${TELEMETRY_TABLE}
  WHERE RUN_ID = ?
), m AS (
  SELECT MIN(EVENT_TS) AS MIN_TS, MAX(EVENT_TS) AS MAX_TS, MAX(COMMIT_TS) AS MAX_COMMIT_TS FROM r
)
SELECT
  COUNT(r.EVENT_ID) AS PHYSICAL_ROWS,
  COUNT(DISTINCT r.EVENT_ID) AS DISTINCT_EVENTS,
  COUNT(DISTINCT r.DEVICE_ID) AS REPORTING_DEVICES,
  COUNT(DISTINCT IFF(r.EVENT_TS >= DATEADD(second, -${ACTIVE_WINDOW_SECONDS}, SYSDATE()), r.CLIENT_ID, NULL)) AS ACTIVE_CLIENTS,
  COUNT(DISTINCT IFF(r.EVENT_TS > GREATEST(DATEADD(second, -${ACTIVE_WINDOW_SECONDS + 1}, m.MAX_TS), m.MIN_TS)
                     AND r.EVENT_TS <= DATEADD(second, -1, m.MAX_TS), r.EVENT_ID, NULL)) AS WINDOW_EVENTS,
  LEAST(${ACTIVE_WINDOW_SECONDS * 1000},
        MAX(DATEDIFF('millisecond', m.MIN_TS, DATEADD(second, -1, m.MAX_TS)))) AS WINDOW_MS,
  MAX(DATEDIFF('millisecond', m.MAX_TS, SYSDATE())) AS SERVER_AGE_MS,
  MEDIAN(IFF(r.COMMIT_TS > DATEADD(second, -${LATENCY_WINDOW_SECONDS}, m.MAX_COMMIT_TS),
             GREATEST(0, DATEDIFF('millisecond', r.EVENT_TS, CONVERT_TIMEZONE('UTC', r.COMMIT_TS)::TIMESTAMP_NTZ)),
             NULL)) AS LATENCY_P50_MS,
  MEDIAN(GREATEST(0, DATEDIFF('millisecond', r.EVENT_TS, CONVERT_TIMEZONE('UTC', r.COMMIT_TS)::TIMESTAMP_NTZ))) AS LATENCY_P50_RUN_MS,
  TO_VARCHAR(MAX(m.MAX_TS), ${TS_FMT}) AS LATEST_EVENT_TS
FROM r CROSS JOIN m`

/** Overview: each device's latest row, rolled up per client (≤250 rows). */
export const CLIENTS_SQL = `
WITH latest AS (
  SELECT CLIENT_ID, REGION, DEVICE_ID, EVENT_TS, TEMPERATURE_C, CPU_PCT, TRAFFIC_MBPS
  FROM ${TELEMETRY_TABLE}
  WHERE RUN_ID = ?
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DEVICE_ID ORDER BY EVENT_TS DESC, SEQUENCE DESC) = 1
)
SELECT
  CLIENT_ID,
  ANY_VALUE(REGION) AS REGION,
  COUNT(*) AS REPORTING_DEVICES,
  TO_VARCHAR(MAX(EVENT_TS), ${TS_FMT}) AS LATEST_EVENT_TS,
  AVG(TEMPERATURE_C) AS AVG_TEMPERATURE_C,
  AVG(CPU_PCT) AS AVG_CPU_PCT,
  SUM(TRAFFIC_MBPS) AS TOTAL_TRAFFIC_MBPS
FROM latest
GROUP BY CLIENT_ID
ORDER BY CLIENT_ID`

/**
 * Selected routers: last 120 one-second buckets ending at the run's newest event,
 * EVENT_ID deduplicated before averaging. Placeholder count comes from the
 * validated device list, never from raw input.
 */
export function seriesSql(deviceCount: number): string {
  const placeholders = Array.from({ length: deviceCount }, () => "?").join(", ")
  return `
WITH m AS (
  SELECT MAX(EVENT_TS) AS MAX_TS FROM ${TELEMETRY_TABLE} WHERE RUN_ID = ?
), d AS (
  SELECT t.DEVICE_ID, t.CLIENT_ID, t.REGION, t.EVENT_TS, t.TEMPERATURE_C, t.CPU_PCT, t.TRAFFIC_MBPS
  FROM ${TELEMETRY_TABLE} t CROSS JOIN m
  WHERE t.RUN_ID = ?
    AND t.DEVICE_ID IN (${placeholders})
    AND t.EVENT_TS >= DATEADD(second, -${HISTORY_SECONDS - 1}, DATE_TRUNC('second', m.MAX_TS))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.EVENT_ID ORDER BY t.EVENT_TS) = 1
)
SELECT
  DEVICE_ID,
  ANY_VALUE(CLIENT_ID) AS CLIENT_ID,
  ANY_VALUE(REGION) AS REGION,
  TO_VARCHAR(DATE_TRUNC('second', EVENT_TS), ${TS_FMT}) AS BUCKET_TS,
  AVG(TEMPERATURE_C) AS TEMPERATURE_C,
  AVG(CPU_PCT) AS CPU_PCT,
  AVG(TRAFFIC_MBPS) AS TRAFFIC_MBPS
FROM d
GROUP BY DEVICE_ID, DATE_TRUNC('second', EVENT_TS)
ORDER BY DEVICE_ID, DATE_TRUNC('second', EVENT_TS)`
}

export type QueryFn = (sql: string, options: { binds?: (string | number)[]; warehouse?: string }) => Promise<Record<string, any>[]>

export interface SnapshotServiceOptions {
  query: QueryFn
  now?: () => number
  warehouse?: string
  /** Respond with last-good data if the in-flight query set exceeds this. */
  responseTimeoutMs?: number
  maxCacheEntries?: number
}

export function defaultWarehouse(): string | undefined {
  const wh = process.env.ELASTIC_DEMO_WAREHOUSE ?? "ELASTIC_DEMO_WH"
  return IDENT_RE.test(wh) ? wh : undefined
}

function num(v: unknown): number {
  const n = typeof v === "number" ? v : Number(v)
  return Number.isFinite(n) ? n : 0
}

function nullableNum(v: unknown): number | null {
  if (v === null || v === undefined) return null
  const n = typeof v === "number" ? v : Number(v)
  return Number.isFinite(n) ? n : null
}

function region(v: unknown): Region {
  return (REGIONS as readonly string[]).includes(String(v)) ? (v as Region) : (String(v) as Region)
}

export function sanitizeError(e: unknown): string {
  const raw = e instanceof Error ? e.message : String(e)
  const oneLine = raw.replace(/\s+/g, " ").trim()
  return `Snowflake query failed: ${oneLine.length > 200 ? `${oneLine.slice(0, 200)}…` : oneLine}`
}

export function mapClients(rows: Record<string, any>[]): ClientSummary[] {
  return rows.map((r) => ({
    client_id: num(r.CLIENT_ID),
    region: region(r.REGION),
    reporting_devices: num(r.REPORTING_DEVICES),
    latest_event_ts: String(r.LATEST_EVENT_TS),
    avg_temperature_c: num(r.AVG_TEMPERATURE_C),
    avg_cpu_pct: num(r.AVG_CPU_PCT),
    total_traffic_mbps: num(r.TOTAL_TRAFFIC_MBPS),
  }))
}

export function mapSeries(rows: Record<string, any>[]): RouterPoint[] {
  const perDevice = new Map<string, number>()
  const out: RouterPoint[] = []
  for (const r of rows) {
    const id = String(r.DEVICE_ID)
    const count = perDevice.get(id) ?? 0
    if (count >= MAX_POINTS_PER_DEVICE) continue
    perDevice.set(id, count + 1)
    out.push({
      device_id: id,
      client_id: num(r.CLIENT_ID),
      region: region(r.REGION),
      event_ts: String(r.BUCKET_TS),
      temperature_c: num(r.TEMPERATURE_C),
      cpu_pct: num(r.CPU_PCT),
      traffic_mbps: num(r.TRAFFIC_MBPS),
    })
  }
  return out
}

export function buildData(
  runId: string,
  totals: Record<string, any> | undefined,
  clients: ClientSummary[],
  series: RouterPoint[],
  nowMs: number,
): SnapshotData {
  const physical = num(totals?.PHYSICAL_ROWS)
  if (!totals || physical === 0) return emptySnapshotData(runId)

  const distinct = num(totals.DISTINCT_EVENTS)
  const latest = totals.LATEST_EVENT_TS ? String(totals.LATEST_EVENT_TS) : null
  const latestMs = latest ? Date.parse(latest) : NaN
  const freshness = Number.isFinite(latestMs) ? Math.max(0, nowMs - latestMs) : null

  // Rate needs at least 1 s of history and a recent newest event; otherwise it is
  // 0 once ingestion stops, or null (unavailable) right after the first row lands.
  const windowMs = nullableNum(totals.WINDOW_MS)
  const serverAge = nullableNum(totals.SERVER_AGE_MS)
  let rate: number | null = null
  if (serverAge !== null && serverAge > (ACTIVE_WINDOW_SECONDS + 2) * 1000) rate = 0
  else if (windowMs !== null && windowMs >= 1000) rate = num(totals.WINDOW_EVENTS) / (windowMs / 1000)

  return {
    run_id: runId,
    active_clients: num(totals.ACTIVE_CLIENTS),
    rows_per_second: rate,
    physical_rows: physical,
    distinct_events: distinct,
    duplicate_rows: Math.max(0, physical - distinct),
    reporting_devices: num(totals.REPORTING_DEVICES),
    latest_event_ts: latest,
    freshness_ms: freshness,
    latency_p50_ms: nullableNum(totals.LATENCY_P50_MS),
    latency_p50_run_ms: nullableNum(totals.LATENCY_P50_RUN_MS),
    clients,
    series,
  }
}

interface CacheEntry {
  data: SnapshotData
  successAt: number
}

type Outcome = { ok: true; data: SnapshotData; at: number } | { ok: false; error: string }

export function createSnapshotService(opts: SnapshotServiceOptions) {
  const now = opts.now ?? Date.now
  const responseTimeoutMs = opts.responseTimeoutMs ?? 10_000
  const maxEntries = opts.maxCacheEntries ?? 32
  const lastGood = new Map<string, CacheEntry>()
  const inflight = new Map<string, Promise<Outcome>>()
  const q = (sql: string, binds: (string | number)[]) => opts.query(sql, { binds, warehouse: opts.warehouse })

  async function runQuerySet(params: SnapshotParams): Promise<SnapshotData> {
    let runId = params.runId
    if (!runId) {
      const rows = await q(RESOLVE_RUN_SQL, [])
      runId = rows[0]?.RUN_ID ? String(rows[0].RUN_ID) : null
    }
    if (!runId) return emptySnapshotData(null)

    const devices = devicesForClients(params.clients)
    const [totals, clientRows, seriesRows] = await Promise.all([
      q(TOTALS_SQL, [runId]),
      q(CLIENTS_SQL, [runId]),
      devices.length > 0 ? q(seriesSql(devices.length), [runId, runId, ...devices]) : Promise.resolve([]),
    ])
    return buildData(runId, totals[0], mapClients(clientRows), mapSeries(seriesRows), now())
  }

  function remember(key: string, entry: CacheEntry) {
    lastGood.delete(key)
    lastGood.set(key, entry)
    while (lastGood.size > maxEntries) lastGood.delete(lastGood.keys().next().value as string)
  }

  function start(key: string, params: SnapshotParams): Promise<Outcome> {
    const existing = inflight.get(key)
    if (existing) return existing
    const p: Promise<Outcome> = runQuerySet(params)
      .then((data): Outcome => {
        const at = now()
        remember(key, { data, successAt: at })
        return { ok: true, data, at }
      })
      .catch((e): Outcome => {
        console.error("[snapshot] query set failed:", e instanceof Error ? e.message : e)
        return { ok: false, error: sanitizeError(e) }
      })
      .finally(() => inflight.delete(key))
    inflight.set(key, p)
    return p
  }

  function fallback(key: string, error: string, runId: string | null): DashboardSnapshot {
    const cached = lastGood.get(key)
    const t = now()
    return {
      contract_version: 1,
      fetched_at: new Date(t).toISOString(),
      last_success_at: cached ? new Date(cached.successAt).toISOString() : null,
      stale: cached ? t - cached.successAt > STALE_AFTER_MS : true,
      query_error: error,
      data: cached ? cached.data : emptySnapshotData(runId),
    }
  }

  async function getSnapshot(params: SnapshotParams): Promise<DashboardSnapshot> {
    const key = `${params.runId ?? "latest"}|${params.clients.join(",")}`
    const pending = start(key, params)
    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => resolve("timeout"), responseTimeoutMs)
    })
    const outcome = await Promise.race([pending, timeout])
    clearTimeout(timer)

    if (outcome === "timeout") {
      // The query set keeps running and stays single-flight; answer with last-good.
      return fallback(key, `Snapshot query still running after ${responseTimeoutMs / 1000} s`, params.runId)
    }
    if (!outcome.ok) return fallback(key, outcome.error, params.runId)
    return {
      contract_version: 1,
      fetched_at: new Date(now()).toISOString(),
      last_success_at: new Date(outcome.at).toISOString(),
      stale: false,
      query_error: null,
      data: outcome.data,
    }
  }

  return { getSnapshot, _inflightCount: () => inflight.size }
}
