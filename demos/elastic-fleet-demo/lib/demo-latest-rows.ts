/**
 * Server-only service for GET /api/latest-rows (CONTRACT.md "Live landing table").
 *
 * Two fixed, bound-parameter statements per poll: COUNT(*) for the run and the
 * newest 12 rows. Their text is returned verbatim so the dashboard can show the
 * exact SQL that produced the numbers. No user-supplied SQL. Concurrent requests
 * for the same run share one in-flight query set; last-good results are kept per
 * run and marked stale after 6 s, like the snapshot.
 */

import { RESOLVE_RUN_SQL, TELEMETRY_TABLE, sanitizeError, type QueryFn } from "@/lib/demo-snapshot"
import {
  LATEST_ROWS_LIMIT,
  REGIONS,
  STALE_AFTER_MS,
  type LandedRow,
  type LatestRows,
  type Region,
  type Scenario,
} from "@/lib/demo-types"

/** ISO-8601 UTC. EVENT_TS is TIMESTAMP_NTZ holding UTC (see sql/setup.sql). */
const TS_FMT = `'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'`

export const LATEST_COUNT_SQL = `SELECT COUNT(*) AS TOTAL_ROWS
FROM ${TELEMETRY_TABLE}
WHERE RUN_ID = ?`

// Aliased EVENT_TS_UTC so ORDER BY EVENT_TS sorts on the timestamp column, not the string.
export const LATEST_ROWS_SQL = `SELECT EVENT_ID, CLIENT_ID, DEVICE_ID, REGION,
       TO_VARCHAR(EVENT_TS, ${TS_FMT}) AS EVENT_TS_UTC,
       TEMPERATURE_C, CPU_PCT, TRAFFIC_MBPS, SCENARIO
FROM ${TELEMETRY_TABLE}
WHERE RUN_ID = ?
ORDER BY EVENT_TS DESC, EVENT_ID DESC
LIMIT ${LATEST_ROWS_LIMIT}`

/** The two statements above, verbatim, as shown in the UI. */
export const LATEST_SQL_TEXT = `${LATEST_COUNT_SQL};\n\n${LATEST_ROWS_SQL};`

const SCENARIOS: readonly string[] = ["normal", "heat", "recover"]

function num(v: unknown): number {
  const n = typeof v === "number" ? v : Number(v)
  return Number.isFinite(n) ? n : 0
}

export function mapLandedRows(rows: Record<string, any>[]): LandedRow[] {
  return rows.slice(0, LATEST_ROWS_LIMIT).map((r) => ({
    event_id: String(r.EVENT_ID),
    client_id: num(r.CLIENT_ID),
    device_id: String(r.DEVICE_ID),
    region: ((REGIONS as readonly string[]).includes(String(r.REGION)) ? r.REGION : String(r.REGION)) as Region,
    event_ts: String(r.EVENT_TS_UTC),
    temperature_c: num(r.TEMPERATURE_C),
    cpu_pct: num(r.CPU_PCT),
    traffic_mbps: num(r.TRAFFIC_MBPS),
    scenario: (SCENARIOS.includes(String(r.SCENARIO)) ? r.SCENARIO : String(r.SCENARIO)) as Scenario,
  }))
}

interface LatestData {
  run_id: string | null
  total_rows: number
  rows: LandedRow[]
}

export interface LatestRowsServiceOptions {
  query: QueryFn
  now?: () => number
  warehouse?: string
  /** Respond with last-good data if the in-flight query set exceeds this. */
  responseTimeoutMs?: number
  maxCacheEntries?: number
}

type Outcome = { ok: true; data: LatestData; at: number } | { ok: false; error: string }

export function createLatestRowsService(opts: LatestRowsServiceOptions) {
  const now = opts.now ?? Date.now
  const responseTimeoutMs = opts.responseTimeoutMs ?? 10_000
  const maxEntries = opts.maxCacheEntries ?? 32
  const lastGood = new Map<string, { data: LatestData; successAt: number }>()
  const inflight = new Map<string, Promise<Outcome>>()
  const q = (sql: string, binds: (string | number)[]) => opts.query(sql, { binds, warehouse: opts.warehouse })

  async function runQuerySet(requestedRun: string | null): Promise<LatestData> {
    let runId = requestedRun
    if (!runId) {
      const rows = await q(RESOLVE_RUN_SQL, [])
      runId = rows[0]?.RUN_ID ? String(rows[0].RUN_ID) : null
    }
    if (!runId) return { run_id: null, total_rows: 0, rows: [] }
    const [count, rows] = await Promise.all([q(LATEST_COUNT_SQL, [runId]), q(LATEST_ROWS_SQL, [runId])])
    return { run_id: runId, total_rows: num(count[0]?.TOTAL_ROWS), rows: mapLandedRows(rows) }
  }

  function start(key: string, runId: string | null): Promise<Outcome> {
    const existing = inflight.get(key)
    if (existing) return existing
    const p: Promise<Outcome> = runQuerySet(runId)
      .then((data): Outcome => {
        const at = now()
        lastGood.delete(key)
        lastGood.set(key, { data, successAt: at })
        while (lastGood.size > maxEntries) lastGood.delete(lastGood.keys().next().value as string)
        return { ok: true, data, at }
      })
      .catch((e): Outcome => {
        console.error("[latest-rows] query set failed:", e instanceof Error ? e.message : e)
        return { ok: false, error: sanitizeError(e) }
      })
      .finally(() => inflight.delete(key))
    inflight.set(key, p)
    return p
  }

  function respond(data: LatestData, successAt: number | null, error: string | null): LatestRows {
    const t = now()
    return {
      contract_version: 2,
      fetched_at: new Date(t).toISOString(),
      last_success_at: successAt === null ? null : new Date(successAt).toISOString(),
      stale: error === null ? false : successAt === null || t - successAt > STALE_AFTER_MS,
      query_error: error,
      run_id: data.run_id,
      sql: LATEST_SQL_TEXT,
      total_rows: data.total_rows,
      rows: data.rows,
    }
  }

  function fallback(key: string, error: string, runId: string | null): LatestRows {
    const cached = lastGood.get(key)
    if (cached) return respond(cached.data, cached.successAt, error)
    return respond({ run_id: runId, total_rows: 0, rows: [] }, null, error)
  }

  async function getLatestRows(runId: string | null): Promise<LatestRows> {
    const key = runId ?? "latest"
    const pending = start(key, runId)
    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => resolve("timeout"), responseTimeoutMs)
    })
    const outcome = await Promise.race([pending, timeout])
    clearTimeout(timer)

    if (outcome === "timeout") {
      return fallback(key, `Latest-rows query still running after ${responseTimeoutMs / 1000} s`, runId)
    }
    if (!outcome.ok) return fallback(key, outcome.error, runId)
    return respond(outcome.data, outcome.at, null)
  }

  return { getLatestRows, _inflightCount: () => inflight.size }
}
