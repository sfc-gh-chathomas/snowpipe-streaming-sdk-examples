/**
 * Request validation for the read-only dashboard API. Pure functions, no I/O.
 */

import { DEFAULT_CLIENTS, MAX_CLIENTS, MAX_SELECTED_CLIENTS } from "@/lib/demo-types"

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

export interface SnapshotParams {
  runId: string | null
  clients: number[]
}

export type ParseResult = { ok: true; params: SnapshotParams } | { ok: false; error: string }

export type RunParseResult = { ok: true; runId: string | null } | { ok: false; error: string }

/** Parse the optional `run_id` (UUID, lowercased). Absent or empty means "newest run". */
export function parseRunParam(search: URLSearchParams): RunParseResult {
  const rawRun = search.get("run_id")
  if (rawRun === null || rawRun === "") return { ok: true, runId: null }
  if (!UUID_RE.test(rawRun)) return { ok: false, error: "run_id must be a UUID" }
  return { ok: true, runId: rawRun.toLowerCase() }
}

/** Parse `run_id` (UUID, optional) and `clients` (1..250, deduped, ≤3, default 1,2,3). */
export function parseSnapshotParams(search: URLSearchParams): ParseResult {
  const run = parseRunParam(search)
  if (!run.ok) return run
  const runId = run.runId

  const rawClients = search.get("clients")
  let clients = DEFAULT_CLIENTS
  if (rawClients !== null && rawClients !== "") {
    const parts = rawClients.split(",")
    if (parts.length > MAX_SELECTED_CLIENTS * 4) return { ok: false, error: "too many client IDs" }
    const ids = new Set<number>()
    for (const p of parts) {
      const t = p.trim()
      if (!/^\d{1,3}$/.test(t)) return { ok: false, error: `invalid client ID: ${t.slice(0, 8)}` }
      const n = Number(t)
      if (n < 1 || n > MAX_CLIENTS) return { ok: false, error: `client ID out of range 1..${MAX_CLIENTS}` }
      ids.add(n)
    }
    if (ids.size > MAX_SELECTED_CLIENTS) {
      return { ok: false, error: `at most ${MAX_SELECTED_CLIENTS} clients may be selected` }
    }
    clients = [...ids].sort((a, b) => a - b)
  }
  return { ok: true, params: { runId, clients } }
}

/**
 * Reject browser requests initiated by another site. The API only reads, but each
 * request spends warehouse time, so cross-site pages must not be able to drive it.
 * Requests without Sec-Fetch-Site (curl, server-side tests) are allowed.
 */
export function isCrossSiteRequest(headers: Headers): boolean {
  return headers.get("sec-fetch-site") === "cross-site"
}
