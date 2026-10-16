/**
 * Dashboard response types. Mirrors CONTRACT.md "Consolidated dashboard response"
 * (contract_version 1). Everything here is read back from Snowflake; the browser
 * never generates telemetry and never sees producer-side counters.
 */

export const REGIONS = ["us-west", "us-east", "eu-central", "ap-south", "sa-east"] as const
export type Region = (typeof REGIONS)[number]

export const MAX_CLIENTS = 250
export const DEVICES_PER_CLIENT = 10
export const MAX_SELECTED_CLIENTS = 3
export const MAX_SELECTED_DEVICES = 24
export const HISTORY_SECONDS = 120
export const MAX_POINTS_PER_DEVICE = 120
export const ACTIVE_WINDOW_SECONDS = 10
export const STALE_AFTER_MS = 6_000
export const POLL_INTERVAL_MS = 2_000
export const DEFAULT_CLIENTS = [1, 2, 3]

/** Static layout mapping from the contract: a client's 10 devices share its region. */
export function regionForClient(clientId: number): Region {
  return REGIONS[(clientId - 1) % REGIONS.length]
}

export function deviceId(clientId: number, device: number): string {
  return `RTR-${String(clientId).padStart(3, "0")}-${String(device).padStart(2, "0")}`
}

/** Selected clients → at most 24 device IDs in ascending client/device order. */
export function devicesForClients(clients: number[]): string[] {
  const out: string[] = []
  for (const c of [...clients].sort((a, b) => a - b)) {
    for (let d = 1; d <= DEVICES_PER_CLIENT; d++) out.push(deviceId(c, d))
  }
  return out.slice(0, MAX_SELECTED_DEVICES)
}

export interface RouterPoint {
  device_id: string
  client_id: number
  region: Region
  event_ts: string
  temperature_c: number
  cpu_pct: number
  traffic_mbps: number
}

export interface ClientSummary {
  client_id: number
  region: Region
  reporting_devices: number
  latest_event_ts: string
  avg_temperature_c: number
  avg_cpu_pct: number
  total_traffic_mbps: number
}

export interface SnapshotData {
  run_id: string | null
  active_clients: number
  rows_per_second: number | null
  physical_rows: number
  distinct_events: number
  duplicate_rows: number
  reporting_devices: number
  latest_event_ts: string | null
  freshness_ms: number | null
  /** p50 of commit time − EVENT_TS over rows committed in the run's last 30 s. */
  latency_p50_ms: number | null
  /** Same measure over every committed row in the run. */
  latency_p50_run_ms: number | null
  clients: ClientSummary[]
  series: RouterPoint[]
}

export interface DashboardSnapshot {
  contract_version: 1
  fetched_at: string
  last_success_at: string | null
  stale: boolean
  query_error: string | null
  data: SnapshotData
}

/** CONTRACT.md "Live landing table": newest rows shown in the live query panel. */
export const LATEST_ROWS_LIMIT = 12

export type Scenario = "normal" | "heat" | "recover"

export interface LandedRow {
  event_id: string
  client_id: number
  device_id: string
  region: Region
  event_ts: string
  temperature_c: number
  cpu_pct: number
  traffic_mbps: number
  scenario: Scenario
}

export interface LatestRows {
  contract_version: 2
  fetched_at: string
  last_success_at: string | null
  stale: boolean
  query_error: string | null
  /** Run bound to `RUN_ID = ?` (resolved with the snapshot's newest-run rule when omitted). */
  run_id: string | null
  /** Exact SQL text executed, binds shown as `?`. */
  sql: string
  total_rows: number
  rows: LandedRow[]
}

export interface CodeSample {
  language: "python"
  path: string
  source: string
}

export function emptySnapshotData(runId: string | null = null): SnapshotData {
  return {
    run_id: runId,
    active_clients: 0,
    rows_per_second: null,
    physical_rows: 0,
    distinct_events: 0,
    duplicate_rows: 0,
    reporting_devices: 0,
    latest_event_ts: null,
    freshness_ms: null,
    latency_p50_ms: null,
    latency_p50_run_ms: null,
    clients: [],
    series: [],
  }
}
