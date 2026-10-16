/**
 * Pure view helpers for the fleet dashboard (no React, no I/O) so they can be
 * unit-tested. Nothing here fabricates telemetry: every value is derived from a
 * DashboardSnapshot that came from Snowflake.
 */

import {
  ACTIVE_WINDOW_SECONDS,
  HISTORY_SECONDS,
  STALE_AFTER_MS,
  type ClientSummary,
  type DashboardSnapshot,
  type LatestRows,
  type RouterPoint,
} from "@/lib/demo-types"

export type Metric = "temperature_c" | "cpu_pct" | "traffic_mbps"

export interface MetricSpec {
  key: Metric
  label: string
  unit: string
  /** Fixed domain where the metric has natural bounds; traffic scales to data. */
  domain: [number, number] | null
  digits: number
}

export const METRICS: Record<Metric, MetricSpec> = {
  temperature_c: { key: "temperature_c", label: "Temperature", unit: "°C", domain: [30, 90], digits: 1 },
  cpu_pct: { key: "cpu_pct", label: "CPU", unit: "%", domain: [0, 100], digits: 0 },
  traffic_mbps: { key: "traffic_mbps", label: "Traffic", unit: "Mbps", domain: null, digits: 0 },
}

/** Per-client value shown on overview tiles for a metric (latest row per device). */
export function clientValue(c: ClientSummary, metric: Metric): number {
  if (metric === "temperature_c") return c.avg_temperature_c
  if (metric === "cpu_pct") return c.avg_cpu_pct
  return c.total_traffic_mbps
}

export type ViewStatus = "loading" | "error" | "stale" | "awaiting" | "live" | "idle"

export interface StatusView {
  status: ViewStatus
  label: string
  detail: string
}

/**
 * Display state derived from the snapshot only. Producer states (starting,
 * draining, stopped) are not observable here; they live in the producer terminal.
 */
export function deriveStatus(
  snap: DashboardSnapshot | undefined,
  lastUpdatedMs: number,
  nowMs: number,
  fetchError: string | null,
): StatusView {
  if (!snap) {
    if (fetchError) return { status: "error", label: "Error", detail: fetchError }
    return { status: "loading", label: "Connecting", detail: "Running first Snowflake snapshot…" }
  }
  const hasData = snap.data.physical_rows > 0
  const clientStale = lastUpdatedMs > 0 && nowMs - lastUpdatedMs > STALE_AFTER_MS
  if (snap.query_error && !snap.last_success_at) {
    return { status: "error", label: "Error", detail: snap.query_error }
  }
  if (snap.stale || clientStale) {
    return {
      status: "stale",
      label: "Stale",
      detail: snap.query_error ?? fetchError ?? "Snapshot is older than 6 s; showing last good data",
    }
  }
  if (!snap.data.run_id || !hasData) {
    return {
      status: "awaiting",
      label: "Awaiting first data",
      detail: snap.data.run_id ? `Run ${shortRun(snap.data.run_id)} has no queryable rows yet` : "No run with events in the last 15 minutes",
    }
  }
  if (snap.data.active_clients > 0) {
    return { status: "live", label: "Live", detail: `Queryable events within the last ${ACTIVE_WINDOW_SECONDS} s` }
  }
  return {
    status: "idle",
    label: "No recent events",
    detail: `No queryable events in the last ${ACTIVE_WINDOW_SECONDS} s (producer stopped or draining?)`,
  }
}

export function shortRun(runId: string | null): string {
  return runId ? runId.slice(0, 8) : "—"
}

/** A tile is reporting if its newest queried event is within the active window of the snapshot. */
export function isReporting(c: ClientSummary, fetchedAtMs: number): boolean {
  const t = Date.parse(c.latest_event_ts)
  return Number.isFinite(t) && fetchedAtMs - t <= ACTIVE_WINDOW_SECONDS * 1000
}

function clamp01(x: number): number {
  return Math.min(1, Math.max(0, x))
}

/** Cool→hot ramp (Snowflake blue → amber → red). Returns an rgb() string. */
export function heatColor(t: number): string {
  const stops: [number, [number, number, number]][] = [
    [0, [17, 86, 157]],
    [0.4, [41, 181, 232]],
    [0.7, [245, 176, 65]],
    [1, [220, 53, 69]],
  ]
  const x = clamp01(t)
  for (let i = 1; i < stops.length; i++) {
    const [p1, c1] = stops[i]
    const [p0, c0] = stops[i - 1]
    if (x <= p1) {
      const f = (x - p0) / (p1 - p0 || 1)
      const c = c0.map((v, k) => Math.round(v + (c1[k] - v) * f))
      return `rgb(${c[0]}, ${c[1]}, ${c[2]})`
    }
  }
  const last = stops[stops.length - 1][1]
  return `rgb(${last[0]}, ${last[1]}, ${last[2]})`
}

/** Domain for a metric: fixed where bounded, widened to include observed values. */
export function metricDomain(metric: Metric, values: number[]): [number, number] {
  const spec = METRICS[metric]
  const finite = values.filter(Number.isFinite)
  const dataMax = finite.length ? Math.max(...finite) : 0
  const dataMin = finite.length ? Math.min(...finite) : 0
  if (spec.domain) return [Math.min(spec.domain[0], dataMin), Math.max(spec.domain[1], dataMax)]
  return [0, dataMax > 0 ? dataMax * 1.1 : 1]
}

export function normalize(v: number, [lo, hi]: [number, number]): number {
  return hi > lo ? clamp01((v - lo) / (hi - lo)) : 0
}

/** Group series points by device (input is already ordered by device, time). */
export function groupSeries(series: RouterPoint[]): Map<string, RouterPoint[]> {
  const m = new Map<string, RouterPoint[]>()
  for (const p of series) {
    const arr = m.get(p.device_id)
    if (arr) arr.push(p)
    else m.set(p.device_id, [p])
  }
  return m
}

/** Shared x-axis end: newest bucket across all selected series. */
export function seriesEndMs(series: RouterPoint[]): number | null {
  let max = -Infinity
  for (const p of series) {
    const t = Date.parse(p.event_ts)
    if (t > max) max = t
  }
  return Number.isFinite(max) ? max : null
}

/**
 * SVG path for one router over the shared 120 s window. A gap of more than
 * 2.5 s starts a new segment so missing data is never drawn as a straight line.
 */
export function linePath(
  points: RouterPoint[],
  metric: Metric,
  domain: [number, number],
  endMs: number,
  width: number,
  height: number,
): string {
  const startMs = endMs - (HISTORY_SECONDS - 1) * 1000
  const span = endMs - startMs || 1
  let d = ""
  let prev = -Infinity
  for (const p of points) {
    const t = Date.parse(p.event_ts)
    if (!Number.isFinite(t) || t < startMs || t > endMs) continue
    const x = ((t - startMs) / span) * width
    const y = height - normalize(p[metric], domain) * height
    d += `${t - prev > 2500 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`
    prev = t
  }
  return d
}

export function formatNumber(n: number | null | undefined, digits = 0): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—"
  return n.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits })
}

export function formatFreshness(ms: number | null): string {
  if (ms === null) return "—"
  if (ms < 10_000) return `${(ms / 1000).toFixed(1)} s`
  if (ms < 120_000) return `${Math.round(ms / 1000)} s`
  return `${Math.round(ms / 60_000)} min`
}

/** Toggle a client in the selection, keeping at most `max` (drops the oldest). */
export function toggleSelection(selected: number[], clientId: number, max: number): number[] {
  if (selected.includes(clientId)) {
    return selected.length > 1 ? selected.filter((c) => c !== clientId) : selected
  }
  const next = [...selected, clientId]
  return next.length > max ? next.slice(next.length - max) : next
}

/**
 * EVENT_IDs in `rows` that were not in the previous poll's rows. `prev` is null on
 * the first poll of a run, so the initial rows are not all flashed as "new".
 */
export function newRowIds(prev: ReadonlySet<string> | null, rows: { event_id: string }[]): Set<string> {
  const out = new Set<string>()
  if (prev === null) return out
  for (const r of rows) if (!prev.has(r.event_id)) out.add(r.event_id)
  return out
}

/** Same stale rule as the snapshot: server-flagged, or no successful poll for 6 s. */
export function isLatestStale(latest: LatestRows | undefined, lastUpdatedMs: number, nowMs: number): boolean {
  if (!latest) return false
  return latest.stale || (lastUpdatedMs > 0 && nowMs - lastUpdatedMs > STALE_AFTER_MS)
}

/** `2026-09-30T20:00:10.123Z` → `20:00:10.123` (UTC time of day for the landed-rows table). */
export function formatEventTime(iso: string): string {
  const m = /T(\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?)/.exec(iso)
  return m ? m[1] : iso
}
