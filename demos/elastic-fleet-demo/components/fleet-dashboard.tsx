"use client"

/**
 * Read-only Elastic Fleet dashboard. Polls GET /api/snapshot (~2 s, never
 * overlapping) and renders only what Snowflake returned. The producer runs in the
 * presenter's terminal; its counters (append calls, acks, SDK request IDs) are
 * intentionally not shown here.
 */

import { useEffect, useMemo, useState } from "react"
import { keepPreviousData, useQuery } from "@tanstack/react-query"
import { Code2, X } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { LiveQuery } from "@/components/live-query-panel"
import { TOKEN_CLASS, highlightPython } from "@/lib/python-highlight"
import { cn } from "@/lib/utils"
import {
  DEFAULT_CLIENTS,
  DEVICES_PER_CLIENT,
  HISTORY_SECONDS,
  MAX_CLIENTS,
  MAX_SELECTED_CLIENTS,
  POLL_INTERVAL_MS,
  REGIONS,
  devicesForClients,
  regionForClient,
  type ClientSummary,
  type CodeSample,
  type DashboardSnapshot,
  type Region,
} from "@/lib/demo-types"
import {
  METRICS,
  clientValue,
  deriveStatus,
  formatFreshness,
  formatNumber,
  groupSeries,
  heatColor,
  isReporting,
  linePath,
  metricDomain,
  normalize,
  seriesEndMs,
  shortRun,
  toggleSelection,
  type Metric,
  type ViewStatus,
} from "@/lib/demo-view"

async function fetchSnapshot(runId: string | null, clients: number[], signal: AbortSignal): Promise<DashboardSnapshot> {
  const qs = new URLSearchParams({ clients: clients.join(",") })
  if (runId) qs.set("run_id", runId)
  const res = await fetch(`/api/snapshot?${qs}`, { signal, cache: "no-store" })
  const body = await res.json().catch(() => null)
  if (!res.ok) throw new Error(body?.error ?? `Snapshot request failed (${res.status})`)
  return body as DashboardSnapshot
}

async function fetchCode(signal: AbortSignal): Promise<CodeSample> {
  const res = await fetch("/api/code", { signal, cache: "no-store" })
  const body = await res.json().catch(() => null)
  if (!res.ok) throw new Error(body?.error ?? `Code request failed (${res.status})`)
  return body as CodeSample
}

function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])
  return now
}

// Tiles are laid out by region so a regional heat wave shows up as one block.
const CLIENTS_BY_REGION: Record<Region, number[]> = Object.fromEntries(
  REGIONS.map((r) => [r, Array.from({ length: MAX_CLIENTS }, (_, i) => i + 1).filter((c) => regionForClient(c) === r)]),
) as Record<Region, number[]>

const METRIC_STROKE: Record<Metric, string> = {
  temperature_c: "#f5a623",
  cpu_pct: "#29b5e8",
  traffic_mbps: "#34c38f",
}

const STATUS_CLASS: Record<ViewStatus, string> = {
  loading: "bg-secondary text-secondary-foreground",
  awaiting: "bg-secondary text-secondary-foreground",
  live: "bg-emerald-600 text-white",
  idle: "bg-slate-500 text-white",
  stale: "bg-amber-500 text-black",
  error: "bg-destructive text-destructive-foreground",
}

export function FleetDashboard({ runId }: { runId: string | null }) {
  const [clients, setClients] = useState<number[]>(DEFAULT_CLIENTS)
  const [metric, setMetric] = useState<Metric>("temperature_c")
  const [codeOpen, setCodeOpen] = useState(false)
  const now = useNow()

  const snapshotQuery = useQuery({
    queryKey: ["snapshot", runId ?? "latest", clients.join(",")],
    queryFn: ({ signal }) => fetchSnapshot(runId, clients, signal),
    refetchInterval: POLL_INTERVAL_MS,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
    placeholderData: keepPreviousData,
  })

  const snap = snapshotQuery.data
  const fetchError = snapshotQuery.error ? snapshotQuery.error.message : null
  const status = deriveStatus(snap, snapshotQuery.dataUpdatedAt, now, fetchError)
  const data = snap?.data

  return (
    <main className="flex h-[calc(100vh-3.5rem)] min-h-[640px] flex-col gap-3 p-4">
      <StatusBar
        status={status}
        snap={snap}
        metric={metric}
        onMetric={setMetric}
        onCode={() => setCodeOpen(true)}
      />
      <KpiStrip snap={snap} />
      <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,5fr)_minmax(0,7fr)] gap-3">
        <FleetOverview
          clients={data?.clients ?? []}
          fetchedAtMs={snap ? Date.parse(snap.fetched_at) : now}
          metric={metric}
          selected={clients}
          onToggle={(c) => setClients((s) => toggleSelection(s, c, MAX_SELECTED_CLIENTS))}
        />
        <RouterCharts snap={snap} selected={clients} metric={metric} />
      </div>
      {/* Same run as the snapshot once it is resolved, so the rows never mix runs with the charts. */}
      <LiveQuery runId={runId ?? data?.run_id ?? null} now={now} className="h-[clamp(180px,26vh,290px)] shrink-0" />
      <p className="text-[11px] text-muted-foreground">
        Every number on this page is queried from <span className="font-mono">ROUTER_TELEMETRY</span> for the
        current run. Producer-side metrics (clients, append calls, acknowledgements, SDK request IDs) are printed in
        the producer terminal and are not shown here.
      </p>
      {codeOpen && <CodeDrawer onClose={() => setCodeOpen(false)} />}
    </main>
  )
}

function StatusBar({
  status,
  snap,
  metric,
  onMetric,
  onCode,
}: {
  status: ReturnType<typeof deriveStatus>
  snap: DashboardSnapshot | undefined
  metric: Metric
  onMetric: (m: Metric) => void
  onCode: () => void
}) {
  return (
    <div className="flex items-center gap-3">
      <Badge className={cn("px-3 py-1 text-sm", STATUS_CLASS[status.status])} data-testid="status-badge">
        {status.label}
      </Badge>
      <span className="truncate text-sm text-muted-foreground" title={status.detail}>
        {status.detail}
      </span>
      <div className="ml-auto flex items-center gap-4 text-sm">
        <span className="text-muted-foreground">
          Run <span className="font-mono text-foreground" title={snap?.data.run_id ?? ""}>{shortRun(snap?.data.run_id ?? null)}</span>
        </span>
        <span className="text-muted-foreground">
          Last snapshot{" "}
          <span className="font-mono text-foreground">
            {snap?.last_success_at ? new Date(snap.last_success_at).toLocaleTimeString() : "—"}
          </span>
        </span>
        <div className="flex rounded-md border border-border p-0.5" role="group" aria-label="Metric">
          {(Object.keys(METRICS) as Metric[]).map((m) => (
            <button
              key={m}
              type="button"
              onClick={() => onMetric(m)}
              className={cn(
                "rounded px-2.5 py-1 text-xs font-medium",
                metric === m ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground",
              )}
            >
              {METRICS[m].label}
            </button>
          ))}
        </div>
        <Button variant="outline" size="sm" onClick={onCode}>
          <Code2 className="h-4 w-4" /> Code
        </Button>
      </div>
    </div>
  )
}

function Kpi({ label, value, sub, warn }: { label: string; value: string; sub?: string; warn?: boolean }) {
  return (
    <div className="rounded-lg border border-border bg-card px-3 py-2">
      <div className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className={cn("font-mono text-2xl font-semibold tabular-nums", warn && "text-amber-500")}>{value}</div>
      {sub && <div className="text-[11px] text-muted-foreground">{sub}</div>}
    </div>
  )
}

/** Latency p50 tile: last 30 s and whole run side by side, same type size. */
function LatencyKpi({ last30Ms, runMs }: { last30Ms: number | null; runMs: number | null }) {
  return (
    <div className="rounded-lg border border-border bg-card px-3 py-2">
      <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Latency p50</div>
      <div className="grid grid-cols-2 divide-x divide-border">
        {[
          { label: "last 30 s", ms: last30Ms },
          { label: "run", ms: runMs },
        ].map(({ label, ms }) => (
          <div key={label} className="min-w-0 px-2 first:pl-0 last:pr-0">
            <div className="whitespace-nowrap font-mono text-2xl font-semibold tabular-nums">{formatFreshness(ms)}</div>
            <div className="text-[11px] text-muted-foreground">{label}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

function KpiStrip({ snap }: { snap: DashboardSnapshot | undefined }) {
  const d = snap?.data
  return (
    <section aria-label="Queried from Snowflake">
      <div className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-primary">Queried from Snowflake</div>
      <div className="grid grid-cols-7 gap-3">
        <Kpi label="Active clients" value={formatNumber(d?.active_clients)} sub={`of ${MAX_CLIENTS} · event in last 10 s`} />
        <Kpi label="Queryable rows / s" value={formatNumber(d?.rows_per_second, 0)} sub="distinct events by EVENT_TS" />
        <Kpi label="Queryable rows" value={formatNumber(d?.physical_rows)} sub="physical rows this run" />
        <Kpi label="Distinct events" value={formatNumber(d?.distinct_events)} sub="unique EVENT_ID" />
        <Kpi
          label="Duplicate rows"
          value={formatNumber(d?.duplicate_rows)}
          sub="physical − distinct"
          warn={(d?.duplicate_rows ?? 0) > 0}
        />
        <Kpi label="Reporting devices" value={formatNumber(d?.reporting_devices)} sub={`of ${MAX_CLIENTS * DEVICES_PER_CLIENT}`} />
        <LatencyKpi last30Ms={d?.latency_p50_ms ?? null} runMs={d?.latency_p50_run_ms ?? null} />
      </div>
    </section>
  )
}

function FleetOverview({
  clients,
  fetchedAtMs,
  metric,
  selected,
  onToggle,
}: {
  clients: ClientSummary[]
  fetchedAtMs: number
  metric: Metric
  selected: number[]
  onToggle: (clientId: number) => void
}) {
  const byId = useMemo(() => new Map(clients.map((c) => [c.client_id, c])), [clients])
  const domain = useMemo(() => metricDomain(metric, clients.map((c) => clientValue(c, metric))), [clients, metric])
  const spec = METRICS[metric]

  return (
    <section className="flex min-h-0 flex-col rounded-lg border border-border bg-card p-3" aria-label="Fleet overview">
      <div className="mb-2 flex items-baseline justify-between">
        <h2 className="text-sm font-semibold">Fleet · 250 SDK clients × 10 routers</h2>
        <span className="text-[11px] text-muted-foreground">
          Tile = one client, latest row per router · click to chart (max {MAX_SELECTED_CLIENTS})
        </span>
      </div>
      <div className="flex min-h-0 flex-1 flex-col justify-between gap-2">
        {REGIONS.map((region) => (
          <div key={region}>
            <div className="mb-0.5 font-mono text-[11px] text-muted-foreground">{region}</div>
            <div className="grid grid-cols-[repeat(25,minmax(0,1fr))] gap-[3px]">
              {CLIENTS_BY_REGION[region].map((id) => {
                const c = byId.get(id)
                const reporting = c ? isReporting(c, fetchedAtMs) : false
                const value = c ? clientValue(c, metric) : null
                const isSelected = selected.includes(id)
                return (
                  <button
                    key={id}
                    type="button"
                    onClick={() => onToggle(id)}
                    aria-pressed={isSelected}
                    aria-label={`Client ${id}`}
                    title={
                      c
                        ? `Client ${id} · ${region}\n${c.reporting_devices}/10 routers · ${reporting ? "reporting" : "no event in 10 s"}\n` +
                          `${formatNumber(c.avg_temperature_c, 1)} °C · ${formatNumber(c.avg_cpu_pct)} % CPU · ${formatNumber(c.total_traffic_mbps)} Mbps`
                        : `Client ${id} · ${region}\nNo queryable rows this run`
                    }
                    className={cn(
                      "aspect-square rounded-[3px] transition-[opacity,background-color] duration-500",
                      !c && "bg-muted",
                      c && !reporting && "opacity-35 saturate-50",
                      isSelected && "ring-2 ring-foreground ring-offset-1 ring-offset-card",
                    )}
                    style={value !== null ? { backgroundColor: heatColor(normalize(value, domain)) } : undefined}
                  />
                )
              })}
            </div>
          </div>
        ))}
      </div>
      <div className="mt-2 flex items-center gap-2 text-[11px] text-muted-foreground">
        <span>{formatNumber(domain[0], spec.digits)} {spec.unit}</span>
        <div
          className="h-2 flex-1 rounded"
          style={{ background: `linear-gradient(90deg, ${[0, 0.4, 0.7, 1].map((t) => heatColor(t)).join(", ")})` }}
        />
        <span>{formatNumber(domain[1], spec.digits)} {spec.unit}</span>
        <span className="ml-3 inline-block h-2 w-2 rounded-sm bg-muted" /> no data
        <span className="ml-2 inline-block h-2 w-2 rounded-sm bg-sky-500 opacity-35" /> no event in 10 s
      </div>
    </section>
  )
}

function RouterCharts({ snap, selected, metric }: { snap: DashboardSnapshot | undefined; selected: number[]; metric: Metric }) {
  const series = snap?.data.series ?? []
  const devices = devicesForClients(selected)
  const grouped = useMemo(() => groupSeries(series), [series])
  const endMs = useMemo(() => seriesEndMs(series), [series])
  const domain = useMemo(() => metricDomain(metric, series.map((p) => p[metric])), [series, metric])
  const spec = METRICS[metric]

  return (
    <section className="flex min-h-0 flex-col rounded-lg border border-border bg-card p-3" aria-label="Selected routers">
      <div className="mb-2 flex items-baseline justify-between">
        <h2 className="text-sm font-semibold">
          {spec.label} ({spec.unit}) · clients {selected.join(", ")} · {devices.length} routers
        </h2>
        <span className="text-[11px] text-muted-foreground">
          Last {HISTORY_SECONDS} s · 1 s means · deduplicated by EVENT_ID · shared scale {formatNumber(domain[0], spec.digits)}–
          {formatNumber(domain[1], spec.digits)}
        </span>
      </div>
      <div className="grid min-h-0 flex-1 grid-cols-6 grid-rows-4 gap-2">
        {devices.map((id) => {
          const pts = grouped.get(id) ?? []
          const last = pts[pts.length - 1]
          return (
            <div key={id} className="flex min-h-0 flex-col rounded-md border border-border bg-background px-2 py-1">
              <div className="flex items-baseline justify-between gap-1">
                <span className="truncate font-mono text-[11px]">{id}</span>
                <span className="font-mono text-xs font-semibold tabular-nums" style={{ color: METRIC_STROKE[metric] }}>
                  {last ? `${formatNumber(last[metric], spec.digits)}` : "—"}
                </span>
              </div>
              <div className="relative min-h-0 flex-1">
                {pts.length > 0 && endMs !== null ? (
                  <svg viewBox="0 0 200 60" preserveAspectRatio="none" className="h-full w-full">
                    <line x1="0" x2="200" y1="30" y2="30" stroke="var(--border)" strokeDasharray="2 3" />
                    <path
                      d={linePath(pts, metric, domain, endMs, 200, 60)}
                      fill="none"
                      stroke={METRIC_STROKE[metric]}
                      strokeWidth={1.75}
                      vectorEffect="non-scaling-stroke"
                    />
                  </svg>
                ) : (
                  <div className="flex h-full items-center justify-center text-[11px] text-muted-foreground">
                    Awaiting data
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </section>
  )
}

function CodeDrawer({ onClose }: { onClose: () => void }) {
  const code = useQuery({
    queryKey: ["code-sample"],
    queryFn: ({ signal }) => fetchCode(signal),
    retry: false,
    staleTime: 60_000,
  })

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose()
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [onClose])

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/40" onClick={onClose}>
      <aside
        role="dialog"
        aria-label="Elastic channel sample code"
        className="flex h-full w-[min(760px,55vw)] flex-col border-l border-border bg-card shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2 border-b border-border px-4 py-3">
          <Code2 className="h-4 w-4 text-primary" />
          <span className="text-sm font-semibold">Snowpipe Streaming SDK · Elastic channel</span>
          <span className="font-mono text-xs text-muted-foreground">{code.data?.path ?? "examples/minimal_elastic.py"}</span>
          <Button variant="ghost" size="icon" className="ml-auto" aria-label="Close" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>
        <div className="min-h-0 flex-1 overflow-auto p-4">
          {code.isPending && <p className="text-sm text-muted-foreground">Loading sample…</p>}
          {code.error && <p className="text-sm text-destructive">{code.error.message}</p>}
          {code.data && <HighlightedPython source={code.data.source} />}
        </div>
      </aside>
    </div>
  )
}

function HighlightedPython({ source }: { source: string }) {
  const tokens = useMemo(() => highlightPython(source), [source])
  return (
    <pre className="whitespace-pre rounded-md bg-[#0f1b2d] p-4 font-mono text-[13px] leading-relaxed text-[#d6deeb]">
      <code>
        {tokens.map((t, i) =>
          t.type === "plain" ? t.text : (
            <span key={i} className={TOKEN_CLASS[t.type]}>{t.text}</span>
          ),
        )}
      </code>
    </pre>
  )
}
