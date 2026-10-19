"use client"

/**
 * "Live query: ROUTER_TELEMETRY" panel. Polls GET /api/latest-rows (~2 s, never
 * overlapping) and shows the exact SQL that ran, COUNT(*) for the run and the
 * newest 12 landed rows. Rows that were not in the previous poll flash briefly.
 */

import { useEffect, useRef, useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { Database } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import { LATEST_ROWS_LIMIT, POLL_INTERVAL_MS, type LatestRows } from "@/lib/demo-types"
import { formatEventTime, formatNumber, isLatestStale, newRowIds, shortRun } from "@/lib/demo-view"

const HIGHLIGHT_MS = 1500
const NO_IDS: ReadonlySet<string> = new Set()

async function fetchLatestRows(runId: string | null, signal: AbortSignal): Promise<LatestRows> {
  const qs = runId ? `?${new URLSearchParams({ run_id: runId })}` : ""
  const res = await fetch(`/api/latest-rows${qs}`, { signal, cache: "no-store" })
  const body = await res.json().catch(() => null)
  if (!res.ok) throw new Error(body?.error ?? `Latest-rows request failed (${res.status})`)
  return body as LatestRows
}

export function LiveQuery({ runId, now, className }: { runId: string | null; now: number; className?: string }) {
  const query = useQuery({
    queryKey: ["latest-rows", runId ?? "latest"],
    queryFn: ({ signal }) => fetchLatestRows(runId, signal),
    refetchInterval: POLL_INTERVAL_MS,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: false,
    retry: false,
  })

  // Previous poll's EVENT_IDs per run; reset when the run changes so a new run never
  // inherits highlights (or rows) from the old one.
  const seen = useRef<{ runId: string | null; ids: Set<string> } | null>(null)
  const [highlight, setHighlight] = useState<ReadonlySet<string>>(NO_IDS)
  const latest = query.data

  useEffect(() => {
    if (!latest) return
    const prev = seen.current && seen.current.runId === latest.run_id ? seen.current.ids : null
    const fresh = newRowIds(prev, latest.rows)
    seen.current = { runId: latest.run_id, ids: new Set(latest.rows.map((r) => r.event_id)) }
    setHighlight(fresh.size > 0 ? fresh : NO_IDS)
    if (fresh.size === 0) return
    const t = setTimeout(() => setHighlight(NO_IDS), HIGHLIGHT_MS)
    return () => clearTimeout(t)
  }, [latest])

  return (
    <LiveQueryView
      latest={latest}
      highlight={highlight}
      stale={isLatestStale(latest, query.dataUpdatedAt, now)}
      fetchError={query.error ? query.error.message : null}
      className={className}
    />
  )
}

export function LiveQueryView({
  latest,
  highlight,
  stale,
  fetchError,
  className,
}: {
  latest: LatestRows | undefined
  highlight: ReadonlySet<string>
  stale: boolean
  fetchError: string | null
  className?: string
}) {
  const error = latest?.query_error ?? fetchError
  const rows = latest?.rows ?? []

  return (
    <section
      className={cn("flex min-h-0 flex-col rounded-lg border border-border bg-card p-3", className)}
      aria-label="Live query: ROUTER_TELEMETRY"
    >
      <div className="mb-1.5 flex items-center gap-3">
        <Database className="h-4 w-4 text-primary" />
        <h2 className="text-sm font-semibold">
          Live query: <span className="font-mono">ROUTER_TELEMETRY</span>
        </h2>
        <span className="text-[11px] text-muted-foreground">
          RUN_ID = ? bound to{" "}
          <span className="font-mono text-foreground" title={latest?.run_id ?? ""}>
            {shortRun(latest?.run_id ?? null)}
          </span>{" "}
          · re-run every {POLL_INTERVAL_MS / 1000} s
        </span>
        {stale && (
          <Badge className="bg-amber-500 text-black" data-testid="latest-stale">
            Stale
          </Badge>
        )}
        {error && (
          <span className="truncate text-[11px] text-destructive" title={error} data-testid="latest-error">
            {error}
          </span>
        )}
        <div className="ml-auto flex items-baseline gap-2">
          <span className="font-mono text-[11px] text-muted-foreground">COUNT(*)</span>
          <span className="font-mono text-xl font-semibold leading-7 tabular-nums" data-testid="latest-count">
            {latest ? formatNumber(latest.total_rows) : "—"}
          </span>
        </div>
      </div>
      <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,5fr)_minmax(0,7fr)] gap-3">
        <pre
          className="min-h-0 overflow-hidden rounded-md border border-border bg-background px-3 py-2 font-mono text-[11px] leading-[1.45]"
          data-testid="latest-sql"
        >
          <code>{latest?.sql ?? ""}</code>
        </pre>
        <div className="min-h-0 overflow-hidden rounded-md border border-border bg-background">
          <table className="w-full table-fixed font-mono text-[11px] leading-4 tabular-nums">
            <thead className="text-left text-muted-foreground">
              <tr className="border-b border-border">
                <th className="w-[92px] px-2 py-0.5 font-medium">EVENT_TS</th>
                <th className="w-[84px] px-2 py-0.5 font-medium">DEVICE_ID</th>
                <th className="w-[46px] px-2 py-0.5 text-right font-medium">CLIENT</th>
                <th className="w-[78px] px-2 py-0.5 font-medium">REGION</th>
                <th className="w-[54px] px-2 py-0.5 text-right font-medium">TEMP_C</th>
                <th className="w-[46px] px-2 py-0.5 text-right font-medium">CPU</th>
                <th className="w-[60px] px-2 py-0.5 text-right font-medium">MBPS</th>
                <th className="w-[64px] px-2 py-0.5 font-medium">SCENARIO</th>
                <th className="px-2 py-0.5 font-medium">EVENT_ID</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const isNew = highlight.has(r.event_id)
                return (
                  <tr
                    key={r.event_id}
                    data-new={isNew ? "true" : undefined}
                    className={cn("transition-colors", isNew ? "bg-primary/25 duration-0" : "duration-1000")}
                  >
                    <td className="px-2 py-0">{formatEventTime(r.event_ts)}</td>
                    <td className="px-2 py-0">{r.device_id}</td>
                    <td className="px-2 py-0 text-right">{r.client_id}</td>
                    <td className="px-2 py-0">{r.region}</td>
                    <td className="px-2 py-0 text-right">{formatNumber(r.temperature_c, 1)}</td>
                    <td className="px-2 py-0 text-right">{formatNumber(r.cpu_pct, 0)}</td>
                    <td className="px-2 py-0 text-right">{formatNumber(r.traffic_mbps, 0)}</td>
                    <td className={cn("px-2 py-0", r.scenario === "heat" && "text-amber-500")}>{r.scenario}</td>
                    <td className="truncate px-2 py-0 text-muted-foreground" title={r.event_id}>
                      {r.event_id}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          {rows.length === 0 && (
            <p className="px-2 py-3 text-[11px] text-muted-foreground" data-testid="latest-empty">
              {!latest
                ? "Running first query…"
                : latest.run_id
                  ? "No rows for this run yet — waiting for the first commit."
                  : "No run with events in the last 15 minutes."}
            </p>
          )}
          {rows.length > 0 && rows.length < LATEST_ROWS_LIMIT && (
            <p className="px-2 py-1 text-[11px] text-muted-foreground">{rows.length} rows so far</p>
          )}
        </div>
      </div>
    </section>
  )
}
