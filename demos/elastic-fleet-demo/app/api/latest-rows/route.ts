/**
 * GET /api/latest-rows?run_id=<uuid>
 *
 * Read-only "Live query: ROUTER_TELEMETRY" panel data (CONTRACT.md "Live landing
 * table"): COUNT(*) for the run and the newest 12 rows, plus the exact SQL text
 * executed. run_id is optional (newest run, same rule as /api/snapshot) and bound.
 */

import { querySnowflake } from "@/lib/snowflake"
import { isCrossSiteRequest, parseRunParam } from "@/lib/demo-params"
import { defaultWarehouse } from "@/lib/demo-snapshot"
import { createLatestRowsService } from "@/lib/demo-latest-rows"

export const dynamic = "force-dynamic"

const service = createLatestRowsService({ query: querySnowflake, warehouse: defaultWarehouse() })

const NO_STORE = { "Cache-Control": "no-store" }

export async function GET(request: Request) {
  if (isCrossSiteRequest(request.headers)) {
    return Response.json({ error: "Cross-site requests are not allowed" }, { status: 403, headers: NO_STORE })
  }
  const parsed = parseRunParam(new URL(request.url).searchParams)
  if (!parsed.ok) {
    return Response.json({ error: parsed.error }, { status: 400, headers: NO_STORE })
  }
  const latest = await service.getLatestRows(parsed.runId)
  return Response.json(latest, { headers: NO_STORE })
}
