/**
 * GET /api/snapshot?run_id=<uuid>&clients=1,2,3
 *
 * Read-only consolidated dashboard snapshot (CONTRACT.md). All values are queried
 * from Snowflake with bound parameters. There are no mutation routes in this app.
 */

import { querySnowflake } from "@/lib/snowflake"
import { isCrossSiteRequest, parseSnapshotParams } from "@/lib/demo-params"
import { createSnapshotService, defaultWarehouse } from "@/lib/demo-snapshot"

export const dynamic = "force-dynamic"

const service = createSnapshotService({ query: querySnowflake, warehouse: defaultWarehouse() })

const NO_STORE = { "Cache-Control": "no-store" }

export async function GET(request: Request) {
  if (isCrossSiteRequest(request.headers)) {
    return Response.json({ error: "Cross-site requests are not allowed" }, { status: 403, headers: NO_STORE })
  }
  const parsed = parseSnapshotParams(new URL(request.url).searchParams)
  if (!parsed.ok) {
    return Response.json({ error: parsed.error }, { status: 400, headers: NO_STORE })
  }
  const snapshot = await service.getSnapshot(parsed.params)
  return Response.json(snapshot, { headers: NO_STORE })
}
