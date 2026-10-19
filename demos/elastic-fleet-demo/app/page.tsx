import { FleetDashboard } from "@/components/fleet-dashboard"

// Required: Snowflake is not reachable during docker build.
export const dynamic = "force-dynamic"

/** Optional `?run_id=<uuid>` pins a run; otherwise the newest run is followed. */
export default async function Home({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>
}) {
  const sp = await searchParams
  const runId = typeof sp.run_id === "string" && sp.run_id !== "" ? sp.run_id : null
  return <FleetDashboard runId={runId} />
}
