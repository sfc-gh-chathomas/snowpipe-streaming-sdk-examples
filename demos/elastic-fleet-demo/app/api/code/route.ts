/**
 * GET /api/code — the maintained minimal Elastic channel example (account, user, url masked).
 */

import { isCrossSiteRequest } from "@/lib/demo-params"
import { CODE_SAMPLE_PATH, readCodeSample } from "@/lib/demo-code"

export const dynamic = "force-dynamic"

export async function GET(request: Request) {
  if (isCrossSiteRequest(request.headers)) {
    return Response.json({ error: "Cross-site requests are not allowed" }, { status: 403 })
  }
  try {
    const sample = await readCodeSample()
    if (!sample) {
      return Response.json({ error: `Sample not found: ${CODE_SAMPLE_PATH}` }, { status: 404 })
    }
    return Response.json(sample, { headers: { "Cache-Control": "no-store" } })
  } catch (e) {
    console.error("Code sample read failed:", e)
    return Response.json({ error: "Could not read code sample" }, { status: 500 })
  }
}
