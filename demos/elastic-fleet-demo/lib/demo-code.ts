/**
 * Reads the maintained, shareable SDK example for the code drawer. The drawer shows
 * this file as-is so the on-screen snippet can never drift from the real sample, except
 * that connection identifiers (account, user, url) are masked for screen recording.
 */

import fs from "fs/promises"
import path from "path"
import type { CodeSample } from "@/lib/demo-types"

/** Relative to the app root (standalone server.js chdirs to its own root). */
export const CODE_SAMPLE_PATH = "examples/minimal_elastic.py"
const MAX_SAMPLE_BYTES = 64 * 1024

/** Keys whose string values identify the presenter's account; shown as "***". */
const MASKED_KEYS = ["account", "user", "url"]
const MASK_RE = new RegExp(`(["'](?:${MASKED_KEYS.join("|")})["']\\s*:\\s*)(["'])[^"'\\n]*\\2`, "g")

export function maskConnectionValues(source: string): string {
  return source.replace(MASK_RE, "$1$2***$2")
}

export async function readCodeSample(root: string = process.cwd()): Promise<CodeSample | null> {
  const file = path.join(root, CODE_SAMPLE_PATH)
  try {
    const stat = await fs.stat(file)
    if (!stat.isFile() || stat.size > MAX_SAMPLE_BYTES) return null
    const source = maskConnectionValues(await fs.readFile(file, "utf8"))
    return { language: "python", path: CODE_SAMPLE_PATH, source }
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return null
    throw e
  }
}
