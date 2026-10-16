/**
 * Tiny dependency-free Python tokenizer for the code drawer. Good enough for the
 * maintained sample (examples/minimal_elastic.py), not a full Python lexer.
 * Concatenating every token's text always reproduces the input exactly.
 */

export type TokenType =
  | "comment"
  | "string"
  | "keyword"
  | "builtin"
  | "number"
  | "decorator"
  | "definition"
  | "call"
  | "sdk"
  | "plain"

export interface Token {
  type: TokenType
  text: string
}

/** SDK calls the talk track names on camera; rendered with extra emphasis. */
export const SDK_CALLS = new Set([
  "from_table",
  "get_elastic_channel",
  "append_rows",
  "append_row",
  "set_success_handler",
  "set_error_handler",
  "wait_for_flush",
  "close",
])

const KEYWORDS = new Set([
  "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class",
  "continue", "def", "del", "elif", "else", "except", "finally", "for", "from", "global",
  "if", "import", "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return",
  "try", "while", "with", "yield",
])

const BUILTINS = new Set([
  "print", "len", "range", "str", "int", "float", "dict", "list", "set", "tuple", "bool",
  "isinstance", "enumerate", "zip", "min", "max", "sum", "open", "super", "self",
])

// Order matters: comments and strings first so their contents are never re-tokenized.
const RULES: Array<[TokenType | "word", RegExp]> = [
  ["comment", /#[^\n]*/y],
  ["string", /[rRbBuUfF]{0,2}("""[\s\S]*?(?:"""|$)|'''[\s\S]*?(?:'''|$))/y],
  ["string", /[rRbBuUfF]{0,2}("(?:\\.|[^"\\\n])*"?|'(?:\\.|[^'\\\n])*'?)/y],
  ["decorator", /@[A-Za-z_][\w.]*/y],
  ["number", /\b(?:0[xX][\da-fA-F_]+|\d[\d_]*(?:\.\d[\d_]*)?(?:[eE][+-]?\d+)?)\b/y],
  ["word", /[A-Za-z_]\w*/y],
]

export function highlightPython(source: string): Token[] {
  const tokens: Token[] = []
  let plain = ""
  let prevWord = ""
  const flush = () => {
    if (plain) tokens.push({ type: "plain", text: plain })
    plain = ""
  }

  let i = 0
  outer: while (i < source.length) {
    for (const [type, re] of RULES) {
      re.lastIndex = i
      const m = re.exec(source)
      if (!m || m[0].length === 0) continue
      const text = m[0]
      let t: TokenType
      if (type === "word") {
        const next = source.slice(i + text.length).match(/^\s*\(/)
        if (prevWord === "def" || prevWord === "class") t = "definition"
        else if (KEYWORDS.has(text)) t = "keyword"
        else if (next && SDK_CALLS.has(text)) t = "sdk"
        else if (BUILTINS.has(text)) t = "builtin"
        else if (next) t = "call"
        else t = "plain"
        prevWord = text
      } else {
        t = type
        prevWord = ""
      }
      if (t === "plain") {
        plain += text
      } else {
        flush()
        tokens.push({ type: t, text })
      }
      i += text.length
      continue outer
    }
    const ch = source[i]
    if (!/\s/.test(ch)) prevWord = ""
    plain += ch
    i += 1
  }
  flush()
  return tokens
}

/** Fixed dark-editor palette (same in light and dark app themes). */
export const TOKEN_CLASS: Record<TokenType, string> = {
  comment: "italic text-[#7f8c98]",
  string: "text-[#a5d6a7]",
  keyword: "font-semibold text-[#c792ea]",
  builtin: "text-[#82aaff]",
  number: "text-[#f78c6c]",
  decorator: "text-[#ffcb6b]",
  definition: "font-semibold text-[#ffcb6b]",
  call: "text-[#82aaff]",
  sdk: "font-semibold text-[#29b5e8] underline decoration-[#29b5e8]/40 underline-offset-4",
  plain: "",
}
