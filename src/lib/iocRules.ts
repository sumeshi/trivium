import type { BulkAnnotateArgs } from "./backend";
import type { FlagSymbol, IocEntry } from "./types";

/** Preserve invalid stored values rather than choosing a severity for them. */
export function normalizeIocFlag(value: string): string {
  const flag = value.trim().toLowerCase();
  if (isIocFlag(flag)) return flag;
  if (flag === "◯") return "safe";
  if (flag === "?") return "suspicious";
  if (flag === "✗") return "critical";
  return value.trim();
}

export function isIocFlag(value: string): value is FlagSymbol {
  return value === "safe" || value === "suspicious" || value === "critical";
}

export function requireIocFlag(value: string, location = "IOC rule"): FlagSymbol {
  const flag = normalizeIocFlag(value);
  if (!isIocFlag(flag)) {
    throw new Error(`${location}: invalid flag ${JSON.stringify(value)}. Choose Safe, Suspicious, or Critical.`);
  }
  return flag;
}

/** Immediate input feedback only; the engine validates the canonical tokens. */
export function iocQueryError(query: string): string | null {
  if (!query.trim()) return "Enter a query first.";
  const parts = (query.match(/"[^"]*"|"[^"]*$|[^"\s|]+|\|+/gu) ?? [])
    .filter(part => !part.startsWith('"') || part.slice(1).replace(/"$/u, "").length > 0);
  let expectingTerm = true;
  for (let index = 0; index < parts.length; index += 1) {
    let part = parts[index];
    if (part.startsWith("|")) {
      if (expectingTerm) return "Enter a search term on each side of |.";
      expectingTerm = true;
      continue;
    }
    if (!part.startsWith('"')) {
      if (part.startsWith("-") && part.length > 1) part = part.slice(1);
      const colon = part.indexOf(":");
      if (colon >= 0) {
        part = part.slice(colon + 1);
        if (!part && parts[index + 1]?.startsWith('"')) {
          part = parts[++index].slice(1).replace(/"$/u, "");
        }
        if (!part) return "Enter search text after the column name.";
      }
    }
    expectingTerm = false;
  }
  return expectingTerm ? "Enter at least one search term and complete each |." : null;
}

export function requireIocQuery(value: string, location = "IOC rule"): string {
  const error = iocQueryError(value);
  if (error) throw new Error(`${location}: ${error}`);
  return value.trim();
}

/** IOC rules use the normal bulk preview/apply transaction, never a live overlay. */
export function iocBulkArgs(projectId: string, entry: IocEntry): BulkAnnotateArgs {
  const search = requireIocQuery(entry.query), tag = entry.tag.trim();
  const flag = requireIocFlag(entry.flag);
  return {
    projectId, target: "filtered", search, searchColumns: [], condition: null,
    flagFilter: "all", bookmarkedOnly: false, sortKey: null, sortDirection: "asc",
    flag,
    ...(tag ? { add_tags: [tag] } : {}),
  };
}
