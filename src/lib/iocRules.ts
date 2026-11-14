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

/** IOC rules use the normal bulk preview/apply transaction, never a live overlay. */
export function iocBulkArgs(projectId: string, entry: IocEntry): BulkAnnotateArgs {
  const search = entry.query.trim(), tag = entry.tag.trim();
  if (!search) throw new Error("Enter a query first.");
  const flag = requireIocFlag(entry.flag);
  return {
    projectId, target: "filtered", search, searchColumns: [], condition: null,
    flagFilter: "all", bookmarkedOnly: false, sortKey: null, sortDirection: "asc",
    flag,
    ...(tag ? { add_tags: [tag] } : {}),
  };
}
