import type { BulkAnnotateArgs } from "./backend";
import type { FlagSymbol, IocEntry } from "./types";

export function isIocFlag(value: string): value is FlagSymbol {
  return value === "safe" || value === "suspicious" || value === "critical";
}

export function requireIocFlag(value: string, location = "IOC rule"): FlagSymbol {
  if (!isIocFlag(value)) {
    throw new Error(`${location}: invalid flag ${JSON.stringify(value)}. Choose Safe, Suspicious, or Critical.`);
  }
  return value;
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

const MAX_CELL_VALUE_BYTES = 64 * 1024;

/** Saved cell rules use the same exact-value predicate as Filter in. */
export function prepareIocEntries(entries: readonly IocEntry[]): IocEntry[] {
  return entries.flatMap<IocEntry>((entry, index) => {
    const location = `IOC rule ${index + 1}`;
    const flag = requireIocFlag(entry.flag, location), tag = entry.tag.trim();
    if (entry.cell) {
      if (entry.query.trim()) throw new Error(`${location}: choose a query or a cell value, not both.`);
      const { column, value } = entry.cell;
      if (typeof column !== "string" || !column.trim() || typeof value !== "string") {
        throw new Error(`${location}: choose a column and enter its exact value.`);
      }
      if (new TextEncoder().encode(value).length > MAX_CELL_VALUE_BYTES) {
        throw new Error(`${location}: cell values must be at most 64 KiB.`);
      }
      return [{ flag, tag, query: "", cell: { column, value } }];
    }
    const query = entry.query.trim();
    if (!query) {
      if (tag) throw new Error(`${location}: Enter a query first.`);
      return [];
    }
    return [{ flag, tag, query: requireIocQuery(query, location) }];
  });
}

export function createCellIocEntry(column: string, value: string): IocEntry {
  return prepareIocEntries([{ flag: "critical", tag: "", query: "", cell: { column, value } }])[0];
}

export function iocEntryError(entry: IocEntry, columns?: readonly string[]): string | null {
  try {
    if (!prepareIocEntries([entry]).length) return "Enter a query first.";
    if (entry.cell && columns && !columns.includes(entry.cell.column)) {
      return "This column is unavailable in the current project.";
    }
    return null;
  } catch (error) { return error instanceof Error ? error.message.replace(/^IOC rule 1: /, "") : "Invalid IOC rule."; }
}

/** IOC rules use the normal bulk preview/apply transaction, never a live overlay. */
export function iocBulkArgs(projectId: string, entry: IocEntry): BulkAnnotateArgs {
  const rule = prepareIocEntries([entry])[0];
  if (!rule) throw new Error("Enter a query first.");
  return {
    projectId, target: "filtered", search: rule.query, searchColumns: [],
    condition: rule.cell ? { column: rule.cell.column, op: "eq", values: [rule.cell.value], negated: false } : null,
    flagFilter: "all", bookmarkedOnly: false, sortKey: null, sortDirection: "asc",
    flag: requireIocFlag(rule.flag),
    ...(rule.tag ? { add_tags: [rule.tag] } : {}),
  };
}
