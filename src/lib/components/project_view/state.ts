import { derived, get, writable } from "svelte/store";
import type { Backend } from "../../backend";
import type { IocEntry, LoadProjectResponse, ProjectRow } from "../../types";

export const FLAG_OPTIONS = [
  { value: "safe", label: "Safe", hint: "✓", tone: "safe" },
  { value: "suspicious", label: "Suspicious", hint: "?", tone: "suspicious" },
  { value: "critical", label: "Critical", hint: "!", tone: "critical" },
] as const;

export type FlagSymbol = (typeof FLAG_OPTIONS)[number]["value"];
export type FlagFilterValue = FlagSymbol | "all" | "none" | "priority";

export const PRIORITY_FLAGS: FlagSymbol[] = ["suspicious", "critical"];

export const FLAG_FILTER_OPTIONS: Array<{
  value: FlagFilterValue;
  label: string;
  hint?: string;
}> = [
  { value: "all", label: "All" },
  { value: "safe", label: "Safe", hint: "✓" },
  { value: "suspicious", label: "Suspicious", hint: "?" },
  { value: "critical", label: "Critical", hint: "!" },
  { value: "priority", label: "Sus + Crit", hint: "!?" },
  { value: "none", label: "Unflagged", hint: "–" },
];

export const ROW_HEIGHT = 56;
export const BUFFER = 8;
export const INDEX_COL_WIDTH = 80;
export const FLAG_COL_WIDTH = 130;
export const MEMO_COL_WIDTH = 200;
export const MIN_DATA_WIDTH = 80;
export const WIDTH_LIMIT_CHARS = 100;
export const CHAR_PIXEL = 9;
export const COLUMN_PADDING = 32;
export const MAX_DATA_WIDTH = WIDTH_LIMIT_CHARS * CHAR_PIXEL + COLUMN_PADDING;
export const STICKY_COLUMNS_WIDTH =
  INDEX_COL_WIDTH + FLAG_COL_WIDTH + MEMO_COL_WIDTH;

export const PAGE_SIZE = 250;
export const PREFETCH_PAGES = 1;

export type CachedRow = ProjectRow & {
  memo: string;
  displayCache: Record<string, string>;
};

export type VirtualRow = { position: number; row: CachedRow | null };

export type AppliedFilters = {
  search: string;
  flag: FlagFilterValue;
  columns: string[];
};

export type ScheduledFilters = AppliedFilters & { resetScroll: boolean };

export const projectDetail = writable<LoadProjectResponse | null>(null);
export const backend = writable<Backend | null>(null);

export const currentProjectId = writable<string | null>(null);
export const lastHiddenColumnsRef = writable<string[] | null>(null);
export const hiddenColumns = writable(new Set<string>());
export const columnsOpen = writable(false);

export const search = writable("");
export const flagFilter = writable<FlagFilterValue>("all");

export const sortKey = writable<string | null>(null);
export const sortDirection = writable<"asc" | "desc">("asc");

export const isExporting = writable(false);
export const isUpdatingColumns = writable(false);

export const rowsCache = writable<Map<number, CachedRow>>(new Map());
export const pendingPages = writable<Set<number>>(new Set());
export const loadedPages = writable<Set<number>>(new Set());
export const totalRows = writable(0);
export const totalFlagged = writable(0);
export const flaggedCount = writable(0);

export const expandedCell = writable<{ column: string; value: string } | null>(
  null
);
export const memoEditor = writable<{ row: CachedRow } | null>(null);
export const iocManagerOpen = writable(false);

export const viewportHeight = writable(0);
export const scrollTop = writable(0);
export const tableWidth = writable(0);
export const bodyScrollEl = writable<HTMLDivElement | null>(null);
export const headerScrollEl = writable<HTMLDivElement | null>(null);

export const visibleColumns = derived(
  [projectDetail, hiddenColumns],
  ([$projectDetail, $hiddenColumns]) => {
    if (!$projectDetail) return [];
    return $projectDetail.columns.filter(
      (column) => !$hiddenColumns.has(column)
    );
  }
);

export const iocDraft = writable<IocEntry[]>([]);

export function normalizeIocFlag(value: string): FlagSymbol {
  const lowered = value.trim().toLowerCase();
  if (
    lowered === "critical" ||
    lowered === "suspicious" ||
    lowered === "safe"
  ) {
    return lowered as FlagSymbol;
  }
  return "safe";
}

export function sanitizeMemoInput(value: string): string {
  const withoutControl = value.replace(
    /[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g,
    ""
  );
  return withoutControl.replace(/<[^>]*>/g, "");
}

export function normalizeFlag(f: string | null | undefined): FlagSymbol | "" {
  if (!f) return "";
  const lower = f.trim().toLowerCase();
  if (lower === "safe" || lower === "suspicious" || lower === "critical") {
    return lower;
  }
  return "";
}

export function mapStoredFlag(
  flag: string | null | undefined
): FlagSymbol | "" {
  const normalized = normalizeFlag(flag);
  if (normalized) return normalized;
  const trimmed = flag?.trim();
  if (!trimmed) return "";
  if (trimmed === "◯") return "safe";
  if (trimmed === "?") return "suspicious";
  if (trimmed === "✗") return "critical";
  return "";
}

export function formatCell(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

export function normalizeRow(incoming: ProjectRow): CachedRow {
  const displayCache: Record<string, string> = {};
  for (const [column, value] of Object.entries(incoming.data)) {
    const formatted = formatCell(value);
    displayCache[column] = formatted;
  }
  return {
    ...incoming,
    flag: mapStoredFlag(incoming.flag) || "",
    memo: sanitizeMemoInput(incoming.memo ?? ""),
    displayCache,
  };
}

export const escapeCsvValue = (value: string): string =>
  /[\",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;

export const buildIocCsv = (entries: IocEntry[]) => {
  const header = "flag,tag,query";
  const rows = entries.map((entry) =>
    [entry.flag, entry.tag, entry.query].map(escapeCsvValue).join(",")
  );
  return [header, ...rows].join("\n");
};

export const parseIocCsvRows = (content: string): string[][] => {
  const rows: string[][] = [];
  let currentRow: string[] = [];
  let currentField = "";
  let inQuotes = false;

  for (let i = 0; i < content.length; i += 1) {
    const char = content[i];
    const next = content[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        currentField += '"';
        i += 1;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }

    if (char === "," && !inQuotes) {
      currentRow.push(currentField);
      currentField = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") {
        i += 1;
      }
      currentRow.push(currentField);
      if (currentRow.some((cell) => cell.trim().length > 0)) {
        rows.push([...currentRow]);
      }
      currentRow = [];
      currentField = "";
      continue;
    }

    currentField += char;
  }

  if (currentField.length > 0 || currentRow.length > 0) {
    currentRow.push(currentField);
    if (currentRow.some((cell) => cell.trim().length > 0)) {
      rows.push([...currentRow]);
    }
  }

  return rows;
};

export const parseIocCsvText = (content: string): IocEntry[] => {
  const table = parseIocCsvRows(content);
  if (!table.length) {
    return [];
  }
  const [header, ...body] = table;
  const firstCell = header[0]?.toLowerCase() ?? "";
  const hasHeader = firstCell.includes("flag");
  const records = hasHeader ? body : table;
  const result: IocEntry[] = [];
  for (const record of records) {
    const [flagValue = "", tag = "", queryValue = ""] = record;
    const normalizedQuery = queryValue.trim();
    if (!normalizedQuery) continue;
    result.push({
      flag: normalizeIocFlag(flagValue),
      tag: tag.trim(),
      query: normalizedQuery,
    });
  }
  return result;
};

export const toggleSort = (column: string) => {
  sortKey.update((current) => {
    if (current === column) {
      sortDirection.update((dir) => (dir === "asc" ? "desc" : "asc"));
      if (get(sortDirection) === "asc") {
        return null;
      }
      return current;
    } else {
      sortDirection.set("asc");
      return column;
    }
  });
};

export const forceRefreshFilteredRows = (resetScroll: boolean) => {
  if (!get(projectDetail)) return Promise.resolve();
  // This is a bit of a hack, but it's the easiest way to force a refresh
  // without rewriting a lot of the filtering logic.
  const currentSortKey = get(sortKey);
  sortKey.set(currentSortKey);
  return Promise.resolve();
};

export const setFlag = async (row: CachedRow, flag: FlagSymbol) => {
  const currentFlag = normalizeFlag(row.flag);
  const nextFlag = currentFlag === flag ? "" : flag;
  try {
    await get(backend).updateFlag({
      projectId: get(projectDetail).project.meta.id,
      rowIndex: row.row_index,
      flag: nextFlag ?? "",
      memo: row.memo && row.memo.trim().length ? row.memo : null,
    });
    await forceRefreshFilteredRows(false);
  } catch (error) {
    console.error(error);
    // dispatch('notify', { message: 'Failed to update flag.', tone: 'error' });
  }
};

export const editMemo = (row: CachedRow) => {
  memoEditor.set({ row });
};

export const openCell = (column: string, value: string) => {
  expandedCell.set({ column, value });
};

export const handleCellKeydown = (
  event: KeyboardEvent,
  column: string,
  value: string
) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    openCell(column, value);
  }
};
