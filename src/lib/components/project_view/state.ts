import type { ConditionNode, FlagEntry, FlagFilterValue, FlagFilterSelection, FlagSymbol, ProjectRow } from "../../types";
import { buildCellCaches } from "../../ipcValues.ts";
import { FLAG_HINTS } from "../../skin.ts";
import { ROW_HEIGHT } from "../../virtualScroll.ts";
export { annotationVersions, bulkAnnotationVersions } from "../../annotationVersions.ts";

export const FLAG_OPTIONS = [
  {
    value: "critical",
    label: "Critical",
    hint: FLAG_HINTS.critical,
    tone: "critical",
  },
  {
    value: "suspicious",
    label: "Suspicious",
    hint: FLAG_HINTS.suspicious,
    tone: "suspicious",
  },
  { value: "safe", label: "Safe", hint: FLAG_HINTS.safe, tone: "safe" },
] as const;

export type { FlagFilterValue } from "../../types";

export const FLAG_FILTER_OPTIONS: Array<{
  value: FlagFilterSelection;
  label: string;
  hint?: string;
}> = [
  { value: "critical", label: "Critical", hint: FLAG_HINTS.critical },
  { value: "suspicious", label: "Suspicious", hint: FLAG_HINTS.suspicious },
  { value: "safe", label: "Safe", hint: FLAG_HINTS.safe },
  { value: "none", label: "Unflagged", hint: "–" },
];

export { ROW_HEIGHT };
export const BUFFER = 8;
export const SELECT_COL_WIDTH = 40;
export const INDEX_COL_WIDTH = 80;
export const FLAG_COL_WIDTH = 130;
export const MEMO_COL_WIDTH = 200;
export const MIN_MEMO_WIDTH = 120;
export const MAX_MEMO_WIDTH = 600;
export const MIN_DATA_WIDTH = 80;
export const WIDTH_LIMIT_CHARS = 100;
export const CHAR_PIXEL = 9;
export const COLUMN_PADDING = 32;
export const MAX_DATA_WIDTH = WIDTH_LIMIT_CHARS * CHAR_PIXEL + COLUMN_PADDING;
export const STICKY_COLUMNS_WIDTH =
  SELECT_COL_WIDTH + INDEX_COL_WIDTH + FLAG_COL_WIDTH + MEMO_COL_WIDTH;

export const PAGE_SIZE = 250;
export const PREFETCH_PAGES = 1;

export type CachedRow = ProjectRow & {
  memo: string;
  /** Text rendered in the table/detail view (null/missing are distinguished). */
  displayCache: Record<string, string>;
  /** Stable source-cell estimate, calculated while the display cache is built. */
  readonly cellBytes: number;
};

export type VirtualRow = { position: number; row: CachedRow | null };

export type AppliedFilters = {
  search: string;
  flag: FlagFilterValue;
  bookmarkedOnly: boolean;
  /** Empty means every data column. Independent from visible columns. */
  searchColumns: string[];
  condition: ConditionNode | null;
};

/**
 * Frozen output target captured when the export dialog opens. Background
 * search edits must not change what the dialog is about to write.
 */
export type ExportSnapshot = {
  search: string;
  searchColumns: string[];
  condition: ConditionNode | null;
  flagFilter: FlagFilterValue;
  bookmarkedOnly: boolean;
  sortKey: string | null;
  sortDirection: "asc" | "desc";
  /** Original row ids of the explicit selection at dialog open. */
  rowIndices: number[];
  totalRows: number;
  totalFilteredRows: number;
  selectedCount: number;
  /** Visible data column ids for the "Visible columns" option. */
  visibleColumns: string[];
};

export type ExportOptions = {
  scope: "all" | "filtered" | "selected";
  useVisibleColumns: boolean;
  includeAnnotations: boolean;
};

export type MemoEditorTarget = {
  projectId: string;
  /** Original row id, independent of page-cache lifetime. */
  rowIndex: number;
  /**
   * Row snapshot supplying the editor's initial values and patch baseline.
   * It is not part of the page cache and survives page eviction. The explicit
   * selection is kept by row id in a separate store for the same reason.
   */
  row: CachedRow;
};


export function normalizeFlag(f: string | null | undefined): FlagSymbol | "" {
  if (!f) return "";
  if (f === "safe" || f === "suspicious" || f === "critical") return f;
  return "";
}

export function normalizeRow(incoming: ProjectRow): CachedRow {
  const { displayCache, cellBytes } = buildCellCaches(incoming.cells);
  return {
    ...incoming,
    flag: normalizeFlag(incoming.flag) || "",
    memo: incoming.memo ?? "",
    displayCache,
    cellBytes,
  };
}

/**
 * Applies an update result to an existing cached row. The row data and caches
 * stay untouched; only annotation fields are replaced.
 */
export function applyAnnotationEntry(
  row: CachedRow,
  entry: FlagEntry | null
): CachedRow {
  const memo = entry?.memo ?? "";
  return {
    ...row,
    flag: normalizeFlag(entry?.flag),
    memo,
    tags: entry?.tags ?? [],
    bookmarked: entry?.bookmarked ?? false,
  };
}
