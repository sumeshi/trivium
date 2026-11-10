import { derived, get, writable } from "svelte/store";
import type { Backend } from "../../backend";
import type {
  ConditionNode,
  FlagSymbol,
  IocEntry,
  LoadProjectResponse,
  ProjectRow,
} from "../../types";
import { FLAG_HINTS } from "../../skin";

export const FLAG_OPTIONS = [
  { value: "safe", label: "Safe", hint: FLAG_HINTS.safe, tone: "safe" },
  {
    value: "suspicious",
    label: "Suspicious",
    hint: FLAG_HINTS.suspicious,
    tone: "suspicious",
  },
  {
    value: "critical",
    label: "Critical",
    hint: FLAG_HINTS.critical,
    tone: "critical",
  },
] as const;

export type FlagFilterValue = FlagSymbol | "all" | "none" | "priority";

export const PRIORITY_FLAGS: FlagSymbol[] = ["suspicious", "critical"];

export const FLAG_FILTER_OPTIONS: Array<{
  value: FlagFilterValue;
  label: string;
  hint?: string;
}> = [
  { value: "all", label: "All" },
  { value: "safe", label: "Safe", hint: FLAG_HINTS.safe },
  { value: "suspicious", label: "Suspicious", hint: FLAG_HINTS.suspicious },
  { value: "critical", label: "Critical", hint: FLAG_HINTS.critical },
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
  bookmarkedOnly: boolean;
  /** Empty means every data column. Independent from visible columns. */
  searchColumns: string[];
  condition: ConditionNode | null;
};

export const projectDetail = writable<LoadProjectResponse | null>(null);
export const backend = writable<Backend | null>(null);

/**
 * Maps internal column ids to display labels. Data values, sort keys,
 * conditions and search scopes keep using the id; only display code reads
 * this map. Legacy projects map every id to its own name.
 */
export const columnLabels = derived(projectDetail, (detail) => {
  const labels = new Map<string, string>();
  for (const meta of detail?.columns_meta ?? []) {
    labels.set(meta.id, meta.label);
  }
  for (const id of detail?.columns ?? []) {
    if (!labels.has(id)) {
      labels.set(id, id);
    }
  }
  return labels;
});

export function columnLabel(id: string): string {
  return get(columnLabels).get(id) ?? id;
}

export const currentProjectId = writable<string | null>(null);
export const lastHiddenColumnsRef = writable<string[] | null>(null);
export const hiddenColumns = writable(new Set<string>());
export const columnsOpen = writable(false);

export const search = writable("");
export const searchColumns = writable<string[]>([]);
export const flagFilter = writable<FlagFilterValue>("all");
export const bookmarkedOnly = writable(false);

export const sortKey = writable<string | null>(null);
export const sortDirection = writable<"asc" | "desc">("asc");

export const isExporting = writable(false);
export const isUpdatingColumns = writable(false);

export const rowsCache = writable<Map<number, CachedRow>>(new Map()); // Keyed by row_index
/** Current screen generation. Bumped on project changes and on unmount. */
export const viewGeneration = writable(0);
/** Pending pages are keyed by generation and page so stale responses cannot clear new work. */
export const pendingPages = writable<Set<string>>(new Set());
export const loadedPages = writable<Set<number>>(new Set());

export function pendingPageKey(
  generation: number,
  searchGeneration: number,
  page: number
): string {
  return `${generation}:${searchGeneration}:${page}`;
}

export function invalidateViewRequests(): void {
  viewGeneration.update((value) => value + 1);
}
export const totalRows = writable(0);
/** Rows matching the applied QuerySpec. Sole source for the virtual row count. */
export const totalFilteredRows = writable(0);
/** Annotated rows in the whole project. */
export const flaggedCount = writable(0);
/** Annotated rows inside the current result set. */
export const filteredFlaggedCount = writable(0);
// Maps display position (offset + i) to backend row_index to preserve backend ordering
export const positionToRowIndex = writable<Map<number, number>>(new Map());

export const expandedCell = writable<{
  column: string;
  value: string;
  row: CachedRow;
} | null>(null);
export type MemoEditorTarget = {
  projectId: string;
  rowIndex: number;
  /** Row snapshot taken when editing started, used as the rollback baseline. */
  row: CachedRow;
};
export const memoEditor = writable<MemoEditorTarget | null>(null);
export const iocManagerOpen = writable(false);

/**
 * Monotonic per-row request sequence shared by flag updates and memo saves.
 * Only the latest request for a row may roll its optimistic change back.
 */
const annotationRequestSeq = new Map<number, number>();

export function nextAnnotationRequest(rowIndex: number): number {
  const next = (annotationRequestSeq.get(rowIndex) ?? 0) + 1;
  annotationRequestSeq.set(rowIndex, next);
  return next;
}

export function isLatestAnnotationRequest(
  rowIndex: number,
  requestSeq: number
): boolean {
  return annotationRequestSeq.get(rowIndex) === requestSeq;
}

export function updateCachedRow(
  rowIndex: number,
  updater: (row: CachedRow) => CachedRow
): CachedRow | null {
  const existing = get(rowsCache).get(rowIndex);
  if (!existing) return null;
  const updated = updater(existing);
  rowsCache.update((cache) => {
    const nextCache = new Map(cache);
    nextCache.set(rowIndex, updated);
    return nextCache;
  });
  return updated;
}

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

export const toggleSort = (column: string) => {
  if (get(sortKey) === column) {
    if (get(sortDirection) === "asc") {
      sortDirection.set("desc");
    } else {
      sortKey.set(null);
      sortDirection.set("asc");
    }
  } else {
    sortKey.set(column);
    sortDirection.set("asc");
  }

  // Force refresh to apply new sort
  const currentProjectDetail = get(projectDetail);
  if (currentProjectDetail) {
    // Trigger a refresh by dispatching a custom event
    // This will be handled by ProjectView.svelte
    const event = new CustomEvent("sortChanged", {
      detail: {
        sortKey: get(sortKey),
        sortDirection: get(sortDirection),
      },
    });
    document.dispatchEvent(event);
  }
};

export const setFlag = async (row: CachedRow, flag: FlagSymbol) => {
  const rowIndex = row.row_index;
  // Read the latest optimistic row so rapid clicks toggle from what the table
  // currently shows instead of a stale click-time snapshot.
  const currentRow = get(rowsCache).get(rowIndex) ?? row;
  const currentFlag = normalizeFlag(currentRow.flag);
  const nextFlag = currentFlag === flag ? "" : flag;
  const previousHadFlag = Boolean(currentFlag);
  const nextHasFlag = Boolean(nextFlag);
  const flaggedDelta = (nextHasFlag ? 1 : 0) - (previousHadFlag ? 1 : 0);
  const requestSeq = nextAnnotationRequest(rowIndex);
  let requestProjectId: string | null = null;

  // Optimistic update: Update UI immediately
  updateCachedRow(rowIndex, (cached) => ({ ...cached, flag: nextFlag }));

  if (flaggedDelta !== 0) {
    flaggedCount.update((value) => Math.max(0, value + flaggedDelta));
    filteredFlaggedCount.update((value) => Math.max(0, value + flaggedDelta));
  }

  try {
    const backendInstance = get(backend);
    const projectDetailInstance = get(projectDetail);
    if (!backendInstance || !projectDetailInstance) {
      throw new Error("Backend or project detail not available");
    }
    requestProjectId = projectDetailInstance.project.meta.id;
    const updated = await backendInstance.updateFlag({
      projectId: requestProjectId,
      rowIndex,
      flag: nextFlag ?? "",
    });

    // The project may have changed while the request was in flight. Never
    // apply this row id to another project's cache.
    if (get(projectDetail)?.project.meta.id !== requestProjectId) return;

    // A stale success must not overwrite a newer optimistic change.
    if (isLatestAnnotationRequest(rowIndex, requestSeq)) {
      updateCachedRow(rowIndex, (cached) => ({
        ...cached,
        flag: mapStoredFlag(updated.flag),
      }));
    }
  } catch (error) {
    console.error(error);
    // A request from a project that is no longer displayed has nothing to
    // roll back on screen.
    if (
      requestProjectId &&
      get(projectDetail)?.project.meta.id !== requestProjectId
    ) {
      return;
    }
    // Roll back only this request, and only while it is still the newest one.
    if (!isLatestAnnotationRequest(rowIndex, requestSeq)) return;

    updateCachedRow(rowIndex, (cached) => ({ ...cached, flag: currentFlag }));

    if (flaggedDelta !== 0) {
      flaggedCount.update((value) => Math.max(0, value - flaggedDelta));
      filteredFlaggedCount.update((value) =>
        Math.max(0, value - flaggedDelta)
      );
    }
  }
};

export const editMemo = (row: CachedRow, projectId: string) => {
  memoEditor.set({ projectId, rowIndex: row.row_index, row });
};

export const openCell = (column: string, value: string, row: CachedRow) => {
  expandedCell.set({ column, value, row });
};

export const handleCellKeydown = (
  event: KeyboardEvent,
  column: string,
  value: string,
  row: CachedRow
) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    openCell(column, value, row);
  }
};
