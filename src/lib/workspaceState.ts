import { derived, get, writable } from "svelte/store";
import type { AnnotationPatch, Backend } from "./backend";
import type { FlagEntry, FlagFilterValue, FlagSymbol, LoadProjectResponse } from "./types";
import { createWorkspacePages } from "./workspacePages.ts";
import { emptySelection, selectionAfterScopeChange, setRowsSelected, toggleRowId } from "./selection.ts";
import type { SelectionScope } from "./selection";
import { displayColumnOrder } from "./columnLayoutActions.ts";
import type { ColumnDisplayFormat } from "./dateDisplay";
import { logicalRowAt } from "./virtualScroll.ts";
import { tableRowHeight } from "./tableTextSize.ts";
import { showToast } from "./utils/toast.ts";
import { effectiveSearchColumns, normalizeSearchText } from "./searchScope.ts";
import { PAGE_SIZE, normalizeFlag, applyAnnotationEntry } from "./components/project_view/state.ts";
import type { AppliedFilters, CachedRow, MemoEditorTarget } from "./components/project_view/state";

/** Owns one active investigation's stores, bounded pages and row mutations. */
export function createWorkspaceState(notifyError = (message: string) => showToast(message, "error")) {
  let disposed = false;
  const projectDetail = writable<LoadProjectResponse | null>(null);
  const backend = writable<Backend | null>(null);

  /**
   * Maps internal column ids to display labels. Data values, sort keys,
   * conditions and search scopes keep using the id; only display code reads
   * this map. Legacy projects map every id to its own name.
   */
  const columnLabels = derived(projectDetail, (detail) => {
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

  const currentProjectId = writable<string | null>(null);
  const lastHiddenColumnsRef = writable<string[] | null>(null);
  const hiddenColumns = writable(new Set<string>());
  /** Missing keeps the first-visible default; empty explicitly unfreezes data. */
  const frozenThrough = writable<string | undefined>(undefined);
  const columnOrder = writable<string[]>([]);
  const columnFormats = writable<Record<string, ColumnDisplayFormat>>({});
  const orderedColumns = derived([projectDetail, columnOrder], ([detail, order]) =>
    displayColumnOrder(detail?.columns ?? [], order));

  const search = writable("");
  const searchColumns = writable<string[]>([]);
  const flagFilter = writable<FlagFilterValue>("all");
  const bookmarkedOnly = writable(false);
  /**
   * The QuerySpec that produced the current table result. Panels, bulk actions
   * and export read this snapshot instead of the live inputs, which may still be
   * waiting for the debounce.
   */
  const appliedFilters = writable<AppliedFilters>({
    search: "",
    flag: "all",
    bookmarkedOnly: false,
    searchColumns: [],
    condition: null,
  });

  const sortKey = writable<string | null>(null);
  const sortDirection = writable<"asc" | "desc">("asc");

  const isExporting = writable(false);
  const sortRevision = writable(0);

  /** Current screen generation. Bumped on project changes and on unmount. */
  const viewGeneration = writable(0);
  function pendingPageKey(
    generation: number,
    searchGeneration: number,
    page: number
  ): string {
    return `${generation}:${searchGeneration}:${page}`;
  }

  function invalidateViewRequests(): void {
    viewGeneration.update((value) => value + 1);
    pendingAnnotations.clear();
    lastAnnotationRevision = -1;
    needsAnnotationRefresh = false;
    annotationBlockedPages.clear();
  }
  const totalRows = writable(0);
  /** Rows matching the applied QuerySpec. Sole source for the virtual row count. */
  const totalFilteredRows = writable(0);
  /** True while the first page of a newly applied result is loading. */
  const appliedResultLoading = writable(false);
  /** A result count is available only after this query has returned successfully. */
  const queryCountKnown = writable(false);
  const queryError = writable<{ message: string; page: number } | null>(null);
  /** Annotated rows in the whole project. */
  const flaggedCount = writable(0);
  /** Annotated rows inside the current result set. */
  const filteredFlaggedCount = writable(0);
  const pages = createWorkspacePages(() => Math.floor(logicalRowAt(
    get(scrollTop), get(totalFilteredRows), get(viewportHeight), get(tableRowHeight),
  ) / PAGE_SIZE));
  const {
    rowsCache, positionToRowIndex, pendingPages, loadedPages, largePages, updateCachedRow,
    currentVisiblePage, touchPage, registerLoadedPage: registerPage, resetPageCache,
  } = pages;

  function registerLoadedPage(page: number, offset: number, rows: CachedRow[], annotationRevision?: number): void {
    if (!isAnnotationRevisionCurrent(annotationRevision)) return;
    if (annotationRevision !== undefined) lastAnnotationRevision = annotationRevision;
    registerPage(page, offset, rows.map((row) => {
      const pending = pendingAnnotations.get(row.row_index);
      if (!pending) return row;
      // The query has already rejected stale annotation versions. This row is a
      // newer committed baseline, including bulk edits made between local saves.
      if (annotationRevision === undefined || annotationRevision >= pending.revision) {
        pending.confirmed = annotationEntry(row);
        if (annotationRevision !== undefined) pending.revision = annotationRevision;
      }
      pending.scopeKey = annotationScopeKey();
      const entry = pendingEntry(pending);
      pending.displayedFlag = normalizeFlag(entry.flag);
      return applyAnnotationEntry(row, entry);
    }));
  }

  /**
   * Explicit row selection, keyed by original row id. Row objects are never
   * copied here, so page-cache eviction does not affect the selection.
   */
  const selectedRowIds = writable<Set<number>>(new Set());
  const selectedCount = derived(selectedRowIds, (ids) => ids.size);
  let selectionScope: SelectionScope | null = null;

  function toggleRowSelection(rowIndex: number): void {
    selectedRowIds.update((selected) => toggleRowId(selected, rowIndex));
  }

  /** Selects or clears only the delivered rows (usually the rendered viewport). */
  function selectVisibleRows(
    rowIndices: readonly number[],
    isSelected = true
  ): void {
    selectedRowIds.update((selected) =>
      setRowsSelected(selected, rowIndices, isSelected)
    );
  }

  function clearSelection(): void {
    selectedRowIds.set(emptySelection());
  }

  /**
   * Ties the selection to a project and applied QuerySpec. A new project,
   * filter, search scope, condition, or sort starts a new selection; page moves
   * inside the same scope keep it.
   */
  function syncSelectionScope(
    projectId: string | null,
    specSignature: string
  ): void {
    const next: SelectionScope = { projectId, specSignature };
    selectedRowIds.update((selected) =>
      selectionAfterScopeChange(selected, selectionScope, next)
    );
    selectionScope = next;
  }

  const expandedCell = writable<{
    projectId: string;
    /** Original row id, stable across page-cache eviction. */
    rowIndex: number;
    columnId: string;
    /** List preview; never treated as the full value. */
    preview: string;
    /** Row snapshot for the surrounding-row view; annotation is not edited here. */
    row: CachedRow;
  } | null>(null);
  const memoEditor = writable<MemoEditorTarget | null>(null);

  interface PendingAnnotation {
    confirmed: FlagEntry;
    patches: Map<number, AnnotationPatch>;
    displayedFlag: string;
    revision: number;
    scopeKey: string;
  }
  // Retain only annotation fields for rows with outstanding saves, never row data.
  const pendingAnnotations = new Map<number, PendingAnnotation>();
  let annotationSequence = 0;
  let lastAnnotationRevision = -1;
  const annotationRefreshRequested = writable(0);
  let needsAnnotationRefresh = false;
  const annotationBlockedPages = new Set<number>();

  function annotationScopeKey(): string {
    const filters = get(appliedFilters);
    return JSON.stringify([normalizeSearchText(filters.search), filters.flag, filters.bookmarkedOnly,
      effectiveSearchColumns(filters.search, filters.searchColumns), filters.condition]);
  }

  function annotationEntry(row: CachedRow): FlagEntry {
    return { flag: normalizeFlag(row.flag), memo: row.memo,
      tags: [...(row.tags ?? [])], bookmarked: row.bookmarked ?? false };
  }

  function pendingEntry(state: PendingAnnotation): FlagEntry {
    let entry = { ...state.confirmed };
    for (const patch of state.patches.values()) entry = { ...entry, ...patch };
    return entry;
  }

  function pendingFlagDelta(filtered = false): number {
    let delta = 0;
    const scopeKey = filtered ? annotationScopeKey() : null;
    for (const state of pendingAnnotations.values()) {
      if (!filtered || state.scopeKey === scopeKey) {
        delta += Number(Boolean(state.displayedFlag)) - Number(Boolean(state.confirmed.flag));
      }
    }
    return delta;
  }

  function isAnnotationRevisionCurrent(revision?: number): boolean {
    return revision === undefined || revision >= lastAnnotationRevision;
  }

  /** Backend counts describe committed annotations; add only outstanding changes. */
  function applyAnnotationCounts(total: number, filtered: number, revision?: number): void {
    if (!isAnnotationRevisionCurrent(revision)) return;
    if (revision !== undefined) lastAnnotationRevision = revision;
    flaggedCount.set(Math.max(0, total + pendingFlagDelta()));
    filteredFlaggedCount.set(Math.max(0, filtered + pendingFlagDelta(true)));
  }

  function publishAnnotation(rowIndex: number, state: PendingAnnotation): void {
    const entry = pendingEntry(state);
    const flag = normalizeFlag(entry.flag);
    const delta = Number(Boolean(flag)) - Number(Boolean(state.displayedFlag));
    state.displayedFlag = flag;
    const previouslyBlocked = get(largePages);
    updateCachedRow(rowIndex, (cached) => applyAnnotationEntry(cached, entry));
    const blocked = get(largePages);
    if (blocked !== previouslyBlocked) {
      for (const page of blocked) {
        if (!previouslyBlocked.has(page)) annotationBlockedPages.add(page);
      }
      if (annotationBlockedPages.size > 0) needsAnnotationRefresh = true;
    }
    // An optimistic note/tag can exceed the cache bound and remove this page.
    // Retry once after pending saves settle, when a rollback or smaller retry
    // can fit again. Do not repeatedly reload the oversized pending value.
    if (!get(rowsCache).has(rowIndex) && get(largePages).has(currentVisiblePage())) {
      needsAnnotationRefresh = true;
    }
    if (delta !== 0) {
      flaggedCount.update((value) => Math.max(0, value + delta));
      if (state.scopeKey === annotationScopeKey()) {
        filteredFlaggedCount.update((value) => Math.max(0, value + delta));
      }
    }
  }

  /** Reconcile every save against confirmed storage, then replay pending fields. */
  async function saveAnnotation(projectId: string, row: CachedRow, patch: AnnotationPatch): Promise<boolean> {
    if (disposed || get(projectDetail)?.project.meta.id !== projectId) return false;
    const rowIndex = row.row_index;
    const generation = get(viewGeneration);
    let state = pendingAnnotations.get(rowIndex);
    if (!state) {
      const baseline = get(rowsCache).get(rowIndex) ?? row;
      state = {
        confirmed: annotationEntry(baseline),
        patches: new Map(), displayedFlag: normalizeFlag(baseline.flag), revision: -1,
        scopeKey: annotationScopeKey(),
      };
      pendingAnnotations.set(rowIndex, state);
    }
    const sequence = ++annotationSequence;
    state.patches.set(sequence, { ...patch, ...(patch.tags ? { tags: [...patch.tags] } : {}) });
    publishAnnotation(rowIndex, state);
    const isCurrent = () => !disposed && get(viewGeneration) === generation
      && get(projectDetail)?.project.meta.id === projectId && pendingAnnotations.get(rowIndex) === state;
    try {
      const api = get(backend);
      if (!api) throw new Error("Backend is not available.");
      const updated = await api.updateFlag({ projectId, rowIndex, ...patch });
      if (!isCurrent()) return false;
      if (updated.revision >= state.revision) {
        state.confirmed = { flag: normalizeFlag(updated.entry?.flag), memo: updated.entry?.memo ?? "",
          tags: [...(updated.entry?.tags ?? [])], bookmarked: updated.entry?.bookmarked ?? false };
        state.revision = updated.revision;
      }
      state.patches.delete(sequence);
      publishAnnotation(rowIndex, state);
      if (updated.revision >= lastAnnotationRevision) {
        lastAnnotationRevision = updated.revision;
        flaggedCount.set(Math.max(0, updated.flaggedRecords + pendingFlagDelta()));
      }
      // A changed result may contain this row outside its loaded pages. Let the
      // existing query path recount it instead of guessing result membership.
      if (state.scopeKey !== annotationScopeKey() || updated.revision < lastAnnotationRevision) needsAnnotationRefresh = true;
      return true;
    } catch (error) {
      if (!isCurrent()) return false;
      state.patches.delete(sequence);
      publishAnnotation(rowIndex, state);
      throw error;
    } finally {
      if (isCurrent() && state.patches.size === 0) {
        pendingAnnotations.delete(rowIndex);
        if (pendingAnnotations.size === 0 && needsAnnotationRefresh) {
          // Only optimistic annotation growth created these markers. Permit one
          // fresh read after settlement, including pages the user scrolled away
          // from. A truly oversized saved page marks itself again when read.
          if (annotationBlockedPages.size > 0) {
            largePages.update(pages => new Set([...pages].filter(page => !annotationBlockedPages.has(page))));
            annotationBlockedPages.clear();
          }
          needsAnnotationRefresh = false;
          annotationRefreshRequested.update((value) => value + 1);
        }
      }
    }
  }

  const viewportHeight = writable(0);
  const scrollTop = writable(0);
  const scrollLeft = writable(0);
  const tableWidth = writable(0);
  const bodyScrollEl = writable<HTMLDivElement | null>(null);
  const headerScrollEl = writable<HTMLDivElement | null>(null);

  const visibleColumns = derived(
    [orderedColumns, hiddenColumns],
    ([$orderedColumns, $hiddenColumns]) => {
      return $orderedColumns.filter(
        (column) => !$hiddenColumns.has(column)
      );
    }
  );

  const toggleSort = (column: string) => {
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

    sortRevision.update((revision) => revision + 1);
  };

  const setFlag = async (row: CachedRow, flag: FlagSymbol) => {
    if (disposed) return;
    const projectId = get(projectDetail)?.project.meta.id;
    if (!projectId) return;
    const currentRow = get(rowsCache).get(row.row_index) ?? row;
    const nextFlag = normalizeFlag(currentRow.flag) === flag ? "" : flag;
    try {
      await saveAnnotation(projectId, row, { flag: nextFlag });
    } catch {
      notifyError("Failed to save flag.");
    }
  };

  const editMemo = (row: CachedRow, projectId: string) => {
    memoEditor.set({ projectId, rowIndex: row.row_index, row });
  };

  const openCell = (
    projectId: string,
    columnId: string,
    preview: string,
    row: CachedRow
  ) => {
    expandedCell.set({
      projectId,
      rowIndex: row.row_index,
      columnId,
      preview,
      row,
    });
  };

  const handleCellKeydown = (
    event: KeyboardEvent,
    projectId: string,
    columnId: string,
    preview: string,
    row: CachedRow
  ) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      openCell(projectId, columnId, preview, row);
    }
  };

  const dispose = () => {
    if (disposed) return;
    disposed = true;
    invalidateViewRequests();
    rowsCache.set(new Map());
    positionToRowIndex.set(new Map());
    pendingPages.set(new Set());
    loadedPages.set(new Set());
    resetPageCache();
    selectedRowIds.set(new Set());
    selectionScope = null;
    expandedCell.set(null);
    memoEditor.set(null);
    projectDetail.set(null);
    currentProjectId.set(null);
    backend.set(null);
    bodyScrollEl.set(null);
    headerScrollEl.set(null);
  };

  return {
    rowHeight: tableRowHeight,
    projectDetail,
    backend,
    columnLabels,
    currentProjectId,
    lastHiddenColumnsRef,
    hiddenColumns,
    search,
    searchColumns,
    flagFilter,
    bookmarkedOnly,
    appliedFilters,
    sortKey,
    sortDirection,
    isExporting,
    rowsCache,
    viewGeneration,
    pendingPages,
    loadedPages,
    largePages,
    pendingPageKey,
    invalidateViewRequests,
    totalRows,
    totalFilteredRows,
    appliedResultLoading,
    queryCountKnown,
    queryError,
    flaggedCount,
    filteredFlaggedCount,
    positionToRowIndex,
    selectedRowIds,
    selectedCount,
    toggleRowSelection,
    selectVisibleRows,
    clearSelection,
    syncSelectionScope,
    expandedCell,
    memoEditor,
    saveAnnotation,
    applyAnnotationCounts,
    isAnnotationRevisionCurrent,
    annotationRefreshRequested,
    updateCachedRow,
    viewportHeight,
    scrollTop,
    scrollLeft,
    tableWidth,
    bodyScrollEl,
    headerScrollEl,
    currentVisiblePage,
    touchPage,
    registerLoadedPage,
    resetPageCache,
    visibleColumns,
    frozenThrough,
    columnOrder,
    columnFormats,
    orderedColumns,
    toggleSort,
    setFlag,
    editMemo,
    openCell,
    handleCellKeydown,
    sortRevision,
    dispose
  };
}

export type WorkspaceState = ReturnType<typeof createWorkspaceState>;
