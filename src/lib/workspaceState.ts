import { derived, get, writable } from "svelte/store";
import type { Backend } from "./backend";
import type { FlagFilterValue, FlagSymbol, IocEntry, LoadProjectResponse } from "./types";
import { createWorkspacePages } from "./workspacePages.ts";
import { emptySelection, selectionAfterScopeChange, setRowsSelected, toggleRowId } from "./selection.ts";
import type { SelectionScope } from "./selection";
import { logicalRowAt } from "./virtualScroll.ts";
import { PAGE_SIZE, normalizeFlag, applyAnnotationEntry } from "./components/project_view/state.ts";
import type { AppliedFilters, CachedRow, MemoEditorTarget } from "./components/project_view/state";

/** Owns one active investigation's stores, bounded pages and row mutations. */
export function createWorkspaceState() {
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
  }
  const totalRows = writable(0);
  /** Rows matching the applied QuerySpec. Sole source for the virtual row count. */
  const totalFilteredRows = writable(0);
  /** True while the first page of a newly applied result is loading. */
  const appliedResultLoading = writable(false);
  /** Annotated rows in the whole project. */
  const flaggedCount = writable(0);
  /** Annotated rows inside the current result set. */
  const filteredFlaggedCount = writable(0);
  const pages = createWorkspacePages(() => Math.floor(logicalRowAt(
    get(scrollTop), get(totalFilteredRows), get(viewportHeight),
  ) / PAGE_SIZE));
  const {
    rowsCache, positionToRowIndex, pendingPages, loadedPages, largePages, updateCachedRow,
    currentVisiblePage, touchPage, registerLoadedPage, resetPageCache,
  } = pages;

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
  const iocManagerOpen = writable(false);

  /**
   * Monotonic per-row request sequence shared by flag updates and memo saves.
   * Only the latest request for a row may roll its optimistic change back.
   */
  const annotationRequestSeq = new Map<number, number>();

  function nextAnnotationRequest(rowIndex: number): number {
    const next = (annotationRequestSeq.get(rowIndex) ?? 0) + 1;
    annotationRequestSeq.set(rowIndex, next);
    return next;
  }

  function isLatestAnnotationRequest(
    rowIndex: number,
    requestSeq: number
  ): boolean {
    return annotationRequestSeq.get(rowIndex) === requestSeq;
  }

  const viewportHeight = writable(0);
  const scrollTop = writable(0);
  const scrollLeft = writable(0);
  const tableWidth = writable(0);
  const bodyScrollEl = writable<HTMLDivElement | null>(null);
  const headerScrollEl = writable<HTMLDivElement | null>(null);

  const visibleColumns = derived(
    [projectDetail, hiddenColumns],
    ([$projectDetail, $hiddenColumns]) => {
      if (!$projectDetail) return [];
      return $projectDetail.columns.filter(
        (column) => !$hiddenColumns.has(column)
      );
    }
  );

  const iocDraft = writable<IocEntry[]>([]);

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
        updateCachedRow(rowIndex, (cached) =>
          applyAnnotationEntry(cached, updated.entry)
        );
        flaggedCount.set(updated.flaggedRecords);
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
    annotationRequestSeq.clear();
    expandedCell.set(null);
    memoEditor.set(null);
    iocDraft.set([]);
    projectDetail.set(null);
    currentProjectId.set(null);
    backend.set(null);
    bodyScrollEl.set(null);
    headerScrollEl.set(null);
  };

  return {
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
    iocManagerOpen,
    nextAnnotationRequest,
    isLatestAnnotationRequest,
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
    iocDraft,
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
