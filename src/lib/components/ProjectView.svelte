<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { get } from 'svelte/store';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import DataTable from './project_view/DataTable.svelte';
  import IocManagerDialog from './project_view/IocManagerDialog.svelte';
  import MemoEditorDialog from './project_view/MemoEditorDialog.svelte';
  import ExpandedCellDialog from './project_view/ExpandedCellDialog.svelte';
  import ExportDialog from './project_view/ExportDialog.svelte';
  import FilterControls from './project_view/FilterControls.svelte';
  import TimelinePanel from './project_view/TimelinePanel.svelte';
  import type { IocEntry, LoadProjectResponse, ProjectRow, ViewState } from '../types';
  import type { Backend } from '../backend';
  import { listenToJobEvents } from '../backend';
  import { isCurrentQueryJob, isJobLimitError } from '../jobTracking';
  import './project-view.css';
  import { conditionPayload, conditionTree } from '../stores/condition';
  import { cloneTree } from '../viewState';
  import type { SavedView } from '../viewState';
  import {
    savedViewToViewState,
    viewStateToSavedView,
    viewStateWarnings,
  } from '../viewStateMapping';
  import {
    goBack,
    goForward,
    historyRestore,
    loadProjectView,
    recordAppliedCondition,
    rememberOffset,
    resetHistory,
    setHistoryProject,
  } from '../stores/viewHistory';
  import {
    projectDetail as projectDetailStore,
    backend as backendStore,
    visibleColumns,
    hiddenColumns,
    columnLabels,
    search,
    searchColumns,
    flagFilter,
    bookmarkedOnly,
    sortKey,
    sortDirection,
    isExporting,
    isUpdatingColumns,
    rowsCache,
    pendingPages,
    loadedPages,
    largePages,
    totalRows,
    totalFilteredRows,
    appliedResultLoading,
    flaggedCount,
    filteredFlaggedCount,
    expandedCell,
    memoEditor,
    iocManagerOpen,
    viewportHeight,
    scrollTop,
    tableWidth,
    bodyScrollEl,
    headerScrollEl,
    iocDraft,
    normalizeIocFlag,
    normalizeFlag,
    normalizeRow,
    mapStoredFlag,
    FLAG_OPTIONS,
    FLAG_FILTER_OPTIONS,
    PRIORITY_FLAGS,
    ROW_HEIGHT,
    BUFFER,
    SELECT_COL_WIDTH,
    INDEX_COL_WIDTH,
    FLAG_COL_WIDTH,
    MEMO_COL_WIDTH,
    MIN_DATA_WIDTH,
    WIDTH_LIMIT_CHARS,
    CHAR_PIXEL,
    COLUMN_PADDING,
    MAX_DATA_WIDTH,
    STICKY_COLUMNS_WIDTH,
    PAGE_SIZE,
    PREFETCH_PAGES,
    currentProjectId,
    lastHiddenColumnsRef,
    viewGeneration,
    pendingPageKey,
    invalidateViewRequests,
    registerLoadedPage,
    resetPageCache,
    touchPage,
    syncSelectionScope,
    selectedRowIds,
    appliedFilters
  } from './project_view/state';
import type {
    FlagFilterValue,
    CachedRow,
    VirtualRow,
    AppliedFilters,
    ExportOptions,
    ExportSnapshot
  } from './project_view/state';
  import {
    clampRow,
    logicalRowAt,
    maxScrollTop,
    scrollTopForRow
  } from '../virtualScroll';

  export let projectDetail: LoadProjectResponse;
  export let backend: Backend;

  const dispatch = createEventDispatcher<{
    refresh: void;
    notify: { message: string; tone: 'success' | 'error' };
    summary: { flagged: number; iocApplied: number; hiddenColumns: string[] };
  }>();

  let columnsOpen = false;
  let columnPickerEl: HTMLDivElement | null = null;

  let flagMenuOpen = false;
  let flagPickerEl: HTMLDivElement | null = null;


  let iocError: string | null = null;
  let isSavingIocs = false;
  let iocImportInput: HTMLInputElement | null = null;

  let initialized = false;
  let filterTimeout: ReturnType<typeof setTimeout> | null = null;
  let pendingFilters: (AppliedFilters & { resetScroll: boolean }) | null = null;
  let filterRequestId = 0;
  let lastAppliedFilters = '';
  let lastSearchValue: string | null = null;
  let lastFlagFilter: FlagFilterValue | null = null;
  let lastBookmarkedOnly: boolean | null = null;
  let lastSearchColumnsSignature: string | null = null;
  let lastConditionJson = 'null';
  let applyingHistory = false;
  let suppressScrollRecord = false;
  let appliedRestoreNonce = 0;
  let pendingScrollRow: number | null = null;
  let gotoRowInput = '';
  let timelineOpen = false;
  let persistTimer: ReturnType<typeof setTimeout> | null = null;
  let applyingProjectId: string | null = null;
  // False when the saved view state is newer/unreadable; do not overwrite it.
  let viewStateWritable = true;
  let lastSummaryFlagged = -1;
  let lastSummaryHiddenKey = '';

  let columnWidths: Map<string, number> = new Map();
  let activeFilters: AppliedFilters = { search: '', flag: 'all', bookmarkedOnly: false, searchColumns: [], condition: null };
  let exportSnapshot: ExportSnapshot | null = null;
  let resizeObserver: ResizeObserver | null = null;
  let observedScrollEl: HTMLDivElement | null = null;

  let releaseHeaderSyncFrame: number | null = null;
  let releaseBodySyncFrame: number | null = null;
  let isSyncingHeaderScroll = false;
  let isSyncingBodyScroll = false;

  // Query jobs are tracked by the job-started/finished events so a new applied
  // filter can cancel the previous search at its next batch boundary.
  let activeQueryJobs = new Set<number>();
  let unlistenJobEvents: (() => void) | null = null;
  let destroyed = false;

  // Keep at most two page queries in flight, matching the engine job limit.
  // Prefetch can otherwise ask for three pages and the third is rejected.
  const PAGE_QUERY_LIMIT = 2;
  let pageQuerySlots = PAGE_QUERY_LIMIT;
  const pageQueryWaiters: Array<() => void> = [];

  const acquirePageSlot = async () => {
    if (pageQuerySlots > 0) {
      pageQuerySlots -= 1;
      return;
    }
    await new Promise<void>((resolve) => pageQueryWaiters.push(resolve));
  };

  const releasePageSlot = () => {
    const next = pageQueryWaiters.shift();
    if (next) {
      next();
    } else {
      pageQuerySlots += 1;
    }
  };

  const cancelActiveQueryJobs = () => {
    const ids = Array.from(activeQueryJobs);
    activeQueryJobs.clear();
    for (const id of ids) {
      void backend.cancelJob(id).catch((error) => {
        console.error('[jobs] failed to cancel query job', id, error);
      });
    }
  };

  projectDetailStore.set(projectDetail);
  backendStore.set(backend);

  const hasRow = (row: CachedRow | null): row is CachedRow => row !== null;

  const areStringsEqual = (left: string[], right: string[]) => {
    if (left.length !== right.length) {
      return false;
    }
    for (let i = 0; i < left.length; i += 1) {
      if (left[i] !== right[i]) {
        return false;
      }
    }
    return true;
  };

  const buildFilterSignature = (filters: AppliedFilters) =>
    `${filters.search}::${filters.flag}::${filters.bookmarkedOnly}::${filters.searchColumns.join('|')}::${$sortKey ?? ''}::${$sortDirection}::${JSON.stringify(filters.condition)}`;

  const resetPaginationState = () => {
    rowsCache.set(new Map());
    pendingPages.set(new Set());
    loadedPages.set(new Set());
    resetPageCache();
    totalRows.set(0);
    flaggedCount.set(0);
    filteredFlaggedCount.set(0);
  };

  const initializeColumnWidths = (savedWidths?: Record<string, number>) => {
    if (!projectDetail) {
      columnWidths = new Map();
      return;
    }
    const next = new Map<string, number>();
    for (const column of projectDetail.columns) {
      // Widths use the display label, while the column key stays the internal id.
      const saved = savedWidths?.[column];
      if (typeof saved === 'number' && Number.isFinite(saved) && saved > 0) {
        next.set(column, Math.min(Math.max(saved, MIN_DATA_WIDTH), MAX_DATA_WIDTH));
        continue;
      }
      const label = $columnLabels.get(column) ?? column;
      const headerChars = Math.min(label.length, WIDTH_LIMIT_CHARS);
      const dataChars = Math.min(projectDetail.column_max_chars[column] ?? headerChars, WIDTH_LIMIT_CHARS);
      const maxChars = Math.max(headerChars, dataChars);
      const estimated = Math.round(maxChars * CHAR_PIXEL + COLUMN_PADDING);
      const width = Math.min(Math.max(estimated, MIN_DATA_WIDTH), MAX_DATA_WIDTH);
      next.set(column, width);
    }
    columnWidths = next;
  };

  // View state and history store a result-relative logical row, never a raw
  // physical pixel offset.
  const rowOffset = () =>
    clampRow(
      logicalRowAt(get(scrollTop), get(totalFilteredRows), get(viewportHeight)),
      get(totalFilteredRows)
    );

  const buildViewState = (): ViewState => {
    const logicalRow = rowOffset();
    return {
      version: 1,
      search: get(search),
      searchColumns: [...get(searchColumns)],
      condition: cloneTree(get(conditionTree)) as unknown as Record<string, unknown>,
      flagFilter: get(flagFilter),
      bookmarkedOnly: get(bookmarkedOnly),
      sortKey: get(sortKey),
      sortDirection: get(sortDirection),
      hiddenColumns: Array.from(get(hiddenColumns)),
      columnWidths: Object.fromEntries(columnWidths),
      logicalRow,
    };
  };

  /**
   * Writes the current project's view state to the engine. The snapshot is
   * taken synchronously, so a project switch that resets the stores cannot
   * change what is being saved for the previous project.
   */
  const flushPersist = async (
    projectId: string | null,
    notifyOnError = true
  ): Promise<void> => {
    if (!projectId || !viewStateWritable) {
      return;
    }
    const state = buildViewState();
    try {
      await backend.saveViewState({ projectId, state });
    } catch (error) {
      console.error('[view] failed to save view state:', error);
      if (notifyOnError) {
        dispatch('notify', {
          message: 'Failed to save view settings. Evidence and annotations are unchanged.',
          tone: 'error',
        });
      }
    }
  };

  const schedulePersist = () => {
    if (persistTimer) {
      clearTimeout(persistTimer);
    }
    persistTimer = setTimeout(() => {
      persistTimer = null;
      void flushPersist(get(currentProjectId));
    }, 250);
  };

  const applyScrollOffset = (offset: number, totalOverride?: number) => {
    const total = totalOverride ?? get(totalFilteredRows);
    const row = clampRow(offset, total);
    const px = scrollTopForRow(row, total, get(viewportHeight));
    suppressScrollRecord = true;
    scrollTop.set(px);
    if ($bodyScrollEl) {
      $bodyScrollEl.scrollTop = px;
    }
    suppressScrollRecord = false;
  };

  const scrollToRow = (row: number) => {
    if (!initialized) {
      return;
    }
    const total = get(totalFilteredRows);
    if (total <= 0) {
      return;
    }
    const target = clampRow(row, total);
    pendingScrollRow = null;
    applyScrollOffset(target, total);
    rememberOffset(target);
    void requestPage(Math.floor(target / PAGE_SIZE));
  };

  const jumpToEnteredRow = () => {
    if (gotoRowInput.trim() === '') {
      return;
    }
    const parsed = Number(gotoRowInput);
    if (!Number.isFinite(parsed)) {
      return;
    }
    // The input is 1-based; the result rows are 0-based.
    scrollToRow(Math.floor(parsed) - 1);
  };

  const jumpToFirstRow = () => scrollToRow(0);

  const jumpToLastRow = () => scrollToRow(Math.max(0, get(totalFilteredRows) - 1));

  const applyProjectDetail = async (detail: LoadProjectResponse, _resetScroll: boolean) => {
    // Any response from the previous project or screen must be ignored from here on.
    invalidateViewRequests();
    const generation = get(viewGeneration);
    const projectId = detail.project.meta.id;
    const previousId = get(currentProjectId);
    if (persistTimer) {
      clearTimeout(persistTimer);
      persistTimer = null;
    }
    // Save the old project before its stores are replaced for the new one.
    if (previousId && previousId !== projectId) {
      await flushPersist(previousId);
    }
    if (generation !== get(viewGeneration)) {
      if (applyingProjectId === projectId) {
        applyingProjectId = null;
      }
      return;
    }

    // Requests from the previous project or screen must not keep the job
    // slots while this project loads.
    cancelActiveQueryJobs();
    initialized = false;
    applyingProjectId = projectId;
    viewStateWritable = true;
    currentProjectId.set(projectId);
    setHistoryProject(projectId);

    // Start from this project's defaults. Another project's global store values
    // must not be visible while the engine view state is being read.
    const emptyTree = {
      type: 'group' as const,
      id: 'root',
      kind: 'and' as const,
      collapsed: false,
      children: []
    };
    search.set('');
    searchColumns.set([]);
    flagFilter.set('all');
    bookmarkedOnly.set(false);
    appliedFilters.set({
      search: '',
      flag: 'all',
      bookmarkedOnly: false,
      searchColumns: [],
      condition: null,
    });
    sortKey.set(null);
    sortDirection.set('asc');
    conditionTree.set(emptyTree);
    hiddenColumns.set(new Set(detail.hidden_columns ?? []));
    initializeColumnWidths();

    let engineState: ViewState | null = null;
    let loadFailed = false;
    let viewStateStatus: "ok" | "missing" | "unreadable" | "newer" = "missing";
    try {
      const loaded = await backend.loadViewState({ projectId });
      engineState = loaded.state;
      viewStateStatus = loaded.status;
    } catch (error) {
      console.error('[view] failed to load view state:', error);
      loadFailed = true;
    }
    if (generation !== get(viewGeneration) || get(currentProjectId) !== projectId) {
      if (applyingProjectId === projectId) {
        applyingProjectId = null;
      }
      return;
    }

    let saved: SavedView | null = null;
    if (engineState) {
      saved = viewStateToSavedView(engineState, detail.columns);
      if (!saved) {
        // Do not replace an unreadable condition tree with an empty filter
        // without telling the user.
        dispatch('notify', {
          message: 'Saved view settings could not be restored. Showing the default view.',
          tone: 'error',
        });
      } else {
        const warnings = viewStateWarnings(engineState, detail.columns);
        if (warnings.length > 0) {
          dispatch('notify', {
            message: `Saved view settings used unknown values: ${warnings.join(', ')}. Showing defaults for those fields.`,
            tone: 'error',
          });
        }
      }
    } else if (!loadFailed && viewStateStatus === "missing") {
      // Legacy projects stored their view in localStorage. Migrate it once the
      // engine write succeeds; later loads read the engine file.
      const legacy = loadProjectView(projectId, detail.columns);
      if (legacy) {
        saved = legacy;
        void backend
          .saveViewState({ projectId, state: savedViewToViewState(legacy) })
          .catch((error) => {
            console.error('[view] failed to migrate legacy view state:', error);
            dispatch('notify', {
              message: 'Failed to migrate saved view settings. Evidence and annotations are unchanged.',
              tone: 'error',
            });
          });
      }
    }
    if (loadFailed || viewStateStatus === "unreadable" || viewStateStatus === "newer") {
      // Keep the original file untouched instead of replacing it with the
      // defaults shown on screen.
      viewStateWritable = false;
      dispatch('notify', {
        message:
          viewStateStatus === "newer"
            ? 'Saved view settings were written by a newer version. The file is kept; changes in this session will not overwrite it.'
            : 'Saved view settings could not be read. The file is kept; changes in this session will not overwrite it.',
        tone: 'error',
      });
    }

    const savedRow = saved?.logicalRow ?? saved?.offset ?? 0;
    conditionTree.set(saved ? cloneTree(saved.condition) : emptyTree);
    lastConditionJson = JSON.stringify(get(conditionPayload));
    search.set(saved?.search ?? '');
    searchColumns.set(saved ? [...saved.searchColumns] : []);
    flagFilter.set(saved?.flagFilter ?? 'all');
    bookmarkedOnly.set(saved?.bookmarkedOnly ?? false);
    hiddenColumns.set(new Set(saved ? saved.hiddenColumns : (detail.hidden_columns ?? [])));
    sortKey.set(saved?.sortKey ?? null);
    sortDirection.set(saved?.sortDirection ?? 'asc');
    iocDraft.set(detail.iocs.map((entry) => ({
      id: crypto.randomUUID(),
      flag: normalizeIocFlag(entry.flag),
      tag: entry.tag,
      query: entry.query
    })));
    initializeColumnWidths(saved?.columnWidths);
    const filters: AppliedFilters = {
      search: $search.trim(),
      flag: $flagFilter,
      bookmarkedOnly: $bookmarkedOnly,
      searchColumns: [...$searchColumns],
      condition: get(conditionPayload),
    };
    lastHiddenColumnsRef.set([...(detail.hidden_columns ?? [])]);
    if (filterTimeout) {
      clearTimeout(filterTimeout);
      filterTimeout = null;
    }
    pendingFilters = null;
    resetPaginationState();
    activeFilters = {
      search: filters.search,
      flag: filters.flag,
      bookmarkedOnly: filters.bookmarkedOnly,
      searchColumns: [...filters.searchColumns],
      condition: filters.condition,
    };
    appliedFilters.set({
      search: activeFilters.search,
      flag: activeFilters.flag,
      bookmarkedOnly: activeFilters.bookmarkedOnly,
      searchColumns: [...activeFilters.searchColumns],
      condition: activeFilters.condition,
    });
    lastAppliedFilters = buildFilterSignature(filters);
    syncSelectionScope(projectId, lastAppliedFilters);
    lastSearchValue = filters.search;
    lastFlagFilter = filters.flag;
    lastBookmarkedOnly = filters.bookmarkedOnly;
    lastSearchColumnsSignature = filters.searchColumns.join('|');
    filterRequestId += 1;
    lastSummaryFlagged = -1;
    lastSummaryHiddenKey = '';
    // Dialog state belongs to the previous project and must not be applied here.
    expandedCell.set(null);
    memoEditor.set(null);
    iocManagerOpen.set(false);
    iocError = null;
    isSavingIocs = false;
    resetHistory(get(conditionTree), savedRow);

    // Don't use seededRows from initial_rows as they may be stale
    // Always fetch fresh data from backend to ensure flags and memos are current
    totalRows.set(detail.project.meta.total_records);
    totalFilteredRows.set(0);
    appliedResultLoading.set(true);
    flaggedCount.set(detail.project.meta.flagged_records);
    // The saved logical row is authoritative; the pixel offset derived from the
    // project total is only an estimate until the first page returns the
    // filtered count.
    pendingScrollRow = savedRow;
    applyScrollOffset(savedRow, detail.project.meta.total_records);
    if ($bodyScrollEl) {
      $bodyScrollEl.scrollLeft = 0;
    }
    if ($headerScrollEl) {
      $headerScrollEl.scrollLeft = 0;
    }
    initialized = true;
    applyingProjectId = null;
    // Always request first page to ensure flags and memos are reflected after project reload
    // Wait for the first page to load before marking as ready to render
    requestPage(0, true).catch((error) => {
      console.error('[debug] applyProjectDetail first page failed:', error);
    });
  };

  $: if (projectDetail) {
    const projectId = projectDetail.project.meta.id;
    const projectChanged = projectId !== $currentProjectId;
    const nextHiddenColumns = projectDetail.hidden_columns ?? [];
    const hiddenChanged =
      $lastHiddenColumnsRef === null ||
      !areStringsEqual(nextHiddenColumns, $lastHiddenColumnsRef);
    if (!initialized || projectChanged) {
      // A pending async load owns the stores until it finishes; a second call
      // would reset the values that load is about to apply.
      if (applyingProjectId !== projectId) {
        applyingProjectId = projectId;
        void applyProjectDetail(projectDetail, true);
      }
    } else if (hiddenChanged) {
      // Visibility is presentation only: the search scope (and therefore the
      // result set) must not change when a column is hidden.
      hiddenColumns.set(new Set(nextHiddenColumns));
      initializeColumnWidths();
    }
    lastHiddenColumnsRef.set([...nextHiddenColumns]);
  }

  $: if ($historyRestore && initialized && $historyRestore.nonce !== appliedRestoreNonce) {
    appliedRestoreNonce = $historyRestore.nonce;
    // A restore recorded for another project must not rewrite this project's view.
    if ($historyRestore.projectId === $currentProjectId) {
      applyingHistory = true;
      conditionTree.set(cloneTree($historyRestore.tree));
      applyScrollOffset($historyRestore.offset);
    }
  }

  $: if (initialized) {
    const searchValue = $search.trim();
    const flagValue = $flagFilter;
    const bookmarkedValue = $bookmarkedOnly;
    const scopedColumns = [...$searchColumns];
    const signature = scopedColumns.join('|');
    const conditionJson = JSON.stringify($conditionPayload);
    const searchChanged = lastSearchValue === null || searchValue !== lastSearchValue;
    const flagChanged = lastFlagFilter === null || flagValue !== lastFlagFilter;
    const bookmarkedChanged =
      lastBookmarkedOnly === null || bookmarkedValue !== lastBookmarkedOnly;
    const searchColumnsChanged =
      lastSearchColumnsSignature === null || signature !== lastSearchColumnsSignature;
    const conditionChanged = conditionJson !== lastConditionJson;
    let resetScroll = flagChanged || bookmarkedChanged || conditionChanged;
    if (conditionChanged && applyingHistory) {
      resetScroll = false;
      applyingHistory = false;
    } else if (conditionChanged) {
      recordAppliedCondition(rowOffset(), get(conditionTree), 0);
    }

    if (searchChanged || flagChanged || bookmarkedChanged || searchColumnsChanged || conditionChanged) {
      scheduleFilterRefresh(searchValue, flagValue, scopedColumns, resetScroll);
      lastSearchValue = searchValue;
      lastFlagFilter = flagValue;
      lastBookmarkedOnly = bookmarkedValue;
      lastSearchColumnsSignature = signature;
      lastConditionJson = conditionJson;
    }
  }

  // `$scrollTop` is a dependency so scrolling alone schedules the debounced
  // view-state save; `rowOffset()` reads the store via `get()` and would
  // otherwise stay invisible to the reactive dependency graph.
  $: persistedView = initialized && projectDetail && $scrollTop >= 0
    ? {
        search: $search,
        searchColumns: [...$searchColumns],
        flagFilter: $flagFilter,
        bookmarkedOnly: $bookmarkedOnly,
        hidden: Array.from($hiddenColumns),
        sortKey: $sortKey,
        sortDirection: $sortDirection,
        columnWidths,
        offset: rowOffset(),
        logicalRow: rowOffset(),
        tree: $conditionTree,
      }
    : null;
  $: if (persistedView) {
    schedulePersist();
  }

  $: viewportRows = Math.ceil(($viewportHeight || ROW_HEIGHT) / ROW_HEIGHT);
  $: visibleCount = viewportRows + BUFFER * 2;
  // Keep in sync with DataTable: the applied QuerySpec result is the only
  // source for the active row count, including bookmark-only filtering.
  $: effectiveTotalRows = $totalFilteredRows;
  $: maxStart = Math.max(0, effectiveTotalRows - viewportRows);
  $: anchorRow = logicalRowAt(
    $scrollTop,
    effectiveTotalRows,
    $viewportHeight
  );
  $: startIndex = Math.min(
    maxStart,
    Math.max(0, anchorRow - BUFFER)
  );
  $: endIndex = Math.min(effectiveTotalRows, startIndex + visibleCount);
  $: ensureRangeLoaded(startIndex, endIndex);
  $: if ($bodyScrollEl && maxScrollTop(effectiveTotalRows, $viewportHeight) >= $scrollTop && Math.abs($bodyScrollEl.scrollTop - $scrollTop) > 1) {
    suppressScrollRecord = true;
    $bodyScrollEl.scrollTop = $scrollTop;
    suppressScrollRecord = false;
  }
  $: loadedRowCount = $rowsCache.size;

  const ensureRangeLoaded = (start: number, end: number) => {
    if (!initialized || !projectDetail || end <= start) {
      return;
    }
    const firstPage = Math.max(0, Math.floor(start / PAGE_SIZE) - PREFETCH_PAGES);
    const lastPosition = Math.max(start, end - 1);
    const lastPage = Math.max(firstPage, Math.floor(lastPosition / PAGE_SIZE) + PREFETCH_PAGES);
    const maxPageIndex = Math.max(0, Math.floor(Math.max(effectiveTotalRows - 1, 0) / PAGE_SIZE));
    const clampedLastPage = Math.min(lastPage, maxPageIndex);
    const generation = get(viewGeneration);
    for (let page = firstPage; page <= clampedLastPage; page += 1) {
      // Keep the displayed range and its prefetch neighbours recent even when
      // they are already cached, so eviction prefers pages scrolled away from.
      touchPage(page);
      if ($largePages.has(page)) {
        continue;
      }
      const pendingKey = pendingPageKey(generation, filterRequestId, page);
      if (!$loadedPages.has(page) && !$pendingPages.has(pendingKey)) {
        void requestPage(page);
      }
    }
  };

  const requestPage = async (
    pageIndex: number,
    force = false
  ): Promise<void> => {
    if (!projectDetail) return;
    const normalizedPage = Math.max(0, pageIndex);
    const requestProjectId = projectDetail.project.meta.id;
    const requestGeneration = get(viewGeneration);
    const requestId = filterRequestId;
    const pendingKey = pendingPageKey(
      requestGeneration,
      requestId,
      normalizedPage
    );
    const isCurrentRequest = () =>
      requestId === filterRequestId &&
      requestGeneration === get(viewGeneration) &&
      requestProjectId === get(currentProjectId);

    if (!force && $loadedPages.has(normalizedPage)) {
      return;
    }
    if (!force && $pendingPages.has(pendingKey)) {
      return;
    }
    const offset = normalizedPage * PAGE_SIZE;
    const limit = PAGE_SIZE;
    const filters = activeFilters;
    pendingPages.update((pending) => {
      const next = new Set(pending);
      next.add(pendingKey);
      return next;
    });

    const payload = {
      projectId: requestProjectId,
      flagFilter: filters.flag,
      bookmarkedOnly: filters.bookmarkedOnly,
      search: filters.search.trim(),
      searchColumns: filters.searchColumns,
      offset,
      limit,
      sortKey: $sortKey ?? null,
      sortDirection: $sortDirection ?? null,
      condition: filters.condition,
    };

    // The engine permits two concurrent heavy jobs. Prefetching three pages
    // used to make the third fail and leave the page permanently unloaded.
    // Queue extra page requests locally instead.
    await acquirePageSlot();
    try {
      if (!isCurrentRequest()) {
        return;
      }
      for (let attempt = 0; attempt < 3; attempt += 1) {
        try {
          const response = await backend.queryProjectRows(payload);
          if (!isCurrentRequest()) {
            return;
          }
          totalRows.set(response.total_rows);
          totalFilteredRows.set(response.total_filtered_rows);
          // The saved logical row was recorded against the result set, so apply
          // it once the real filtered count is known (it may have shrunk).
          if (pendingScrollRow !== null) {
            const row = clampRow(pendingScrollRow, response.total_filtered_rows);
            pendingScrollRow = null;
            applyScrollOffset(row, response.total_filtered_rows);
            rememberOffset(row);
          }
          // `flaggedCount` always means the whole project, never this page's slice.
          flaggedCount.set(response.annotated_total);
          filteredFlaggedCount.set(response.annotated_filtered);
          appliedResultLoading.set(false);
          emitSummary();
          const normalizedRows = response.rows.map((row) => normalizeRow(row));
          // Adds the page and evicts the least recently used pages in one place
          // so rows, positions and loadedPages stay consistent.
          registerLoadedPage(
            Math.floor(response.offset / PAGE_SIZE),
            response.offset,
            normalizedRows
          );
          return;
        } catch (error) {
          // Import/export may hold the engine slots even when this page slot is
          // free. Retry a bounded number of times instead of dropping the page.
          if (attempt < 2 && isJobLimitError(error)) {
            await new Promise((resolve) =>
              setTimeout(resolve, 250 * (attempt + 1))
            );
            if (!isCurrentRequest()) {
              return;
            }
            continue;
          }
          throw error;
        }
      }
    } catch (error) {
      console.error(error);
    } finally {
      releasePageSlot();
      // A stale first-page request must not clear the loading state of the
      // newer result.
      if (normalizedPage === 0 && isCurrentRequest()) {
        appliedResultLoading.set(false);
      }
      // Remove only this request's pending key; a stale request must not
      // clear the pending state of a newer generation.
      pendingPages.update((pending) => {
        if (!pending.has(pendingKey)) {
          return pending;
        }
        const next = new Set(pending);
        next.delete(pendingKey);
        return next;
      });
    }
  };

  const applyFilters = (
    filters: AppliedFilters,
    resetScroll: boolean,
    force: boolean
  ): Promise<void> => {
    if (!projectDetail) {
      return Promise.resolve();
    }
    const normalized: AppliedFilters = {
      search: filters.search.trim(),
      flag: filters.flag,
      bookmarkedOnly: filters.bookmarkedOnly,
      searchColumns: [...filters.searchColumns],
      condition: filters.condition,
    };
    const signature = buildFilterSignature(normalized);
    if (!force && signature === lastAppliedFilters) {
      return Promise.resolve();
    }
    // A new applied query supersedes the previous search. Cancel it before
    // queueing pages for the new result.
    cancelActiveQueryJobs();
    activeFilters = {
      search: normalized.search,
      flag: normalized.flag,
      bookmarkedOnly: normalized.bookmarkedOnly,
      searchColumns: [...normalized.searchColumns],
      condition: normalized.condition,
    };
    appliedFilters.set({
      search: activeFilters.search,
      flag: activeFilters.flag,
      bookmarkedOnly: activeFilters.bookmarkedOnly,
      searchColumns: [...activeFilters.searchColumns],
      condition: activeFilters.condition,
    });
    appliedResultLoading.set(true);
    lastAppliedFilters = signature;
    syncSelectionScope(projectDetail.project.meta.id, signature);
    filterRequestId += 1;
    pendingScrollRow = null;
    resetPaginationState();
    if (resetScroll) {
      scrollTop.set(0);
      if ($bodyScrollEl) {
        $bodyScrollEl.scrollTop = 0;
        $bodyScrollEl.scrollLeft = 0;
      }
      if ($headerScrollEl) {
        $headerScrollEl.scrollLeft = 0;
      }
    }
    lastSummaryFlagged = -1;
    lastSummaryHiddenKey = '';
    // Page selection uses the same logical mapping as the table and the
    // persisted position, not a raw pixel offset.
    const targetRow = resetScroll
      ? 0
      : logicalRowAt($scrollTop, effectiveTotalRows, $viewportHeight);
    return requestPage(Math.floor(targetRow / PAGE_SIZE), true);
  };

  const resolveColumnWidth = (column: string) => {
    const width = columnWidths.get(column) ?? MIN_DATA_WIDTH;
    if (width < MIN_DATA_WIDTH) return MIN_DATA_WIDTH;
    if (width > MAX_DATA_WIDTH) return MAX_DATA_WIDTH;
    return width;
  };

  const expandColumnWidths = (baseWidths: number[], _availableWidth: number) => {
    return baseWidths.map((width) => {
      if (width < MIN_DATA_WIDTH) return MIN_DATA_WIDTH;
      if (width > MAX_DATA_WIDTH) return MAX_DATA_WIDTH;
      return Math.round(width);
    });
  };

  $: baseDataWidths = $visibleColumns.map((column) => resolveColumnWidth(column));
  $: availableDataWidth = Math.max(0, $tableWidth - STICKY_COLUMNS_WIDTH);
  $: distributedDataWidths = expandColumnWidths(baseDataWidths, availableDataWidth);
  $: totalDataWidth = distributedDataWidths.reduce((sum, width) => sum + width, 0);
  $: totalTableWidth = STICKY_COLUMNS_WIDTH + totalDataWidth;
  $: effectiveTableWidth = Math.max(totalTableWidth, $tableWidth);
  $: gridTemplate = [
    `${SELECT_COL_WIDTH}px`,
    `${INDEX_COL_WIDTH}px`,
    `${FLAG_COL_WIDTH}px`,
    `${MEMO_COL_WIDTH}px`,
    ...distributedDataWidths.map((width) => `${width}px`)
  ].join(' ');
  $: if (resizeObserver) {
    if ($bodyScrollEl && $bodyScrollEl !== observedScrollEl) {
      if (observedScrollEl) {
        resizeObserver.unobserve(observedScrollEl);
      }
      resizeObserver.observe($bodyScrollEl);
      observedScrollEl = $bodyScrollEl;
      tableWidth.set($bodyScrollEl.clientWidth);
    } else if (!$bodyScrollEl && observedScrollEl) {
      resizeObserver.unobserve(observedScrollEl);
      observedScrollEl = null;
      tableWidth.set(0);
    }
  }

  $: firstDataColumn = $visibleColumns.length > 0 ? $visibleColumns[0] : null;
  $: stickyFlagOffset = SELECT_COL_WIDTH + INDEX_COL_WIDTH;
  $: stickyMemoOffset = stickyFlagOffset + FLAG_COL_WIDTH;
  $: stickyDataOffset = stickyMemoOffset + MEMO_COL_WIDTH;
  $: stickyVariables = `--sticky-index:${SELECT_COL_WIDTH}px; --sticky-flag:${stickyFlagOffset}px; --sticky-memo:${stickyMemoOffset}px; --sticky-data:${stickyDataOffset}px;`;

  const padNumber = (value: number) => value.toString().padStart(2, '0');
  const formatTimestampForFilename = (date: Date) =>
    `${date.getFullYear()}${padNumber(date.getMonth() + 1)}${padNumber(date.getDate())}-${padNumber(date.getHours())}${padNumber(date.getMinutes())}${padNumber(date.getSeconds())}`;

  const getFlagFilterDetails = (value: FlagFilterValue) => {
    return (
      FLAG_FILTER_OPTIONS.find((option) => option.value === value) ?? FLAG_FILTER_OPTIONS[0]
    );
  };

  const toggleFlagMenu = () => {
    flagMenuOpen = !flagMenuOpen;
    if (flagMenuOpen) {
      columnsOpen = false;
    }
  };

  const selectFlagFilter = (value: FlagFilterValue) => {
    flagFilter.set(value);
    flagMenuOpen = false;
  };


  const closeCell = () => {
    expandedCell.set(null);
  };



  const handleScroll = (event: Event) => {
    const target = event.currentTarget as HTMLDivElement;
    if (suppressScrollRecord) {
      return;
    }
    scrollTop.set(target.scrollTop);
    rememberOffset(
      logicalRowAt(target.scrollTop, get(totalFilteredRows), get(viewportHeight))
    );
    if (isSyncingBodyScroll) {
      isSyncingBodyScroll = false;
      return;
    }
    if ($headerScrollEl && $headerScrollEl.scrollLeft !== target.scrollLeft) {
      if (releaseHeaderSyncFrame !== null) {
        cancelAnimationFrame(releaseHeaderSyncFrame);
      }
      isSyncingHeaderScroll = true;
      $headerScrollEl.scrollLeft = target.scrollLeft;
      releaseHeaderSyncFrame = requestAnimationFrame(() => {
        isSyncingHeaderScroll = false;
        releaseHeaderSyncFrame = null;
      });
    }
  };

  const handleHeaderScroll = () => {
    if (!$headerScrollEl || !$bodyScrollEl) return;
    if (isSyncingHeaderScroll) {
      isSyncingHeaderScroll = false;
      return;
    }
    const nextLeft = $headerScrollEl.scrollLeft;
    if ($bodyScrollEl.scrollLeft !== nextLeft) {
      if (releaseBodySyncFrame !== null) {
        cancelAnimationFrame(releaseBodySyncFrame);
      }
      isSyncingBodyScroll = true;
      $bodyScrollEl.scrollLeft = nextLeft;
      releaseBodySyncFrame = requestAnimationFrame(() => {
        isSyncingBodyScroll = false;
        releaseBodySyncFrame = null;
      });
    }
  };

  const forwardWheel = (event: WheelEvent) => {
    if (!$bodyScrollEl) return;
    if (event.currentTarget === $bodyScrollEl) {
      return;
    }
    let handled = false;
    if (event.deltaY !== 0) {
      const previousTop = $bodyScrollEl.scrollTop;
      $bodyScrollEl.scrollTop += event.deltaY;
      handled = handled || $bodyScrollEl.scrollTop !== previousTop;
    }
    if (event.deltaX !== 0) {
      const previousLeft = $bodyScrollEl.scrollLeft;
      $bodyScrollEl.scrollLeft += event.deltaX;
      handled = handled || $bodyScrollEl.scrollLeft !== previousLeft;
    }
    if (handled) {
      event.preventDefault();
    }
  };

  const scheduleFilterRefresh = (
    searchValue: string,
    flagValue: FlagFilterValue,
    scopedColumns: string[],
    resetScroll: boolean
  ) => {
    pendingFilters = {
      search: searchValue,
      flag: flagValue,
      bookmarkedOnly: $bookmarkedOnly,
      searchColumns: [...scopedColumns],
      condition: get(conditionPayload),
      resetScroll,
    };
    if (filterTimeout) {
      clearTimeout(filterTimeout);
    }
    filterTimeout = setTimeout(() => {
      const scheduled = pendingFilters;
      pendingFilters = null;
      filterTimeout = null;
      if (!scheduled) {
        return;
      }
      void applyFilters(
        {
          search: scheduled.search,
          flag: scheduled.flag,
          bookmarkedOnly: scheduled.bookmarkedOnly,
          searchColumns: scheduled.searchColumns,
          condition: scheduled.condition,
        },
        scheduled.resetScroll,
        false
      );
    }, 160);
  };

  const forceRefreshFilteredRows = (resetScroll: boolean) => {
    if (!projectDetail) return Promise.resolve();
    if (filterTimeout) {
      clearTimeout(filterTimeout);
      filterTimeout = null;
      pendingFilters = null;
    }
    return applyFilters(
      {
        search: $search.trim(),
        flag: $flagFilter,
        bookmarkedOnly: $bookmarkedOnly,
        searchColumns: [...$searchColumns],
        condition: get(conditionPayload),
      },
      resetScroll,
      true
    );
  };

  const openIocManager = () => {
    if (!projectDetail) {
      return;
    }
    iocDraft.set(projectDetail.iocs.map((entry) => ({
      id: crypto.randomUUID(),
      flag: normalizeIocFlag(entry.flag),
      tag: entry.tag,
      query: entry.query
    })));
    iocError = null;
    isSavingIocs = false;
    iocManagerOpen.set(true);
  };

  const closeIocManager = () => {
    iocManagerOpen.set(false);
  };

  /**
   * Captures the applied QuerySpec and selection once, when the dialog opens.
   * Debounced search input or later edits must not change the exported rows.
   */
  const openExportDialog = () => {
    if (!projectDetail) {
      return;
    }
    // The displayed result counts lag the applied QuerySpec until the first
    // page of a new result returns. Do not open a dialog whose counts and
    // export target disagree.
    const firstPageKey = pendingPageKey(
      get(viewGeneration),
      filterRequestId,
      0
    );
    if ($pendingPages.has(firstPageKey)) {
      dispatch('notify', {
        message: 'Wait for the current query to finish before exporting.',
        tone: 'error'
      });
      return;
    }
    exportSnapshot = {
      search: activeFilters.search,
      searchColumns: [...activeFilters.searchColumns],
      condition: activeFilters.condition,
      flagFilter: activeFilters.flag,
      bookmarkedOnly: activeFilters.bookmarkedOnly,
      sortKey: $sortKey,
      sortDirection: $sortDirection,
      rowIndices: Array.from($selectedRowIds),
      // `$totalRows` is reset to 0 while a result loads; the project metadata
      // is the stable all-rows count.
      totalRows: projectDetail.project.meta.total_records,
      totalFilteredRows: $totalFilteredRows,
      selectedCount: $selectedRowIds.size,
      visibleColumns: [...$visibleColumns]
    };
  };

  const runExport = async (options: ExportOptions) => {
    const snapshot = exportSnapshot;
    exportSnapshot = null;
    if (!snapshot || !projectDetail) {
      return;
    }
    isExporting.set(true);
    try {
      const baseName = projectDetail.project.meta.name || 'trivium-export.csv';
      const stem = baseName.replace(/\.[^.]+$/, '');
      const timestamp = formatTimestampForFilename(new Date());
      const suggested = `${timestamp}_trivium_${stem}.csv`;
      const selected = await save({
        filters: [{ name: 'CSV with flags', extensions: ['csv'] }],
        defaultPath: suggested
      });
      if (!selected) {
        return;
      }
      const result = await backend.exportProject({
        projectId: projectDetail.project.meta.id,
        destination: selected,
        scope: options.scope,
        search: snapshot.search,
        searchColumns: snapshot.searchColumns,
        condition: snapshot.condition,
        flagFilter: snapshot.flagFilter,
        bookmarkedOnly: snapshot.bookmarkedOnly,
        sortKey: snapshot.sortKey,
        sortDirection: snapshot.sortDirection,
        ...(options.scope === 'selected' ? { rowIndices: snapshot.rowIndices } : {}),
        ...(options.useVisibleColumns ? { columns: snapshot.visibleColumns } : {}),
        includeAnnotations: options.includeAnnotations
      });
      const warnings = result.warnings.length ? ` ${result.warnings.join(' ')}` : '';
      dispatch('notify', {
        message: `Exported ${result.rows} rows to CSV.${warnings}`,
        tone: 'success'
      });
    } catch (error) {
      console.error(error);
      // Tauri returns the engine error as a plain string; keep the actionable
      // message (for example, choosing another destination).
      const message =
        typeof error === 'string'
          ? error
          : error instanceof Error
            ? error.message
            : 'Failed to export CSV.';
      dispatch('notify', { message, tone: 'error' });
    } finally {
      isExporting.set(false);
    }
  };

  onMount(() => {
    const handleClickOutside = (event: MouseEvent) => {
      const target = event.target as HTMLElement;
      if (columnsOpen && columnPickerEl && !(columnPickerEl as HTMLElement).contains(target)) {
        columnsOpen = false;
      }
      if (flagMenuOpen && flagPickerEl && !(flagPickerEl as HTMLElement).contains(target)) {
        flagMenuOpen = false;
      }
    };
    const handleKeydown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      const typing = !!target && (
        target.tagName === 'INPUT' ||
        target.tagName === 'TEXTAREA' ||
        target.tagName === 'SELECT' ||
        target.isContentEditable
      );
      if (!typing && event.altKey && event.key === 'ArrowLeft') {
        event.preventDefault();
        goBack();
        return;
      }
      if (!typing && event.altKey && event.key === 'ArrowRight') {
        event.preventDefault();
        goForward();
        return;
      }
      if (event.key === 'Escape') {
        columnsOpen = false;
        flagMenuOpen = false;
        if (exportSnapshot) {
          exportSnapshot = null;
        } else if ($iocManagerOpen) {
          closeIocManager();
        } else if ($memoEditor) {
          memoEditor.set(null);
        } else {
          closeCell();
        }
      }
    };
    resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        tableWidth.set(entry.contentRect.width);
      }
    });
    if ($bodyScrollEl) {
      tableWidth.set($bodyScrollEl.clientWidth);
    }
    const handleSortChanged = (event: CustomEvent) => {
      const filters: AppliedFilters = {
        search: activeFilters.search,
        flag: activeFilters.flag,
        bookmarkedOnly: activeFilters.bookmarkedOnly,
        searchColumns: [...activeFilters.searchColumns],
        condition: activeFilters.condition,
      };
      void applyFilters(filters, false, true);
    };

    document.addEventListener('mousedown', handleClickOutside);
    window.addEventListener('keydown', handleKeydown);
    document.addEventListener('sortChanged', handleSortChanged as EventListener);

    void listenToJobEvents({
      onStarted: (job) => {
        if (isCurrentQueryJob(job, get(currentProjectId))) {
          activeQueryJobs.add(job.id);
        }
      },
      onFinished: (job) => {
        activeQueryJobs.delete(job.id);
      },
    })
      .then((unlisten) => {
        if (destroyed) {
          unlisten();
        } else {
          unlistenJobEvents = unlisten;
        }
      })
      .catch((error) => {
        console.error('[jobs] failed to subscribe to job events', error);
      });

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      window.removeEventListener('keydown', handleKeydown);
      document.removeEventListener('sortChanged', handleSortChanged as EventListener);
      if (resizeObserver && observedScrollEl) {
        resizeObserver.unobserve(observedScrollEl);
      }
      resizeObserver?.disconnect();
    };
  });

  onDestroy(() => {
    destroyed = true;
    unlistenJobEvents?.();
    unlistenJobEvents = null;
    cancelActiveQueryJobs();
    // Invalidate in-flight page requests from this screen instance.
    invalidateViewRequests();
    if (persistTimer) {
      clearTimeout(persistTimer);
      persistTimer = null;
    }
    void flushPersist(get(currentProjectId), false);
    if (filterTimeout) {
      clearTimeout(filterTimeout);
      filterTimeout = null;
    }
    if (releaseHeaderSyncFrame !== null) {
      cancelAnimationFrame(releaseHeaderSyncFrame);
      releaseHeaderSyncFrame = null;
    }
    if (releaseBodySyncFrame !== null) {
      cancelAnimationFrame(releaseBodySyncFrame);
      releaseBodySyncFrame = null;
    }
  });

  const emitSummary = () => {
    const hidden = Array.from($hiddenColumns);
    const hiddenKey = hidden.join('|');
    if (lastSummaryFlagged === $flaggedCount && lastSummaryHiddenKey === hiddenKey) {
      return;
    }
    lastSummaryFlagged = $flaggedCount;
    lastSummaryHiddenKey = hiddenKey;
    dispatch('summary', { 
      flagged: $flaggedCount, 
      iocApplied: projectDetail?.project.meta.ioc_applied_records ?? 0,
      hiddenColumns: hidden 
    });
  };
</script>

<section class="project-view">
  <FilterControls on:iocManagerOpen={openIocManager} on:export={openExportDialog} on:refresh={() => dispatch('refresh')} />

  <div class="row-jump" role="group" aria-label="Go to result row">
    <label class="row-jump-field">
      <span>Row</span>
      <input
        type="number"
        min="1"
        max={Math.max(1, $totalFilteredRows)}
        step="1"
        placeholder={`1–${$totalFilteredRows}`}
        bind:value={gotoRowInput}
        disabled={$totalFilteredRows === 0}
        on:keydown={(event) => {
          if (event.key === 'Enter') {
            event.preventDefault();
            jumpToEnteredRow();
          }
        }}
      />
    </label>
    <button
      type="button"
      class="ghost"
      on:click={jumpToEnteredRow}
      disabled={$totalFilteredRows === 0}
    >
      Go
    </button>
    <button
      type="button"
      class="ghost"
      on:click={jumpToFirstRow}
      disabled={$totalFilteredRows === 0}
    >
      First
    </button>
    <button
      type="button"
      class="ghost"
      on:click={jumpToLastRow}
      disabled={$totalFilteredRows === 0}
    >
      Last
    </button>
    <span class="row-jump-range">of {$totalFilteredRows}</span>
  </div>

  <details class="filter-advanced" bind:open={timelineOpen}>
    <summary>Timeline</summary>
    <TimelinePanel open={timelineOpen} />
  </details>

  <ExpandedCellDialog on:notify={(e) => dispatch('notify', e.detail)} />

  <MemoEditorDialog on:notify={(e) => dispatch('notify', e.detail)} />

  <IocManagerDialog on:notify={(e) => dispatch('notify', e.detail)} on:refresh={() => dispatch('refresh')} />

  {#if exportSnapshot}
    <ExportDialog
      snapshot={exportSnapshot}
      on:close={() => (exportSnapshot = null)}
      on:export={(event) => void runExport(event.detail)}
    />
  {/if}

  <DataTable {columnWidths} on:notify={(e) => dispatch('notify', e.detail)} />
</section>
