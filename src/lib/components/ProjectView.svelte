<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount, tick } from 'svelte';
  import { get } from 'svelte/store';
  import { save } from '@tauri-apps/plugin-dialog';
  import DataTable from './project_view/DataTable.svelte';
  import IocManagerDialog from './project_view/IocManagerDialog.svelte';
  import MemoEditorDialog from './project_view/MemoEditorDialog.svelte';
  import ExpandedCellDialog from './project_view/ExpandedCellDialog.svelte';
  import ExportDialog from './project_view/ExportDialog.svelte';
  import FilterControls from './project_view/FilterControls.svelte';
  import HistoryButtons from './project_view/HistoryButtons.svelte';
  import FilterStatus from './project_view/FilterStatus.svelte';
  import { clearFilterInputs } from '../appliedFilterSummary';
  import { parseFlagFilter } from '../flagFilters';
  import type { LoadProjectResponse } from '../types';
  import type { Backend } from '../backend';
  import { errorMessage } from '../errors';
  import { buildExportArgs, exportFileName } from '../exportFlow';
  import './project-view.css';

  import { cloneTree } from '../viewState';
  import type { WorkspaceSession } from '../workspaceSession';
  import {
    normalizeIocFlag, BUFFER, MIN_DATA_WIDTH,
    WIDTH_LIMIT_CHARS, CHAR_PIXEL, COLUMN_PADDING, MAX_DATA_WIDTH, PAGE_SIZE,
  } from './project_view/state';
  import type {
    FlagFilterValue,
    AppliedFilters,
    ExportOptions,
    ExportSnapshot
  } from './project_view/state';
  import {
    clampRow,
    maxScrollTop,
    virtualWindow
  } from '../virtualScroll';
  import { createWorkspaceController } from "../workspaceController";
  import { provideWorkspace } from "../workspaceContext";
  import { createWorkspaceQuery } from "../workspaceQuery";
  import { createWorkspaceNavigation } from "../workspaceNavigation";
  import { createWorkspaceView } from "../workspaceView";
  import { createWorkspaceAnnotationObserver } from "../workspaceAnnotations";

  export let projectDetail: LoadProjectResponse;
  export let backend: Backend;
  export let session: WorkspaceSession | null = null;
  export let onSessionSnapshot: (session: WorkspaceSession) => void = () => {};

  const workspace = createWorkspaceController();
  provideWorkspace(workspace);
  const {
    goBack, goForward, historyRestore, recordAppliedCondition, rememberOffset, resetHistory,
    restoreHistory, setHistoryProject, projectDetail: projectDetailStore, backend: backendStore,
    visibleColumns, hiddenColumns, columnLabels, search, searchColumns, flagFilter,
    bookmarkedOnly, sortKey, sortDirection, isExporting,
    totalRows, totalFilteredRows, appliedResultLoading, flaggedCount,
    expandedCell, memoEditor, iocManagerOpen, viewportHeight, scrollTop, scrollLeft,
    bodyScrollEl, headerScrollEl, iocDraft, currentProjectId, lastHiddenColumnsRef,
    viewGeneration, invalidateViewRequests, selectedRowIds, appliedFilters, sortRevision,
    conditionPayload, conditionTree,
  } = workspace;

  const dispatch = createEventDispatcher<{
    refresh: void;
    notify: { message: string; tone: 'success' | 'error' };
    summary: { projectId: string; flagged: number; iocApplied: number; hiddenColumns: string[] };
  }>();

  let filterControls: FilterControls | undefined;
  let initialized = false;
  let filterTimeout: ReturnType<typeof setTimeout> | null = null;
  let pendingFilters: (AppliedFilters & { resetScroll: boolean }) | null = null;
  let lastSortRevision = 0;
  let lastSearchValue: string | null = null;
  let lastFlagFilter: FlagFilterValue | null = null;
  let lastBookmarkedOnly: boolean | null = null;
  let lastSearchColumnsSignature: string | null = null;
  let lastConditionJson = 'null';
  let applyingHistory = false;
  let appliedRestoreNonce = 0;
  let pendingScrollRow: number | null = null;
  let pendingScrollLeft: number | null = null;
  let gotoRowInput: number | undefined;
  let annotationRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  let applyingProjectId: string | null = null;
  let lastSummaryFlagged = -1;
  let lastSummaryHiddenKey = '';

  let columnWidths: Map<string, number> = new Map();
  let exportSnapshot: ExportSnapshot | null = null;

  let destroyed = false;

  const queries = createWorkspaceQuery(workspace, {
    backend,
    isReady: () => initialized && !destroyed,
    restoreViewport: async (response, isCurrent) => {
      if (pendingScrollRow === null && pendingScrollLeft === null) return;
      const savedRow = pendingScrollRow;
      const savedLeft = pendingScrollLeft;
      // Restore only after the spacer and column grid have their new dimensions.
      await tick();
      if (!isCurrent() || destroyed) return;
      if (savedRow !== null) {
        const row = clampRow(savedRow, response.total_filtered_rows);
        applyScrollOffset(row, response.total_filtered_rows);
        rememberOffset(row);
      }
      if (savedLeft !== null && $bodyScrollEl) {
        $bodyScrollEl.scrollLeft = savedLeft;
        scrollLeft.set($bodyScrollEl.scrollLeft);
        if ($headerScrollEl) $headerScrollEl.scrollLeft = $bodyScrollEl.scrollLeft;
      }
      // Browser clamping during the DOM update must not replace the saved row.
      if (pendingScrollRow === savedRow) pendingScrollRow = null;
      if (pendingScrollLeft === savedLeft) pendingScrollLeft = null;
    },
    scopeChanged: () => {
      pendingScrollRow = null;
      lastSummaryFlagged = -1;
      lastSummaryHiddenKey = '';
    },
    summary: () => emitSummary(),
    notify: (message) => dispatch('notify', { message, tone: 'error' }),
  });
  const { requestPage, ensureRangeLoaded, applyFilters } = queries;

  // Keep the case stores in sync when the parent replaces the project
  // detail (for example after a refresh); the one-time assignment used to
  // leave child components on the initial snapshot.
  $: projectDetailStore.set(projectDetail);
  $: backendStore.set(backend);

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

  const view = createWorkspaceView(workspace, {
    backend,
    isReady: () => initialized && !destroyed,
    columnWidths: () => columnWidths,
    logicalRow: () => pendingScrollRow ?? rowOffset(),
    scrollLeft: () => pendingScrollLeft ?? get(scrollLeft),
    signature: () => queries.signature,
    notify: (message) => dispatch('notify', { message, tone: 'error' }),
  });
  const { flushPersist, schedulePersist } = view;

  export function snapshotSession(): WorkspaceSession | null {
    return view.snapshotSession();
  }

  const navigation = createWorkspaceNavigation(workspace, queries, {
    backend,
    isReady: () => initialized && !destroyed,
    isFilterPending: () => filterTimeout !== null,
    beforeScroll: () => { pendingScrollRow = null; },
    notify: (message) => dispatch('notify', { message, tone: 'error' }),
  });
  const { locatingRow, rowOffset, applyScrollOffset, jumpToFirstRow, jumpToLastRow } = navigation;
  const jumpToEnteredRow = () => navigation.jumpToEnteredRow(gotoRowInput);

  const queueAnnotationRefresh = () => {
    if (annotationRefreshTimer) clearTimeout(annotationRefreshTimer);
    annotationRefreshTimer = setTimeout(() => {
      annotationRefreshTimer = null;
      if (!destroyed && initialized) void forceRefreshFilteredRows(false);
    }, 50);
  };

  const annotationObserver = createWorkspaceAnnotationObserver(workspace, {
    bulk: queueAnnotationRefresh,
    single: () => {
      const selectedFlags = parseFlagFilter(get(flagFilter)) ?? [];
      // Ordinary source-order rows are updated in place. Annotation-dependent
      // result membership needs a fresh query after a single-row edit.
      if (get(bookmarkedOnly) || (selectedFlags.length > 0 && selectedFlags.length < 4)) {
        queueAnnotationRefresh();
      }
    },
  });

  const applyProjectDetail = async (detail: LoadProjectResponse, _resetScroll: boolean) => {
    // Any response from the previous project or screen must be ignored from here on.
    annotationObserver.suspend();
    if (annotationRefreshTimer) clearTimeout(annotationRefreshTimer);
    annotationRefreshTimer = null;
    invalidateViewRequests();
    const generation = get(viewGeneration);
    const projectId = detail.project.meta.id;
    view.cancelScheduled();
    if (generation !== get(viewGeneration)) {
      if (applyingProjectId === projectId) {
        applyingProjectId = null;
      }
      return;
    }

    // Requests from the previous project or screen must not keep the job
    // slots while this project loads.
    queries.cancel();
    initialized = false;
    applyingProjectId = projectId;
    const tabSession = session?.projectId === projectId ? session : null;
    currentProjectId.set(projectId);
    setHistoryProject(projectId);

    // Use this project's defaults while its saved view is loading.
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

    const saved = await view.loadSavedView(detail, tabSession, () =>
      !destroyed && generation === get(viewGeneration) && get(currentProjectId) === projectId);
    if (saved === undefined) {
      if (applyingProjectId === projectId) applyingProjectId = null;
      return;
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
    queries.initialize(filters);
    if (tabSession?.selectionSignature === queries.signature) {
      selectedRowIds.set(new Set(tabSession.selectedRowIds));
    }
    lastSearchValue = filters.search;
    lastFlagFilter = filters.flag;
    lastBookmarkedOnly = filters.bookmarkedOnly;
    lastSearchColumnsSignature = JSON.stringify(filters.searchColumns);
    lastSummaryFlagged = -1;
    lastSummaryHiddenKey = '';
    // Dialog state belongs to the previous project and must not be applied here.
    expandedCell.set(null);
    memoEditor.set(null);
    iocManagerOpen.set(false);
    if (tabSession) restoreHistory(tabSession.history);
    else resetHistory(get(conditionTree), savedRow);

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
    pendingScrollLeft = tabSession?.scrollLeft ?? 0;
    scrollLeft.set(pendingScrollLeft);
    applyScrollOffset(savedRow, detail.project.meta.total_records);
    if ($bodyScrollEl) {
      $bodyScrollEl.scrollLeft = 0;
    }
    if ($headerScrollEl) {
      $headerScrollEl.scrollLeft = 0;
    }
    annotationObserver.initialize();
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
      initializeColumnWidths(Object.fromEntries(columnWidths));
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
    const signature = JSON.stringify(scopedColumns);
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

  // Keep in sync with DataTable: the applied QuerySpec result is the only
  // source for the active row count, including bookmark-only filtering.
  $: effectiveTotalRows = $totalFilteredRows;
  $: windowGeometry = virtualWindow(
    $scrollTop,
    effectiveTotalRows,
    $viewportHeight,
    BUFFER
  );
  $: startIndex = windowGeometry.start;
  $: endIndex = windowGeometry.end;
  $: ensureRangeLoaded(startIndex, endIndex);
  $: if ($bodyScrollEl && maxScrollTop(effectiveTotalRows, $viewportHeight) >= $scrollTop && Math.abs($bodyScrollEl.scrollTop - $scrollTop) > 1) {
    $bodyScrollEl.scrollTop = $scrollTop;
  }

  const closeCell = () => {
    expandedCell.set(null);
  };

  const scheduleFilterRefresh = (
    searchValue: string,
    flagValue: FlagFilterValue,
    scopedColumns: string[],
    resetScroll: boolean
  ) => {
    navigation.cancelLookup();
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

  const clearAllFilters = () => {
    if (!initialized || destroyed) return;
    const previousOffset = rowOffset();
    const previousCondition = JSON.stringify(get(conditionPayload));
    clearFilterInputs(workspace);
    const nextCondition = JSON.stringify(get(conditionPayload));
    if (previousCondition !== nextCondition) {
      recordAppliedCondition(previousOffset, get(conditionTree), 0);
    }
    // Apply one clear operation immediately; the live-input observer must not
    // schedule a duplicate query or add the same rules to history twice.
    lastSearchValue = '';
    lastFlagFilter = 'all';
    lastBookmarkedOnly = false;
    lastSearchColumnsSignature = '[]';
    lastConditionJson = nextCondition;
    applyingHistory = false;
    navigation.cancelLookup();
    void forceRefreshFilteredRows(true);
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
    // The displayed result counts lag the applied QuerySpec until a page of
    // the new result returns. Do not open a dialog whose counts and export
    // target disagree. This covers a scrolled position too, where the first
    // request is not page 0.
    if ($appliedResultLoading) {
      dispatch('notify', {
        message: 'Wait for the current query to finish before exporting.',
        tone: 'error'
      });
      return;
    }
    const filters = get(appliedFilters);
    exportSnapshot = {
      search: filters.search,
      searchColumns: [...filters.searchColumns],
      condition: filters.condition,
      flagFilter: filters.flag,
      bookmarkedOnly: filters.bookmarkedOnly,
      sortKey: $sortKey,
      sortDirection: $sortDirection,
      rowIndices: Array.from($selectedRowIds),
      // The metadata preserves the evidence row count while a query loads.
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
      const suggested = exportFileName(projectDetail.project.meta.name);
      const selected = await save({
        filters: [{ name: 'CSV with flags', extensions: ['csv'] }],
        defaultPath: suggested
      });
      if (!selected) {
        return;
      }
      const result = await backend.exportProject(
        buildExportArgs(projectDetail.project.meta.id, snapshot, options, selected)
      );
      const warnings = result.warnings.length ? ` ${result.warnings.join(' ')}` : '';
      dispatch('notify', {
        message: `Exported ${result.rows} rows to CSV.${warnings}`,
        tone: 'success'
      });
    } catch (error) {
      console.error(error);
      // Tauri returns the engine error as a plain string; keep the actionable
      // message (for example, choosing another destination).
      dispatch('notify', {
        message: errorMessage(error, 'Failed to export CSV.'),
        tone: 'error'
      });
    } finally {
      isExporting.set(false);
    }
  };

  const handleSortChanged = () => {
    const filters = get(appliedFilters);
    void applyFilters(
      {
        search: filters.search,
        flag: filters.flag,
        bookmarkedOnly: filters.bookmarkedOnly,
        searchColumns: [...filters.searchColumns],
        condition: filters.condition,
      },
      false,
      true
    );
  };

  $: if (initialized && $sortRevision !== lastSortRevision) {
    lastSortRevision = $sortRevision;
    handleSortChanged();
  }

  onMount(() => {
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

    window.addEventListener('keydown', handleKeydown);

    return () => {
      window.removeEventListener('keydown', handleKeydown);
    };
  });

  onDestroy(() => {
    if (annotationRefreshTimer) clearTimeout(annotationRefreshTimer);
    const snapshot = snapshotSession();
    if (snapshot) onSessionSnapshot(snapshot);
    // Snapshot the owned stores before disposal releases the row cache.
    void flushPersist(projectDetail.project.meta.id, false);
    destroyed = true;
    workspace.dispose();
    view.cancelScheduled();
    if (filterTimeout) {
      clearTimeout(filterTimeout);
      filterTimeout = null;
    }
  });

  const emitSummary = () => {
    if (destroyed || !initialized) return;
    const hidden = Array.from($hiddenColumns);
    const hiddenKey = hidden.join('|');
    if (lastSummaryFlagged === $flaggedCount && lastSummaryHiddenKey === hiddenKey) {
      return;
    }
    lastSummaryFlagged = $flaggedCount;
    lastSummaryHiddenKey = hiddenKey;
    dispatch('summary', { 
      projectId: projectDetail.project.meta.id,
      flagged: $flaggedCount, 
      iocApplied: projectDetail?.project.meta.ioc_applied_records ?? 0,
      hiddenColumns: hidden 
    });
  };
</script>

<section class="project-view">
  <FilterControls bind:this={filterControls} on:iocManagerOpen={openIocManager} on:export={openExportDialog} on:refresh={queueAnnotationRefresh}>

  <div slot="navigation" class="table-navigation">
    <HistoryButtons />
    <div class="row-jump" role="group" aria-label="Original row navigation">
    <label class="row-jump-field">
      <span>Original row</span>
      <input
        type="number"
        min="1"
        max={Math.max(1, $totalRows)}
        step="1"
        placeholder={`1–${$totalRows}`}
        aria-label="Original row number"
        title="Original row number shown in the # column"
        bind:value={gotoRowInput}
        disabled={!initialized || $totalRows === 0}
        on:keydown={(event) => {
          if (event.key === 'Enter') {
            event.preventDefault();
            void jumpToEnteredRow();
          }
        }}
      />
    </label>
    <button
      type="button"
      class="ghost"
      on:click={jumpToEnteredRow}
      aria-label="Go"
      aria-busy={$locatingRow}
      title="Find this original row within the current filtered and sorted result"
      disabled={!initialized || $totalRows === 0 || $appliedResultLoading || filterTimeout !== null || $locatingRow}
    >
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M4 12h16m-6-6 6 6-6 6" /></svg>
      <span>Go</span>
    </button>
    <button
      type="button"
      class="ghost"
      on:click={jumpToFirstRow}
      aria-label="First"
      title="First row in the current result"
      disabled={!initialized || $totalFilteredRows === 0 || $appliedResultLoading || filterTimeout !== null}
    >
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 5v14m13-14-7 7 7 7" /></svg>
      <span>First</span>
    </button>
    <button
      type="button"
      class="ghost"
      on:click={jumpToLastRow}
      aria-label="Last"
      title="Last row in the current result"
      disabled={!initialized || $totalFilteredRows === 0 || $appliedResultLoading || filterTimeout !== null}
    >
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M19 5v14M6 5l7 7-7 7" /></svg>
      <span>Last</span>
    </button>
    <span class="row-jump-range">of {$totalRows.toLocaleString()}</span>
    </div>
  </div>
  </FilterControls>

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

  <DataTable {columnWidths} on:notify={(e) => dispatch('notify', e.detail)}
    on:columnRule={(event) => void filterControls?.openRuleForColumn(event.detail.columnId)} />
  <FilterStatus on:clear={clearAllFilters} />
</section>
