<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { get } from 'svelte/store';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import DataTable from './project_view/DataTable.svelte';
  import IocManagerDialog from './project_view/IocManagerDialog.svelte';
  import MemoEditorDialog from './project_view/MemoEditorDialog.svelte';
  import ExpandedCellDialog from './project_view/ExpandedCellDialog.svelte';
  import FilterControls from './project_view/FilterControls.svelte';
  import TimelinePanel from './project_view/TimelinePanel.svelte';
  import type { IocEntry, LoadProjectResponse, ProjectRow } from '../types';
  import type { Backend } from '../backend';
  import './project-view.css';
  import { conditionPayload, conditionTree } from '../stores/condition';
  import { cloneTree } from '../viewState';
  import {
    goBack,
    goForward,
    historyRestore,
    loadProjectView,
    recordAppliedCondition,
    rememberOffset,
    resetHistory,
    saveProjectView,
  } from '../stores/viewHistory';
  import {
    projectDetail as projectDetailStore,
    backend as backendStore,
    visibleColumns,
    hiddenColumns,
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
    totalRows,
    totalFilteredRows,
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
    formatCell,
    FLAG_OPTIONS,
    FLAG_FILTER_OPTIONS,
    PRIORITY_FLAGS,
    ROW_HEIGHT,
    BUFFER,
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
    positionToRowIndex
  } from './project_view/state';
import type {
    FlagFilterValue,
    CachedRow,
    VirtualRow,
    AppliedFilters
  } from './project_view/state';

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
  let persistTimer: ReturnType<typeof setTimeout> | null = null;
  let lastSummaryFlagged = -1;
  let lastSummaryHiddenKey = '';

  let columnWidths: Map<string, number> = new Map();
  let activeFilters: AppliedFilters = { search: '', flag: 'all', bookmarkedOnly: false, searchColumns: [], condition: null };
  let resizeObserver: ResizeObserver | null = null;
  let observedScrollEl: HTMLDivElement | null = null;

  let releaseHeaderSyncFrame: number | null = null;
  let releaseBodySyncFrame: number | null = null;
  let isSyncingHeaderScroll = false;
  let isSyncingBodyScroll = false;

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
    totalRows.set(0);
    flaggedCount.set(0);
    filteredFlaggedCount.set(0);
    positionToRowIndex.set(new Map());
  };

  const initializeColumnWidths = () => {
    if (!projectDetail) {
      columnWidths = new Map();
      return;
    }
    const next = new Map<string, number>();
    for (const column of projectDetail.columns) {
      // Calculate width using maximum character count from Polars
      const headerChars = Math.min(column.length, WIDTH_LIMIT_CHARS);
      const dataChars = Math.min(projectDetail.column_max_chars[column] ?? headerChars, WIDTH_LIMIT_CHARS);
      const maxChars = Math.max(headerChars, dataChars);
      const estimated = Math.round(maxChars * CHAR_PIXEL + COLUMN_PADDING);
      const width = Math.min(Math.max(estimated, MIN_DATA_WIDTH), MAX_DATA_WIDTH);
      next.set(column, width);
    }
    columnWidths = next;
  };

  const rowOffset = () => Math.max(0, Math.floor(get(scrollTop) / ROW_HEIGHT));

  const flushPersist = (projectId: string | null) => {
    if (!projectId) {
      return;
    }
    saveProjectView(projectId, {
      condition: get(conditionTree),
      hiddenColumns: Array.from(get(hiddenColumns)),
      sortKey: get(sortKey),
      sortDirection: get(sortDirection),
      offset: rowOffset(),
    });
  };

  const schedulePersist = () => {
    if (persistTimer) {
      clearTimeout(persistTimer);
    }
    persistTimer = setTimeout(() => {
      persistTimer = null;
      flushPersist(get(currentProjectId));
    }, 250);
  };

  const applyScrollOffset = (offset: number) => {
    const px = Math.max(0, offset) * ROW_HEIGHT;
    suppressScrollRecord = true;
    scrollTop.set(px);
    if ($bodyScrollEl) {
      $bodyScrollEl.scrollTop = px;
    }
    suppressScrollRecord = false;
  };

  const applyProjectDetail = (detail: LoadProjectResponse, resetScroll: boolean) => {
    const previousId = get(currentProjectId);
    if (previousId && previousId !== detail.project.meta.id) {
      flushPersist(previousId);
    }
    const saved = loadProjectView(detail.project.meta.id, detail.columns);
    const hidden = new Set(saved ? saved.hiddenColumns : (detail.hidden_columns ?? []));
    conditionTree.set(
      saved ? cloneTree(saved.condition) : { type: 'group', id: 'root', kind: 'and', collapsed: false, children: [] }
    );
    lastConditionJson = JSON.stringify(get(conditionPayload));
    currentProjectId.set(detail.project.meta.id);
    hiddenColumns.set(hidden);
    searchColumns.set([]);
    sortKey.set(saved?.sortKey ?? null);
    sortDirection.set(saved?.sortDirection ?? 'asc');
    iocDraft.set(detail.iocs.map((entry) => ({
      id: crypto.randomUUID(),
      flag: normalizeIocFlag(entry.flag),
      tag: entry.tag,
      query: entry.query
    })));
    initializeColumnWidths();
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
    lastAppliedFilters = buildFilterSignature(filters);
    lastSearchValue = filters.search;
    lastFlagFilter = filters.flag;
    lastBookmarkedOnly = filters.bookmarkedOnly;
    lastSearchColumnsSignature = filters.searchColumns.join('|');
    filterRequestId = 0;
    lastSummaryFlagged = -1;
    lastSummaryHiddenKey = '';
    expandedCell.set(null);
    resetHistory(get(conditionTree), saved?.offset ?? 0);

    // Don't use seededRows from initial_rows as they may be stale
    // Always fetch fresh data from backend to ensure flags and memos are current
    totalRows.set(detail.project.meta.total_records);
    flaggedCount.set(detail.project.meta.flagged_records);
    applyScrollOffset(saved?.offset ?? 0);
    if ($bodyScrollEl) {
      $bodyScrollEl.scrollLeft = 0;
    }
    if ($headerScrollEl) {
      $headerScrollEl.scrollLeft = 0;
    }
    initialized = true;
    // Always request first page to ensure flags and memos are reflected after project reload
    // Wait for the first page to load before marking as ready to render
    requestPage(0, true).catch((error) => {
      console.error('[debug] applyProjectDetail first page failed:', error);
    });
  };

  $: if (projectDetail) {
    const projectChanged = projectDetail.project.meta.id !== $currentProjectId;
    const nextHiddenColumns = projectDetail.hidden_columns ?? [];
    const hiddenChanged =
      $lastHiddenColumnsRef === null ||
      !areStringsEqual(nextHiddenColumns, $lastHiddenColumnsRef);
    if (!initialized || projectChanged) {
      applyProjectDetail(projectDetail, true);
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
    applyingHistory = true;
    conditionTree.set(cloneTree($historyRestore.tree));
    applyScrollOffset($historyRestore.offset);
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

  $: persistedView = initialized && projectDetail
    ? {
        hidden: Array.from($hiddenColumns),
        sortKey: $sortKey,
        sortDirection: $sortDirection,
        offset: $scrollTop,
        tree: $conditionTree,
      }
    : null;
  $: if (persistedView) {
    schedulePersist();
  }

  $: visibleCount =
    Math.ceil(($viewportHeight || ROW_HEIGHT) / ROW_HEIGHT) + BUFFER * 2;
  $: effectiveTotalRows = ($flagFilter !== 'all' || $bookmarkedOnly || ($search && $search.trim().length > 0) || $conditionPayload != null) ? $totalFilteredRows : $totalRows;
  $: maxStart = Math.max(0, effectiveTotalRows - visibleCount);
  $: startIndex = Math.min(
    maxStart,
    Math.max(0, Math.floor($scrollTop / ROW_HEIGHT) - BUFFER)
  );
  $: endIndex = Math.min(effectiveTotalRows, startIndex + visibleCount);
  $: ensureRangeLoaded(startIndex, endIndex);
  $: offsetY = startIndex * ROW_HEIGHT;
  $: totalHeight = effectiveTotalRows * ROW_HEIGHT;
  $: if ($bodyScrollEl && totalHeight >= $scrollTop && Math.abs($bodyScrollEl.scrollTop - $scrollTop) > 1) {
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
    for (let page = firstPage; page <= clampedLastPage; page += 1) {
      if (!$loadedPages.has(page) && !$pendingPages.has(page)) {
        void requestPage(page);
      }
    }
  };

  const requestPage = (pageIndex: number, force = false): Promise<void> => {
    if (!projectDetail) return Promise.resolve();
    const normalizedPage = Math.max(0, pageIndex);
    if (!force && $loadedPages.has(normalizedPage)) {
      return Promise.resolve();
    }
    if (!force && $pendingPages.has(normalizedPage)) {
      return Promise.resolve();
    }
    const offset = normalizedPage * PAGE_SIZE;
    const limit = PAGE_SIZE;
    const requestId = filterRequestId;
    const filters = activeFilters;
    const nextPending = new Set($pendingPages);
    nextPending.add(normalizedPage);
    pendingPages.set(nextPending);

    const payload = {
      projectId: projectDetail.project.meta.id,
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

    return backend
      .queryProjectRows(payload)
      .then((response) => {
        const nextPendingPages = new Set($pendingPages);
        nextPendingPages.delete(normalizedPage);
        pendingPages.set(nextPendingPages);
        if (requestId !== filterRequestId) {
          return;
        }
        totalRows.set(response.total_rows);
        totalFilteredRows.set(response.total_filtered_rows);
        // `flaggedCount` always means the whole project, never this page's slice.
        flaggedCount.set(response.annotated_total);
        filteredFlaggedCount.set(response.annotated_filtered);
        emitSummary();
        const normalizedRows = response.rows.map((row) => normalizeRow(row));
        rowsCache.update((cache) => {
          const nextCache = new Map(cache);
          normalizedRows.forEach((row) => {
            nextCache.set(row.row_index, row);
          });
          return nextCache;
        });
        // Preserve backend order: map display positions to row_index
        positionToRowIndex.update((m) => {
          const next = new Map(m);
          normalizedRows.forEach((row, i) => {
            next.set(response.offset + i, row.row_index);
          });
          return next;
        });
        const nextLoaded = new Set($loadedPages);
        nextLoaded.add(Math.floor(response.offset / PAGE_SIZE));
        loadedPages.set(nextLoaded);
      })
      .catch((error) => {
        console.error(error);
        const nextPendingPages = new Set($pendingPages);
        nextPendingPages.delete(normalizedPage);
        pendingPages.set(nextPendingPages);
      });
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
    activeFilters = {
      search: normalized.search,
      flag: normalized.flag,
      bookmarkedOnly: normalized.bookmarkedOnly,
      searchColumns: [...normalized.searchColumns],
      condition: normalized.condition,
    };
    lastAppliedFilters = signature;
    filterRequestId += 1;
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
    const targetRow = resetScroll ? 0 : Math.max(0, Math.floor($scrollTop / ROW_HEIGHT));
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
  $: stickyFlagOffset = INDEX_COL_WIDTH;
  $: stickyMemoOffset = INDEX_COL_WIDTH + FLAG_COL_WIDTH;
  $: stickyDataOffset = stickyMemoOffset + MEMO_COL_WIDTH;
  $: stickyVariables = `--sticky-flag:${stickyFlagOffset}px; --sticky-memo:${stickyMemoOffset}px; --sticky-data:${stickyDataOffset}px;`;

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
    rememberOffset(Math.max(0, Math.floor(target.scrollTop / ROW_HEIGHT)));
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

  const exportProject = async () => {
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
        isExporting.set(false);
        return;
      }
      await backend.exportProject({
        projectId: projectDetail.project.meta.id,
        destination: selected
      });
      dispatch('notify', { message: 'Exported CSV with flags and memos.', tone: 'success' });
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to export CSV.', tone: 'error' });
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
        if ($iocManagerOpen) {
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
    if (persistTimer) {
      clearTimeout(persistTimer);
      persistTimer = null;
    }
    flushPersist(get(currentProjectId));
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
  <FilterControls on:iocManagerOpen={openIocManager} on:export={exportProject} on:refresh={() => dispatch('refresh')} />

  <TimelinePanel />

  <ExpandedCellDialog on:notify={(e) => dispatch('notify', e.detail)} />

  <MemoEditorDialog on:notify={(e) => dispatch('notify', e.detail)} />

  <IocManagerDialog on:notify={(e) => dispatch('notify', e.detail)} on:refresh={() => dispatch('refresh')} />

  <DataTable {columnWidths} />
</section>
