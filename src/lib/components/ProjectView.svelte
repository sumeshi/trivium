<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { open, save } from '@tauri-apps/api/dialog';
  import DataTable from './project_view/DataTable.svelte';
  import IocManagerDialog from './project_view/IocManagerDialog.svelte';
  import MemoEditorDialog from './project_view/MemoEditorDialog.svelte';
  import ExpandedCellDialog from './project_view/ExpandedCellDialog.svelte';
  import FilterControls from './project_view/FilterControls.svelte';
  import type { IocEntry, LoadProjectResponse, ProjectRow } from '../types';
  import type { Backend } from '../backend';
  import './project-view.css';
  import {
    projectDetail as projectDetailStore,
    backend as backendStore,
    visibleColumns,
    hiddenColumns,
    search,
    flagFilter,
    sortKey,
    sortDirection,
    isExporting,
    isUpdatingColumns,
    rowsCache,
    pendingPages,
    loadedPages,
    totalRows,
    totalFlagged,
    flaggedCount,
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
    sanitizeMemoInput,
    mapStoredFlag,
    normalizeRow,
    formatCell,
    FLAG_OPTIONS,
    FLAG_FILTER_OPTIONS,
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
    lastHiddenColumnsRef
  } from './project_view/state';
  import type {
    FlagSymbol,
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
    summary: { flagged: number; hiddenColumns: string[] };
  }>();

  let columnsOpen = false;
  let columnPickerEl: HTMLDivElement | null = null;

  let flagMenuOpen = false;
  let flagPickerEl: HTMLDivElement | null = null;

  let memoDraft = '';
  let memoSaving = false;
  let memoError: string | null = null;

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
  let lastColumnsSignature: string | null = null;
  let lastSummaryFlagged = -1;
  let lastSummaryHiddenKey = '';

  let columnWidths: Map<string, number> = new Map();
  let activeFilters: AppliedFilters = { search: '', flag: 'all', columns: [] };
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
    `${filters.search}::${filters.flag}::${filters.columns.join('|')}::${$sortKey ?? ''}::${$sortDirection}`;

  const resetPaginationState = () => {
    rowsCache.set(new Map());
    pendingPages.set(new Set());
    loadedPages.set(new Set());
    totalRows.set(0);
    totalFlagged.set(0);
    flaggedCount.set(0);
  };

  const initializeColumnWidths = () => {
    if (!projectDetail) {
      columnWidths = new Map();
      return;
    }
    console.log('initializeColumnWidths called, column_max_chars:', projectDetail.column_max_chars);
    console.log('Available columns:', projectDetail.columns);
    console.log('column_max_chars type:', typeof projectDetail.column_max_chars);
    console.log('column_max_chars is array:', Array.isArray(projectDetail.column_max_chars));
    console.log('column_max_chars keys:', Object.keys(projectDetail.column_max_chars));
    console.log('column_max_chars values:', Object.values(projectDetail.column_max_chars));
    const next = new Map<string, number>();
    for (const column of projectDetail.columns) {
      // Polarsから取得した最大文字数を使用して幅を計算
      const headerChars = Math.min(column.length, WIDTH_LIMIT_CHARS);
      const dataChars = Math.min(projectDetail.column_max_chars[column] ?? headerChars, WIDTH_LIMIT_CHARS);
      const maxChars = Math.max(headerChars, dataChars);
      const estimated = Math.round(maxChars * CHAR_PIXEL + COLUMN_PADDING);
      const width = Math.min(Math.max(estimated, MIN_DATA_WIDTH), MAX_DATA_WIDTH);
      console.log(`Column ${column}: headerChars=${headerChars}, dataChars=${dataChars}, maxChars=${maxChars}, estimated=${estimated}, width=${width}`);
      next.set(column, width);
    }
    columnWidths = next;
    console.log('Final columnWidths:', columnWidths);
  };

  const applyProjectDetail = (detail: LoadProjectResponse, resetScroll: boolean) => {
    currentProjectId.set(detail.project.meta.id);
    hiddenColumns.set(new Set(detail.hidden_columns ?? []));
    iocDraft.set(detail.iocs.map((entry) => ({
      id: crypto.randomUUID(),
      flag: normalizeIocFlag(entry.flag),
      tag: entry.tag,
      query: entry.query
    })));
    initializeColumnWidths();
    const currentColumns = detail.columns.filter((column) => !$hiddenColumns.has(column));
    const filters: AppliedFilters = {
      search: $search.trim(),
      flag: $flagFilter,
      columns: currentColumns,
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
      columns: [...filters.columns],
    };
    lastAppliedFilters = buildFilterSignature(filters);
    lastSearchValue = filters.search;
    lastFlagFilter = filters.flag;
    lastColumnsSignature = currentColumns.join('|');
    filterRequestId = 0;
    lastSummaryFlagged = -1;
    lastSummaryHiddenKey = '';
    sortKey.set(null);
    sortDirection.set('asc');
    expandedCell.set(null);

    // Don't use seededRows from initial_rows as they may be stale
    // Always fetch fresh data from backend to ensure flags and memos are current
    console.log('[debug] applyProjectDetail', {
      projectId: detail.project.meta.id,
      totalRecords: detail.project.meta.total_records,
      columns: detail.columns.length,
    });
    
    totalRows.set(detail.project.meta.total_records);
    totalFlagged.set(detail.project.flagged_records);
    flaggedCount.set($totalFlagged);
    scrollTop.set(0);
    if ($bodyScrollEl) {
      $bodyScrollEl.scrollTop = 0;
      $bodyScrollEl.scrollLeft = 0;
    }
    if ($headerScrollEl) {
      $headerScrollEl.scrollLeft = 0;
    }
    initialized = true;
    // Always request first page to ensure flags and memos are reflected after project reload
    console.log('[debug] applyProjectDetail request first page');
    void requestPage(0, true);
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
      hiddenColumns.set(new Set(nextHiddenColumns));
      initializeColumnWidths();
      const columns = getSearchableColumns();
      const filters: AppliedFilters = {
        search: $search.trim(),
        flag: $flagFilter,
        columns,
      };
      lastSearchValue = filters.search;
      lastFlagFilter = filters.flag;
      lastColumnsSignature = columns.join('|');
      void applyFilters(filters, false, true);
    }
    lastHiddenColumnsRef.set([...nextHiddenColumns]);
  }

  $: if (initialized) {
    const searchValue = $search.trim();
    const flagValue = $flagFilter;
    const columns = getSearchableColumns();
    const signature = columns.join('|');
    const searchChanged = lastSearchValue === null || searchValue !== lastSearchValue;
    const flagChanged = lastFlagFilter === null || flagValue !== lastFlagFilter;
    const columnsChanged = lastColumnsSignature === null || signature !== lastColumnsSignature;
    if (searchChanged || flagChanged || columnsChanged) {
      scheduleFilterRefresh(searchValue, flagValue, columns, searchChanged);
      lastSearchValue = searchValue;
      lastFlagFilter = flagValue;
      lastColumnsSignature = signature;
    }
  }

  $: visibleCount =
    Math.ceil(($viewportHeight || ROW_HEIGHT) / ROW_HEIGHT) + BUFFER * 2;
  $: maxStart = Math.max(0, $totalRows - visibleCount);
  $: startIndex = Math.min(
    maxStart,
    Math.max(0, Math.floor($scrollTop / ROW_HEIGHT) - BUFFER)
  );
  $: endIndex = Math.min($totalRows, startIndex + visibleCount);
  $: ensureRangeLoaded(startIndex, endIndex);
  $: virtualRows = buildVirtualRows(startIndex, endIndex, $rowsCache);
  $: offsetY = startIndex * ROW_HEIGHT;
  $: totalHeight = $totalRows * ROW_HEIGHT;
  $: loadedRowCount = $rowsCache.size;

  const buildVirtualRows = (
    start: number,
    end: number,
    cache: Map<number, CachedRow>
  ): VirtualRow[] => {
    if (end <= start) {
      return [];
    }
    const result: VirtualRow[] = [];
    for (let position = start; position < end; position += 1) {
      const row = cache.get(position) ?? null;
      result.push({ position, row });
    }
    return result;
  };

  const updateColumnWidthsFromRows = (rows: CachedRow[]) => {
    if (!rows.length) {
      return;
    }
    const next = new Map(columnWidths);
    for (const column of $visibleColumns) {
      const current = next.get(column) ?? MIN_DATA_WIDTH;
      let updated = current;
      for (const row of rows) {
        const value = row.displayCache[column] ?? '';
        const estimated = Math.round(Math.min(value.length, WIDTH_LIMIT_CHARS) * CHAR_PIXEL + COLUMN_PADDING);
        if (estimated > updated) {
          updated = Math.min(estimated, MAX_DATA_WIDTH);
        }
      }
      next.set(column, updated);
    }
    columnWidths = next;
  };

  const ensureRangeLoaded = (start: number, end: number) => {
    if (!initialized || !projectDetail || end <= start) {
      return;
    }
    const firstPage = Math.max(0, Math.floor(start / PAGE_SIZE) - PREFETCH_PAGES);
    const lastPosition = Math.max(start, end - 1);
    const lastPage = Math.max(firstPage, Math.floor(lastPosition / PAGE_SIZE) + PREFETCH_PAGES);
    const maxPageIndex = Math.max(0, Math.floor(Math.max($totalRows - 1, 0) / PAGE_SIZE));
    const clampedLastPage = Math.min(lastPage, maxPageIndex);
    console.log('[debug] ensureRangeLoaded', {
      start,
      end,
      firstPage,
      lastPage: clampedLastPage,
      loadedPages: Array.from($loadedPages.values()),
      pendingPages: Array.from($pendingPages.values()),
    });
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
      search: filters.search.length > 0 ? filters.search : undefined,
      flagFilter: filters.flag,
      columns:
        filters.columns.length === projectDetail.columns.length
          ? undefined
          : [...filters.columns],
      offset,
      limit,
      sortKey: $sortKey ?? undefined,
      sortDirection: $sortDirection,
    };

    console.log('[debug] requestPage', {
      page: normalizedPage,
      offset,
      limit,
      filters,
      sortKey: $sortKey,
      sortDirection: $sortDirection,
    });
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
        totalFlagged.set(response.total_flagged);
        flaggedCount.set($totalFlagged);
        emitSummary();
        const normalizedRows = response.rows.map((row) => normalizeRow(row));
        console.log('[debug] requestPage response', {
          page: normalizedPage,
          received: normalizedRows.length,
          totalRows: response.total_rows,
          offset: response.offset,
        });
        updateColumnWidthsFromRows(normalizedRows);
        const nextCache = new Map($rowsCache);
        normalizedRows.forEach((row, index) => {
          nextCache.set(response.offset + index, row);
        });
        rowsCache.set(nextCache);
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
    console.log('applyFilters called with:', filters, 'projectDetail:', !!projectDetail);
    if (!projectDetail) {
      console.log('No projectDetail, returning early');
      return Promise.resolve();
    }
    const normalized: AppliedFilters = {
      search: filters.search.trim(),
      flag: filters.flag,
      columns: [...filters.columns],
    };
    const signature = buildFilterSignature(normalized);
    if (!force && signature === lastAppliedFilters) {
      return Promise.resolve();
    }
    activeFilters = {
      search: normalized.search,
      flag: normalized.flag,
      columns: [...normalized.columns],
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


  const openCell = (column: string, value: string) => {
    expandedCell.set({ column, value });
  };

  const closeCell = () => {
    expandedCell.set(null);
  };

  const openMemoEditor = (row: CachedRow) => {
    memoEditor.set({ row });
    memoDraft = row.memo ?? '';
    memoError = null;
    memoSaving = false;
  };

  const closeMemoEditor = () => {
    memoEditor.set(null);
    memoDraft = '';
    memoError = null;
    memoSaving = false;
  };

  const handleCellKeydown = (event: KeyboardEvent, column: string, value: string) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      openCell(column, value);
    }
  };

  const handleBackdropKey = (event: KeyboardEvent) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      closeCell();
    }
  };

  const handleBackdropClick = (event: MouseEvent) => {
    if (event.target === event.currentTarget) {
      closeCell();
    }
  };

  const saveMemo = async () => {
    if (!$memoEditor || memoSaving || !projectDetail) return;
    const sanitized = sanitizeMemoInput(memoDraft).trim();
    memoSaving = true;
    memoError = null;
    try {
      await backend.updateFlag({
        projectId: projectDetail.project.meta.id,
        rowIndex: $memoEditor.row.row_index,
        flag: $memoEditor.row.flag,
        memo: sanitized.length ? sanitized : null
      });
      await forceRefreshFilteredRows(false);
      dispatch('notify', { message: 'Memo updated.', tone: 'success' });
      closeMemoEditor();
    } catch (error) {
      console.error(error);
      memoError = 'Failed to update memo.';
      dispatch('notify', { message: 'Failed to update memo.', tone: 'error' });
    } finally {
      memoSaving = false;
    }
  };

  const handleMemoBackdropClick = (event: MouseEvent) => {
    if (event.target === event.currentTarget && !memoSaving) {
      closeMemoEditor();
    }
  };

  const handleMemoBackdropKey = (event: KeyboardEvent) => {
    if (!memoSaving && (event.key === 'Escape' || event.key === 'Enter')) {
      event.preventDefault();
      closeMemoEditor();
    }
  };

  const copyExpandedCell = async () => {
    if (!$expandedCell) return;
    const text = $expandedCell.value ?? '';
    try {
      if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
        await navigator.clipboard.writeText(text);
      } else {
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
      }
      dispatch('notify', { message: 'Copied cell value.', tone: 'success' });
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to copy cell value.', tone: 'error' });
    }
  };

  const openIocManager = () => {
    console.log('openIocManager called, projectDetail:', projectDetail);
    if (!projectDetail) {
      console.log('No projectDetail, returning early');
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
    console.log('iocManagerOpen set to true');
  };

  const closeIocManager = () => {
    iocManagerOpen.set(false);
    iocError = null;
    isSavingIocs = false;
  };

  const addIocEntry = () => {
    iocDraft.update(d => [...d, { flag: 'critical', tag: '', query: '' }]);
  };

  const updateIocEntry = (index: number, field: keyof IocEntry, value: string) => {
    iocDraft.update(d => d.map((entry, current) => {
      if (current !== index) return entry;
      if (field === 'flag') {
        return { ...entry, flag: normalizeIocFlag(value) };
      }
      return { ...entry, [field]: value };
    }));
  };

  const removeIocEntry = (index: number) => {
    iocDraft.update(d => d.filter((_, current) => current !== index));
  };

  const handleIocFieldChange = (index: number, field: keyof IocEntry, event: Event) => {
    const target = event.currentTarget as HTMLInputElement | HTMLSelectElement;
    updateIocEntry(index, field, target.value);
  };

  const sanitizeIocEntries = (): IocEntry[] =>
    $iocDraft
      .map((entry) => ({
        flag: normalizeIocFlag(entry.flag),
        tag: entry.tag.trim(),
        query: entry.query.trim()
      }))
      .filter((entry) => entry.query.length > 0)
      .sort((a, b) => a.tag.localeCompare(b.tag));

  const saveIocEntries = async () => {
    if (!projectDetail) return;
    isSavingIocs = true;
    iocError = null;
    try {
      const sanitized = sanitizeIocEntries();
      await backend.saveIocs({
        projectId: projectDetail.project.meta.id,
        entries: sanitized
      });
      dispatch('notify', { message: 'IOC rules updated.', tone: 'success' });
      closeIocManager();
      dispatch('refresh');
    } catch (error) {
      console.error(error);
      iocError =
        error instanceof Error ? error.message : 'Failed to save IOC rules.';
    } finally {
      isSavingIocs = false;
    }
  };

  const importIocEntries = async () => {
    if (!projectDetail) return;
    iocError = null;
    if (backend.isNative) {
      try {
        const selected = await open({
          multiple: false,
          filters: [{ name: 'IOC CSV', extensions: ['csv'] }]
        });
        if (!selected) {
          return;
        }
        isSavingIocs = true;
        const path = Array.isArray(selected) ? selected[0] : selected;
        await backend.importIocs({
          projectId: projectDetail.project.meta.id,
          path
        });
        dispatch('notify', { message: 'Imported IOC rules.', tone: 'success' });
        closeIocManager();
        dispatch('refresh');
      } catch (error) {
        console.error(error);
        iocError =
          error instanceof Error ? error.message : 'Failed to import IOC rules.';
      } finally {
        isSavingIocs = false;
      }
    } else if (iocImportInput) {
      iocImportInput.value = '';
      iocImportInput.click();
    }
  };

  const escapeCsvValue = (value: string): string =>
    /[",\n\r]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;

  const buildIocCsv = (entries: IocEntry[]) => {
    const header = 'flag,tag,query';
    const rows = entries.map((entry) =>
      [entry.flag, entry.tag, entry.query].map(escapeCsvValue).join(',')
    );
    return [header, ...rows].join('\n');
  };

  const exportIocEntries = async () => {
    if (!projectDetail) return;
    try {
      if (backend.isNative) {
        const destination = await save({
          filters: [{ name: 'IOC CSV', extensions: ['csv'] }],
          defaultPath: `${projectDetail.project.meta.name.replace(/\.[^.]+$/, '')}-iocs.csv`
        });
        if (!destination) {
          return;
        }
        await backend.exportIocs({
          projectId: projectDetail.project.meta.id,
          destination
        });
      } else {
        const csv = buildIocCsv(sanitizeIocEntries());
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = `${projectDetail.project.meta.name.replace(/\.[^.]+$/, '')}-iocs.csv`;
        document.body.appendChild(anchor);
        anchor.click();
        document.body.removeChild(anchor);
        URL.revokeObjectURL(url);
      }
      dispatch('notify', { message: 'Exported IOC rules.', tone: 'success' });
    } catch (error) {
      console.error(error);
      iocError =
        error instanceof Error ? error.message : 'Failed to export IOC rules.';
    }
  };

  const parseIocCsvRows = (content: string): string[][] => {
    const rows: string[][] = [];
    let currentRow: string[] = [];
    let currentField = '';
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

      if (char === ',' && !inQuotes) {
        currentRow.push(currentField);
        currentField = '';
        continue;
      }

      if ((char === '\n' || char === '\r') && !inQuotes) {
        if (char === '\r' && next === '\n') {
          i += 1;
        }
        currentRow.push(currentField);
        if (currentRow.some((cell) => cell.trim().length > 0)) {
          rows.push([...currentRow]);
        }
        currentRow = [];
        currentField = '';
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

  const parseIocCsvText = (content: string): IocEntry[] => {
    const table = parseIocCsvRows(content);
    if (!table.length) {
      return [];
    }
    const [header, ...body] = table;
    const firstCell = header[0]?.toLowerCase() ?? '';
    const hasHeader = firstCell.includes('flag');
    const records = hasHeader ? body : table;
    const result: IocEntry[] = [];
    for (const record of records) {
      const [flagValue = '', tag = '', queryValue = ''] = record;
      const normalizedQuery = queryValue.trim();
      if (!normalizedQuery) continue;
      result.push({
        flag: normalizeIocFlag(flagValue),
        tag: tag.trim(),
        query: normalizedQuery
      });
    }
    return result;
  };

  const handleIocFileUpload = async (event: Event) => {
    if (!projectDetail) return;
    const target = event.currentTarget as HTMLInputElement | null;
    const file = target?.files?.[0];
    if (!file) return;
    isSavingIocs = true;
    try {
      const text = await file.text();
      const imported = parseIocCsvText(text);
      if (!imported.length) {
        iocError = 'No IOC entries found in selected file.';
        return;
      }
      await backend.saveIocs({
        projectId: projectDetail.project.meta.id,
        entries: imported
      });
      dispatch('notify', { message: 'Imported IOC rules.', tone: 'success' });
      closeIocManager();
      dispatch('refresh');
    } catch (error) {
      console.error(error);
      iocError =
        error instanceof Error ? error.message : 'Failed to import IOC rules.';
    } finally {
      if (iocImportInput) {
        iocImportInput.value = '';
      }
      isSavingIocs = false;
    }
  };

  const setFlag = async (row: CachedRow, flag: FlagSymbol) => {
    const currentFlag = normalizeFlag(row.flag);
    const nextFlag = currentFlag === flag ? '' : flag;
    try {
      await backend.updateFlag({
        projectId: projectDetail.project.meta.id,
        rowIndex: row.row_index,
        flag: nextFlag ?? '',
        memo: row.memo && row.memo.trim().length ? row.memo : null
      });
      await forceRefreshFilteredRows(false);
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to update flag.', tone: 'error' });
    }
  };

  const editMemo = (row: CachedRow) => {
    openMemoEditor(row);
  };

  const toggleColumn = async (column: string) => {
    const nextHidden = new Set($hiddenColumns);
    if (nextHidden.has(column)) {
      nextHidden.delete(column);
    } else {
      nextHidden.add(column);
    }
    hiddenColumns.set(nextHidden);
    initializeColumnWidths();
    const updatedColumns = getSearchableColumns();
    const filters: AppliedFilters = {
      search: $search.trim(),
      flag: $flagFilter,
      columns: updatedColumns,
    };
    lastSearchValue = filters.search;
    lastFlagFilter = filters.flag;
    lastColumnsSignature = updatedColumns.join('|');
    void applyFilters(filters, false, true);
    isUpdatingColumns.set(true);
    try {
      await backend.setHiddenColumns({
        projectId: projectDetail.project.meta.id,
        hiddenColumns: Array.from(nextHidden)
      });
      dispatch('notify', {
        message: `${nextHidden.has(column) ? 'Hid' : 'Showing'} column ${column}`,
        tone: 'success'
      });
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to update column visibility.', tone: 'error' });
      dispatch('refresh');
    } finally {
      isUpdatingColumns.set(false);
    }
  };

  const exportProject = async () => {
    isExporting.set(true);
    try {
      let destination: string | undefined;
      if (backend.isNative) {
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
        destination = selected;
      }
      await backend.exportProject({
        projectId: projectDetail.project.meta.id,
        destination
      });
      dispatch('notify', { message: 'Exported CSV with flags and memos.', tone: 'success' });
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to export CSV.', tone: 'error' });
    } finally {
      isExporting.set(false);
    }
  };

  const handleScroll = (event: Event) => {
    const target = event.currentTarget as HTMLDivElement;
    scrollTop.set(target.scrollTop);
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

  const getSearchableColumns = () =>
    projectDetail ? projectDetail.columns.filter((column) => !$hiddenColumns.has(column)) : [];

  const scheduleFilterRefresh = (
    searchValue: string,
    flagValue: FlagFilterValue,
    columns: string[],
    resetScroll: boolean
  ) => {
    pendingFilters = { search: searchValue, flag: flagValue, columns: [...columns], resetScroll };
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
          columns: getSearchableColumns(),
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
        columns: getSearchableColumns(),
      },
      resetScroll,
      true
    );
  };

  onMount(() => {
    const handleClickOutside = (event: MouseEvent) => {
      const target = event.target as Node;
      if (columnsOpen && columnPickerEl && !columnPickerEl.contains(target)) {
        columnsOpen = false;
      }
      if (flagMenuOpen && flagPickerEl && !flagPickerEl.contains(target)) {
        flagMenuOpen = false;
      }
    };
    const handleKeydown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        columnsOpen = false;
        flagMenuOpen = false;
        if ($iocManagerOpen) {
          closeIocManager();
        } else if ($memoEditor) {
          memoEditor.set(null);
          memoDraft = '';
          memoError = null;
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
        columns: [...activeFilters.columns],
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
    dispatch('summary', { flagged: $flaggedCount, hiddenColumns: hidden });
  };
</script>

<section class="project-view">
  <FilterControls on:iocManagerOpen={openIocManager} on:export={exportProject} />

  <ExpandedCellDialog on:notify={(e) => dispatch('notify', e.detail)} />

  <MemoEditorDialog on:notify={(e) => dispatch('notify', e.detail)} on:refresh={() => forceRefreshFilteredRows(false)} />

  <IocManagerDialog on:notify={(e) => dispatch('notify', e.detail)} on:refresh={() => dispatch('refresh')} />

  <DataTable {columnWidths} />
</section>
