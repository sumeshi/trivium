<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { open, save } from '@tauri-apps/api/dialog';
  import type { Backend } from '../backend';
  import type { IocEntry, LoadProjectResponse, ProjectRow } from '../types';
  import './project-view.css';

  export let projectDetail: LoadProjectResponse;
  export let backend: Backend;

  const dispatch = createEventDispatcher<{
    refresh: void;
    notify: { message: string; tone: 'success' | 'error' };
    summary: { flagged: number; hiddenColumns: string[] };
  }>();

  const FLAG_OPTIONS = [
    { value: 'safe', label: 'Safe', hint: '✓', tone: 'safe' },
    { value: 'suspicious', label: 'Suspicious', hint: '?', tone: 'suspicious' },
    { value: 'critical', label: 'Critical', hint: '!', tone: 'critical' }
  ] as const;
  type FlagSymbol = (typeof FLAG_OPTIONS)[number]['value'];
  type FlagFilterValue = FlagSymbol | 'all' | 'none' | 'priority';
  const PRIORITY_FLAGS: FlagSymbol[] = ['suspicious', 'critical'];
  const FLAG_FILTER_OPTIONS: Array<{ value: FlagFilterValue; label: string; hint?: string }> = [
    { value: 'all', label: 'All' },
    { value: 'safe', label: 'Safe', hint: '✓' },
    { value: 'suspicious', label: 'Suspicious', hint: '?' },
    { value: 'critical', label: 'Critical', hint: '!' },
    { value: 'priority', label: 'Sus + Crit', hint: '!?' },
    { value: 'none', label: 'Unflagged', hint: '–' }
  ];
  const ROW_HEIGHT = 56;
  const BUFFER = 8;
  const INDEX_COL_WIDTH = 80;
  const FLAG_COL_WIDTH = 130;
  const MEMO_COL_WIDTH = 200;
  const MIN_DATA_WIDTH = 80;
  const WIDTH_LIMIT_CHARS = 100;
  const CHAR_PIXEL = 9;
  const COLUMN_PADDING = 32;
  const MAX_DATA_WIDTH = WIDTH_LIMIT_CHARS * CHAR_PIXEL + COLUMN_PADDING;
  const STICKY_COLUMNS_WIDTH = INDEX_COL_WIDTH + FLAG_COL_WIDTH + MEMO_COL_WIDTH;

  const PAGE_SIZE = 250;
  const PREFETCH_PAGES = 1;

  let currentProjectId: string | null = null;
  let lastHiddenColumnsRef: string[] | null = null;
  type CachedRow = ProjectRow & {
    memo: string;
    displayCache: Record<string, string>;
  };
  type VirtualRow = { position: number; row: CachedRow | null };

  let hiddenColumns = new Set<string>();
  let columnsOpen = false;
  let columnPickerEl: HTMLDivElement | null = null;

  let search = '';
  let flagFilter: FlagFilterValue = 'all';

  let sortKey: string | null = null;
  let sortDirection: 'asc' | 'desc' = 'asc';

  let isExporting = false;
  let isUpdatingColumns = false;

  let visibleColumns: string[] = [];
  let rowsCache: Map<number, CachedRow> = new Map();
  let pendingPages: Set<number> = new Set();
  let loadedPages: Set<number> = new Set();
  let totalRows = 0;
  let totalFlagged = 0;
  let flaggedCount = 0;
  let lastSummaryFlagged = -1;
  let lastSummaryHiddenKey = '';

  let virtualRows: VirtualRow[] = [];
  let expandedCell: { column: string; value: string } | null = null;

  let columnWidths: Map<string, number> = new Map();
  let firstDataColumn: string | null = null;
  let stickyFlagOffset = 0;
  let stickyMemoOffset = 0;
  let stickyDataOffset = 0;
  let stickyVariables = '';

  let bodyScrollEl: HTMLDivElement | null = null;
  let headerScrollEl: HTMLDivElement | null = null;
  let flagPickerEl: HTMLDivElement | null = null;
  let viewportHeight = 0;
  let scrollTop = 0;
  let initialized = false;
  let columnsKey = '';
  let tableWidth = 0;
  let resizeObserver: ResizeObserver | null = null;
  let observedScrollEl: HTMLDivElement | null = null;
  let baseDataWidths: number[] = [];
  let availableDataWidth = 0;
  let distributedDataWidths: number[] = [];
  let flagMenuOpen = false;
  let visibleCount = 0;
  let maxStart = 0;
  let startIndex = 0;
  let endIndex = 0;
  let offsetY = 0;
  let totalHeight = 0;
  let loadedRowCount = 0;

  type AppliedFilters = {
    search: string;
    flag: FlagFilterValue;
    columns: string[];
  };

  type ScheduledFilters = AppliedFilters & { resetScroll: boolean };

  let activeFilters: AppliedFilters = { search: '', flag: 'all', columns: [] };
  let filterTimeout: ReturnType<typeof setTimeout> | null = null;
  let pendingFilters: ScheduledFilters | null = null;
  let filterRequestId = 0;
  let lastAppliedFilters = '';
  let lastSearchValue: string | null = null;
  let lastFlagFilter: FlagFilterValue | null = null;
  let lastColumnsSignature: string | null = null;
  let memoEditor: { row: CachedRow } | null = null;
  let memoDraft = '';
  let memoSaving = false;
  let memoError: string | null = null;
  let releaseHeaderSyncFrame: number | null = null;
  let releaseBodySyncFrame: number | null = null;
  let isSyncingHeaderScroll = false;
  let isSyncingBodyScroll = false;
  let iocManagerOpen = false;
  let iocDraft: IocEntry[] = [];
  let iocError: string | null = null;
  let isSavingIocs = false;
  let iocImportInput: HTMLInputElement | null = null;

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
    `${filters.search}::${filters.flag}::${filters.columns.join('|')}::${sortKey ?? ''}::${sortDirection}`;

  const resetPaginationState = () => {
    rowsCache = new Map();
    pendingPages = new Set();
    loadedPages = new Set();
    totalRows = 0;
    totalFlagged = 0;
    flaggedCount = 0;
    virtualRows = [];
  };

  const initializeColumnWidths = () => {
    if (!projectDetail) {
      columnWidths = new Map();
      return;
    }
    const next = new Map<string, number>();
    for (const column of projectDetail.columns) {
      const headerChars = Math.min(column.length, WIDTH_LIMIT_CHARS);
      const dataChars = Math.min(projectDetail.column_max_chars[column] ?? headerChars, WIDTH_LIMIT_CHARS);
      const maxChars = Math.max(headerChars, dataChars);
      const estimated = Math.round(maxChars * CHAR_PIXEL + COLUMN_PADDING);
      const width = Math.min(Math.max(estimated, MIN_DATA_WIDTH), MAX_DATA_WIDTH);
      next.set(column, width);
    }
    columnWidths = next;
  };

  const applyProjectDetail = (detail: LoadProjectResponse, resetScroll: boolean) => {
    currentProjectId = detail.project.meta.id;
    hiddenColumns = new Set(detail.hidden_columns ?? []);
    iocDraft = detail.iocs.map((entry) => ({
      flag: normalizeIocFlag(entry.flag),
      tag: entry.tag,
      query: entry.query
    }));
    initializeColumnWidths();
    const currentColumns = detail.columns.filter((column) => !hiddenColumns.has(column));
    const filters: AppliedFilters = {
      search: search.trim(),
      flag: flagFilter,
      columns: currentColumns,
    };
    lastHiddenColumnsRef = [...(detail.hidden_columns ?? [])];
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
    sortKey = null;
    sortDirection = 'asc';
    expandedCell = null;

    const seededRows = detail.initial_rows?.map((row) => normalizeRow(row)) ?? [];
    console.log('[debug] applyProjectDetail', {
      projectId: detail.project.meta.id,
      seededRows: seededRows.length,
      totalRecords: detail.project.meta.total_records,
      columns: detail.columns.length,
    });
    const seededCache = new Map<number, CachedRow>();
    for (const row of seededRows) {
      seededCache.set(row.row_index, row);
    }
    rowsCache = seededCache;
    if (seededRows.length > 0) {
      updateColumnWidthsFromRows(seededRows);
    }
    loadedPages = seededRows.length > 0 ? new Set([0]) : new Set();
    pendingPages = new Set();
    totalRows = detail.project.meta.total_records ?? seededRows.length;
    totalFlagged = detail.project.flagged_records;
    flaggedCount = totalFlagged;
    scrollTop = 0;
    if (bodyScrollEl) {
      bodyScrollEl.scrollTop = 0;
      bodyScrollEl.scrollLeft = 0;
    }
    if (headerScrollEl) {
      headerScrollEl.scrollLeft = 0;
    }
    initialized = true;
    if (seededRows.length === 0) {
      console.log('[debug] applyProjectDetail request first page');
      void requestPage(0, true);
    }
  };

  $: if (projectDetail) {
    const projectChanged = projectDetail.project.meta.id !== currentProjectId;
    const nextHiddenColumns = projectDetail.hidden_columns ?? [];
    const hiddenChanged =
      lastHiddenColumnsRef === null ||
      !areStringsEqual(nextHiddenColumns, lastHiddenColumnsRef);
    if (!initialized || projectChanged) {
      applyProjectDetail(projectDetail, true);
    } else if (hiddenChanged) {
      hiddenColumns = new Set(nextHiddenColumns);
      initializeColumnWidths();
      const columns = getSearchableColumns();
      const filters: AppliedFilters = {
        search: search.trim(),
        flag: flagFilter,
        columns,
      };
      lastSearchValue = filters.search;
      lastFlagFilter = filters.flag;
      lastColumnsSignature = columns.join('|');
      void applyFilters(filters, false, true);
    }
    lastHiddenColumnsRef = [...nextHiddenColumns];
  }

  $: visibleColumns = projectDetail
    ? projectDetail.columns.filter((column) => !hiddenColumns.has(column))
    : [];

  $: columnsKey = visibleColumns.join('|');

  $: if (initialized) {
    const searchValue = search.trim();
    const flagValue = flagFilter;
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
    Math.ceil((viewportHeight || ROW_HEIGHT) / ROW_HEIGHT) + BUFFER * 2;
  $: maxStart = Math.max(0, totalRows - visibleCount);
  $: startIndex = Math.min(
    maxStart,
    Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - BUFFER)
  );
  $: endIndex = Math.min(totalRows, startIndex + visibleCount);
  $: ensureRangeLoaded(startIndex, endIndex);
  $: virtualRows = buildVirtualRows(startIndex, endIndex, rowsCache);
  $: offsetY = startIndex * ROW_HEIGHT;
  $: totalHeight = totalRows * ROW_HEIGHT;
  $: loadedRowCount = rowsCache.size;

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
    for (const column of visibleColumns) {
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
    const maxPageIndex = Math.max(0, Math.floor(Math.max(totalRows - 1, 0) / PAGE_SIZE));
    const clampedLastPage = Math.min(lastPage, maxPageIndex);
    console.log('[debug] ensureRangeLoaded', {
      start,
      end,
      firstPage,
      lastPage: clampedLastPage,
      loadedPages: Array.from(loadedPages.values()),
      pendingPages: Array.from(pendingPages.values()),
    });
    for (let page = firstPage; page <= clampedLastPage; page += 1) {
      if (!loadedPages.has(page) && !pendingPages.has(page)) {
        void requestPage(page);
      }
    }
  };

  const requestPage = (pageIndex: number, force = false): Promise<void> => {
    if (!projectDetail) return Promise.resolve();
    const normalizedPage = Math.max(0, pageIndex);
    if (!force && loadedPages.has(normalizedPage)) {
      return Promise.resolve();
    }
    if (!force && pendingPages.has(normalizedPage)) {
      return Promise.resolve();
    }
    const offset = normalizedPage * PAGE_SIZE;
    const limit = PAGE_SIZE;
    const requestId = filterRequestId;
    const filters = activeFilters;
    const nextPending = new Set(pendingPages);
    nextPending.add(normalizedPage);
    pendingPages = nextPending;

    const payload = {
      projectId: projectDetail.project.meta.id,
      search: filters.search.length > 0 ? filters.search : undefined,
      flagFilter:
        filters.flag === 'all' && filters.search.length === 0 ? undefined : filters.flag,
      columns:
        filters.columns.length === projectDetail.columns.length
          ? undefined
          : [...filters.columns],
      offset,
      limit,
      sortKey: sortKey ?? undefined,
      sortDirection,
    };

    console.log('[debug] requestPage', {
      page: normalizedPage,
      offset,
      limit,
      filters,
      sortKey,
      sortDirection,
    });
    return backend
      .queryProjectRows(payload)
      .then((response) => {
        const nextPendingPages = new Set(pendingPages);
        nextPendingPages.delete(normalizedPage);
        pendingPages = nextPendingPages;
        if (requestId !== filterRequestId) {
          return;
        }
        totalRows = response.total_rows;
        totalFlagged = response.total_flagged;
        flaggedCount = totalFlagged;
        emitSummary();
        const normalizedRows = response.rows.map((row) => normalizeRow(row));
        console.log('[debug] requestPage response', {
          page: normalizedPage,
          received: normalizedRows.length,
          totalRows: response.total_rows,
          offset: response.offset,
        });
        updateColumnWidthsFromRows(normalizedRows);
        const nextCache = new Map(rowsCache);
        normalizedRows.forEach((row, index) => {
          nextCache.set(response.offset + index, row);
        });
        rowsCache = nextCache;
        const nextLoaded = new Set(loadedPages);
        nextLoaded.add(Math.floor(response.offset / PAGE_SIZE));
        loadedPages = nextLoaded;
      })
      .catch((error) => {
        console.error(error);
        const nextPendingPages = new Set(pendingPages);
        nextPendingPages.delete(normalizedPage);
        pendingPages = nextPendingPages;
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
      scrollTop = 0;
      if (bodyScrollEl) {
        bodyScrollEl.scrollTop = 0;
        bodyScrollEl.scrollLeft = 0;
      }
      if (headerScrollEl) {
        headerScrollEl.scrollLeft = 0;
      }
    }
    lastSummaryFlagged = -1;
    lastSummaryHiddenKey = '';
    const targetRow = resetScroll ? 0 : Math.max(0, Math.floor(scrollTop / ROW_HEIGHT));
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

  $: baseDataWidths = visibleColumns.map((column) => resolveColumnWidth(column));
  $: availableDataWidth = Math.max(0, tableWidth - STICKY_COLUMNS_WIDTH);
  $: distributedDataWidths = expandColumnWidths(baseDataWidths, availableDataWidth);
  $: totalDataWidth = distributedDataWidths.reduce((sum, width) => sum + width, 0);
  $: totalTableWidth = STICKY_COLUMNS_WIDTH + totalDataWidth;
  $: effectiveTableWidth = Math.max(totalTableWidth, tableWidth);
  $: gridTemplate = [
    `${INDEX_COL_WIDTH}px`,
    `${FLAG_COL_WIDTH}px`,
    `${MEMO_COL_WIDTH}px`,
    ...distributedDataWidths.map((width) => `${width}px`)
  ].join(' ');
  $: if (resizeObserver) {
    if (bodyScrollEl && bodyScrollEl !== observedScrollEl) {
      if (observedScrollEl) {
        resizeObserver.unobserve(observedScrollEl);
      }
      resizeObserver.observe(bodyScrollEl);
      observedScrollEl = bodyScrollEl;
      tableWidth = bodyScrollEl.clientWidth;
    } else if (!bodyScrollEl && observedScrollEl) {
      resizeObserver.unobserve(observedScrollEl);
      observedScrollEl = null;
      tableWidth = 0;
    }
  }

  $: firstDataColumn = visibleColumns.length > 0 ? visibleColumns[0] : null;
  $: stickyFlagOffset = INDEX_COL_WIDTH;
  $: stickyMemoOffset = INDEX_COL_WIDTH + FLAG_COL_WIDTH;
  $: stickyDataOffset = stickyMemoOffset + MEMO_COL_WIDTH;
  $: stickyVariables = `--sticky-flag:${stickyFlagOffset}px; --sticky-memo:${stickyMemoOffset}px; --sticky-data:${stickyDataOffset}px;`;

  const normalizeFlag = (flag: string | null | undefined): FlagSymbol | '' => {
    if (!flag) return '';
    const lower = flag.trim().toLowerCase();
    if (lower === 'safe' || lower === 'suspicious' || lower === 'critical') {
      return lower;
    }
    return '';
  };

  const mapStoredFlag = (flag: string | null | undefined): FlagSymbol | '' => {
    const normalized = normalizeFlag(flag);
    if (normalized) return normalized;
    const trimmed = flag?.trim();
    if (!trimmed) return '';
    if (trimmed === '◯') return 'safe';
    if (trimmed === '?') return 'suspicious';
    if (trimmed === '✗') return 'critical';
    return '';
  };

  const normalizeRow = (incoming: ProjectRow): CachedRow => {
    const displayCache: Record<string, string> = {};
    for (const [column, value] of Object.entries(incoming.data)) {
      const formatted = formatCell(value);
      displayCache[column] = formatted;
    }
    return {
      ...incoming,
      flag: mapStoredFlag(incoming.flag) || '',
      memo: sanitizeMemoInput(incoming.memo ?? ''),
      displayCache
    };
  };

  const formatCell = (value: unknown): string => {
    if (value === null || value === undefined) {
      return '';
    }
    if (typeof value === 'object') {
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    }
    return String(value);
  };

const sanitizeMemoInput = (value: string): string => {
  const withoutControl = value.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '');
  return withoutControl.replace(/<[^>]*>/g, '');
};

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
    flagFilter = value;
    flagMenuOpen = false;
  };

  const toggleSort = (column: string) => {
    if (sortKey === column) {
      if (sortDirection === 'asc') {
        sortDirection = 'desc';
      } else {
        sortKey = null;
        sortDirection = 'asc';
      }
    } else {
      sortKey = column;
      sortDirection = 'asc';
    }
    const filters: AppliedFilters = {
      search: activeFilters.search,
      flag: activeFilters.flag,
      columns: [...activeFilters.columns],
    };
    void applyFilters(filters, false, true);
  };

  const openCell = (column: string, value: string) => {
    expandedCell = { column, value };
  };

  const closeCell = () => {
    expandedCell = null;
  };

  const openMemoEditor = (row: CachedRow) => {
    memoEditor = { row };
    memoDraft = row.memo ?? '';
    memoError = null;
    memoSaving = false;
  };

  const closeMemoEditor = () => {
    memoEditor = null;
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
    if (!memoEditor || memoSaving || !projectDetail) return;
    const sanitized = sanitizeMemoInput(memoDraft).trim();
    memoSaving = true;
    memoError = null;
    try {
      await backend.updateFlag({
        projectId: projectDetail.project.meta.id,
        rowIndex: memoEditor.row.row_index,
        flag: memoEditor.row.flag,
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
    if (!expandedCell) return;
    const text = expandedCell.value ?? '';
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

  const normalizeIocFlag = (value: string): FlagSymbol => {
    const lowered = value.trim().toLowerCase();
    if (lowered === 'critical' || lowered === 'suspicious' || lowered === 'safe') {
      return lowered as FlagSymbol;
    }
    return 'safe';
  };

  const openIocManager = () => {
    if (!projectDetail) return;
    iocDraft = projectDetail.iocs.map((entry) => ({
      flag: normalizeIocFlag(entry.flag),
      tag: entry.tag,
      query: entry.query
    }));
    iocError = null;
    isSavingIocs = false;
    iocManagerOpen = true;
  };

  const closeIocManager = () => {
    iocManagerOpen = false;
    iocError = null;
    isSavingIocs = false;
  };

  const addIocEntry = () => {
    iocDraft = [...iocDraft, { flag: 'critical', tag: '', query: '' }];
  };

  const updateIocEntry = (index: number, field: keyof IocEntry, value: string) => {
    iocDraft = iocDraft.map((entry, current) => {
      if (current !== index) return entry;
      if (field === 'flag') {
        return { ...entry, flag: normalizeIocFlag(value) };
      }
      return { ...entry, [field]: value };
    });
  };

  const removeIocEntry = (index: number) => {
    iocDraft = iocDraft.filter((_, current) => current !== index);
  };

  const handleIocFieldChange = (index: number, field: keyof IocEntry, event: Event) => {
    const target = event.currentTarget as HTMLInputElement | HTMLSelectElement;
    updateIocEntry(index, field, target.value);
  };

  const sanitizeIocEntries = (): IocEntry[] =>
    iocDraft
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
      // const flagLabel = FLAG_OPTIONS.find((option) => option.value === flag)?.label ?? flag;
      // dispatch('notify', {
      //   message: nextFlag
      //     ? `Marked row ${row.row_index + 1} as ${flagLabel}`
      //     : 'Cleared flag',
      //   tone: 'success'
      // });
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to update flag.', tone: 'error' });
    }
  };

  const editMemo = (row: CachedRow) => {
    openMemoEditor(row);
  };

  const toggleColumn = async (column: string) => {
    const nextHidden = new Set(hiddenColumns);
    if (nextHidden.has(column)) {
      nextHidden.delete(column);
    } else {
      nextHidden.add(column);
    }
    hiddenColumns = nextHidden;
    initializeColumnWidths();
    const updatedColumns = getSearchableColumns();
    const filters: AppliedFilters = {
      search: search.trim(),
      flag: flagFilter,
      columns: updatedColumns,
    };
    lastSearchValue = filters.search;
    lastFlagFilter = filters.flag;
    lastColumnsSignature = updatedColumns.join('|');
    void applyFilters(filters, false, true);
    isUpdatingColumns = true;
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
      isUpdatingColumns = false;
    }
  };

  const exportProject = async () => {
    isExporting = true;
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
      isExporting = false;
    }
  };

  const handleScroll = (event: Event) => {
    const target = event.currentTarget as HTMLDivElement;
    scrollTop = target.scrollTop;
    if (isSyncingBodyScroll) {
      isSyncingBodyScroll = false;
      return;
    }
    if (headerScrollEl && headerScrollEl.scrollLeft !== target.scrollLeft) {
      if (releaseHeaderSyncFrame !== null) {
        cancelAnimationFrame(releaseHeaderSyncFrame);
      }
      isSyncingHeaderScroll = true;
      headerScrollEl.scrollLeft = target.scrollLeft;
      releaseHeaderSyncFrame = requestAnimationFrame(() => {
        isSyncingHeaderScroll = false;
        releaseHeaderSyncFrame = null;
      });
    }
  };

  const handleHeaderScroll = () => {
    if (!headerScrollEl || !bodyScrollEl) return;
    if (isSyncingHeaderScroll) {
      isSyncingHeaderScroll = false;
      return;
    }
    const nextLeft = headerScrollEl.scrollLeft;
    if (bodyScrollEl.scrollLeft !== nextLeft) {
      if (releaseBodySyncFrame !== null) {
        cancelAnimationFrame(releaseBodySyncFrame);
      }
      isSyncingBodyScroll = true;
      bodyScrollEl.scrollLeft = nextLeft;
      releaseBodySyncFrame = requestAnimationFrame(() => {
        isSyncingBodyScroll = false;
        releaseBodySyncFrame = null;
      });
    }
  };

  const forwardWheel = (event: WheelEvent) => {
    if (!bodyScrollEl) return;
    if (event.currentTarget === bodyScrollEl) {
      return;
    }
    let handled = false;
    if (event.deltaY !== 0) {
      const previousTop = bodyScrollEl.scrollTop;
      bodyScrollEl.scrollTop += event.deltaY;
      handled = handled || bodyScrollEl.scrollTop !== previousTop;
    }
    if (event.deltaX !== 0) {
      const previousLeft = bodyScrollEl.scrollLeft;
      bodyScrollEl.scrollLeft += event.deltaX;
      handled = handled || bodyScrollEl.scrollLeft !== previousLeft;
    }
    if (handled) {
      event.preventDefault();
    }
  };

  const getSearchableColumns = () =>
    projectDetail ? projectDetail.columns.filter((column) => !hiddenColumns.has(column)) : [];

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
        search: search.trim(),
        flag: flagFilter,
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
        if (iocManagerOpen) {
          closeIocManager();
        } else if (memoEditor) {
          memoEditor = null;
          memoDraft = '';
          memoError = null;
        } else {
          closeCell();
        }
      }
    };
    resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        tableWidth = entry.contentRect.width;
      }
    });
    if (bodyScrollEl) {
      tableWidth = bodyScrollEl.clientWidth;
    }
    document.addEventListener('mousedown', handleClickOutside);
    window.addEventListener('keydown', handleKeydown);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      window.removeEventListener('keydown', handleKeydown);
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
    const hidden = Array.from(hiddenColumns);
    const hiddenKey = hidden.join('|');
    if (lastSummaryFlagged === flaggedCount && lastSummaryHiddenKey === hiddenKey) {
      return;
    }
    lastSummaryFlagged = flaggedCount;
    lastSummaryHiddenKey = hiddenKey;
    dispatch('summary', { flagged: flaggedCount, hiddenColumns: hidden });
  };
</script>

<section class="project-view">
  <section class="filters">
    <div class="filter-flag">
      <div class="flag-picker" bind:this={flagPickerEl}>
        <button
          type="button"
          class="flag-trigger"
          class:open={flagMenuOpen}
          on:click={toggleFlagMenu}
          aria-haspopup="listbox"
          aria-expanded={flagMenuOpen}
        >
          {#if getFlagFilterDetails(flagFilter).hint}
            <span class="flag-hint">{getFlagFilterDetails(flagFilter).hint}</span>
          {/if}
          <span class="flag-label">{getFlagFilterDetails(flagFilter).label}</span>
        </button>
        {#if flagMenuOpen}
          <div class="flag-menu" role="listbox">
            {#each FLAG_FILTER_OPTIONS as option}
              <button
                type="button"
                class="flag-option"
                class:active={flagFilter === option.value}
                on:click={() => selectFlagFilter(option.value)}
                role="option"
                aria-selected={flagFilter === option.value}
              >
                {#if option.hint}
                  <span class="flag-hint">{option.hint}</span>
                {/if}
                <span>{option.label}</span>
                {#if flagFilter === option.value}
                  <span class="flag-dot" aria-hidden="true">●</span>
                {/if}
              </button>
            {/each}
          </div>
        {/if}
      </div>
    </div>
    <label class="filter-search">
      <input
        placeholder="Enter search text"
        bind:value={search}
        type="search"
      />
    </label>
    <div class="filter-columns">
      <div class="column-picker" bind:this={columnPickerEl}>
        <button
          type="button"
          class="column-trigger"
          on:click={() => {
            flagMenuOpen = false;
            columnsOpen = !columnsOpen;
          }}
          aria-expanded={columnsOpen}
        >
          Columns ({visibleColumns.length}/{projectDetail.columns.length})
        </button>
        {#if columnsOpen}
          <div class="column-panel">
            <ul>
              {#each projectDetail.columns as column}
                <li>
                  <label>
                    <input
                      type="checkbox"
                      checked={!hiddenColumns.has(column)}
                      disabled={isUpdatingColumns}
                      on:change={() => toggleColumn(column)}
                    />
                    <span>{column}</span>
                  </label>
                </li>
              {/each}
            </ul>
            <button type="button" class="close-panel" on:click={() => (columnsOpen = false)}>
              Done
            </button>
          </div>
        {/if}
      </div>
    </div>
    <div class="filter-ioc">
      <button type="button" class="ghost" on:click={openIocManager}>
        IOC Rules
      </button>
    </div>
    <div class="filter-export">
      <button class="primary" on:click={exportProject} disabled={isExporting}>
        {isExporting ? 'Exporting…' : 'Export CSV'}
      </button>
    </div>
  </section>

  {#if expandedCell}
    <div
      class="cell-dialog-backdrop"
      role="button"
      tabindex="0"
      on:click={handleBackdropClick}
      on:keydown={handleBackdropKey}
    >
      <div
        class="cell-dialog"
        role="dialog"
        aria-modal="true"
        aria-label={`Cell value for ${expandedCell.column}`}
      >
        <div class="cell-dialog-header">
          <h3>{expandedCell.column}</h3>
          <div class="cell-dialog-actions">
            <button type="button" class="ghost" on:click={copyExpandedCell}>Copy</button>
            <button type="button" class="ghost close-dialog" on:click={closeCell}>Close</button>
          </div>
        </div>
        <pre class="cell-dialog-body">{expandedCell.value || '—'}</pre>
      </div>
    </div>
  {/if}

  {#if memoEditor}
    <!-- svelte-ignore a11y-click-events-have-key-events -->
    <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
    <div
      class="cell-dialog-backdrop"
      role="dialog"
      aria-modal="true"
      aria-label={`Edit memo for row ${memoEditor.row.row_index + 1}`}
      on:click={handleMemoBackdropClick}
      tabindex="-1"
      on:keydown={handleMemoBackdropKey}
    >
      <div class="cell-dialog memo-dialog">
        <div class="cell-dialog-header">
          <h3>Edit memo</h3>
          <div class="cell-dialog-actions">
            <button type="button" class="ghost" on:click={closeMemoEditor} disabled={memoSaving}>
              Cancel
            </button>
            <button
              type="button"
              class="primary"
              on:click={saveMemo}
              disabled={memoSaving}
            >
              {memoSaving ? 'Saving…' : 'Save'}
            </button>
          </div>
        </div>
        <label class="memo-editor-label">
          <span>Memo</span>
          <textarea
            bind:value={memoDraft}
            rows="8"
            placeholder="Add memo"
            spellcheck="true"
            disabled={memoSaving}
          />
        </label>
        {#if memoError}
          <p class="memo-error">{memoError}</p>
        {/if}
      </div>
    </div>
  {/if}

  {#if iocManagerOpen}
    <div class="cell-dialog-backdrop" role="dialog" aria-modal="true" aria-label="IOC rules">
      <div class="cell-dialog ioc-dialog">
        <div class="cell-dialog-header">
          <h3>IOC Rules</h3>
          <div class="cell-dialog-actions">
            <button type="button" class="ghost" on:click={closeIocManager} disabled={isSavingIocs}>
              Close
            </button>
          </div>
        </div>
        <div class="ioc-controls">
          <button type="button" class="ghost" on:click={addIocEntry}>
            Add rule
          </button>
          <div class="ioc-spacer" />
          <button type="button" class="ghost" on:click={importIocEntries} disabled={isSavingIocs}>
            Import…
          </button>
          <button type="button" class="ghost" on:click={exportIocEntries} disabled={isSavingIocs}>
            Export…
          </button>
        </div>
        <div class="ioc-table">
          <div class="ioc-header">
            <span>Flag</span>
            <span>Tag</span>
            <span>Query</span>
            <span></span>
          </div>
          {#if iocDraft.length === 0}
            <p class="ioc-empty">No IOC rules configured.</p>
          {:else}
            {#each iocDraft as entry, index}
              <div class="ioc-row">
                <select
                  value={entry.flag}
                  on:change={(event) => handleIocFieldChange(index, 'flag', event)}
                  disabled={isSavingIocs}
                >
                  {#each FLAG_OPTIONS as option}
                    <option value={option.value}>{option.label}</option>
                  {/each}
                </select>
                <input
                  value={entry.tag}
                  placeholder="Tag name"
                  on:input={(event) => handleIocFieldChange(index, 'tag', event)}
                  disabled={isSavingIocs}
                />
                <input
                  value={entry.query}
                  placeholder="Search string"
                  on:input={(event) => handleIocFieldChange(index, 'query', event)}
                  disabled={isSavingIocs}
                />
                <button
                  type="button"
                  class="ghost danger"
                  on:click={() => removeIocEntry(index)}
                  disabled={isSavingIocs}
                >
                  Remove
                </button>
              </div>
            {/each}
          {/if}
        </div>
        {#if iocError}
          <p class="memo-error">{iocError}</p>
        {/if}
        <div class="ioc-footer">
          <button type="button" class="ghost" on:click={closeIocManager} disabled={isSavingIocs}>
            Cancel
          </button>
          <button type="button" class="primary" on:click={saveIocEntries} disabled={isSavingIocs}>
            {isSavingIocs ? 'Saving…' : 'Save'}
          </button>
        </div>
        {#if !backend.isNative}
          <input
            type="file"
            accept=".csv"
            class="hidden-input"
            bind:this={iocImportInput}
            on:change={handleIocFileUpload}
          />
        {/if}
      </div>
    </div>
  {/if}

  <section class="table-wrapper">
    <div class="meta">
      <span>{loadedRowCount} / {totalRows} rows</span>
      <span>{flaggedCount} flagged</span>
    </div>
    {#if totalRows === 0}
      <div class="empty-rows">
        {pendingPages.size > 0 ? 'Loading rows…' : 'No rows match your filters.'}
      </div>
    {:else}
      <div class="table-scroll">
        <div
          class="table-header-scroll"
          bind:this={headerScrollEl}
          on:scroll={handleHeaderScroll}
          on:wheel={forwardWheel}
        >
          <div
            class="data-header"
            style={`grid-template-columns: ${gridTemplate}; ${stickyVariables} width: ${effectiveTableWidth}px;`}
          >
            <div class="header-cell sticky sticky-index">#</div>
            <div class="header-cell sticky sticky-flag">Flag</div>
            <div class="header-cell sticky sticky-memo">Memo</div>
            {#each visibleColumns as column, columnIndex}
              <button
                type="button"
                class="header-cell"
                class:sticky={columnIndex === 0}
                class:stickyData={columnIndex === 0}
                class:sorted={sortKey === column}
                on:click={() => toggleSort(column)}
                aria-pressed={sortKey === column}
              >
                <span class="header-label">{column}</span>
                {#if sortKey === column}
                  <span class="sort-indicator" aria-hidden="true">
                    {sortDirection === 'asc' ? '▲' : '▼'}
                  </span>
                {/if}
              </button>
            {/each}
          </div>
        </div>
        <div
          class="virtual-viewport"
          bind:this={bodyScrollEl}
          bind:clientHeight={viewportHeight}
          on:scroll={handleScroll}
          on:wheel={forwardWheel}
          style={stickyVariables}
        >
          <div class="virtual-spacer" style={`height: ${totalHeight}px; width: ${effectiveTableWidth}px;`}>
            <div class="virtual-inner" style={`transform: translateY(${offsetY}px);`}>
{#each virtualRows as item (item.position)}
                <div
                  class="data-row"
                  class:alt-row={item.position % 2 === 1}
                  class:loading={!item.row}
                  style={`grid-template-columns: ${gridTemplate}; ${stickyVariables}; --row-height: ${ROW_HEIGHT}px;`}
                >
                  <div class="cell index sticky sticky-index">{item.position + 1}</div>
                  <div class="cell flag sticky sticky-flag">
                    {#if hasRow(item.row)}
                      <div class="flag-buttons">
                        {#each FLAG_OPTIONS as option}
                          <button
                            type="button"
                            class:selected={item.row ? normalizeFlag(item.row.flag) === option.value : false}
                            class:flag-safe={option.value === 'safe'}
                            class:flag-suspicious={option.value === 'suspicious'}
                            class:flag-critical={option.value === 'critical'}
                            on:click={() => {
                              if (!item.row) return;
                              setFlag(item.row, option.value);
                            }}
                          >
                            {option.hint}
                          </button>
                        {/each}
                      </div>
                    {:else}
                      <div class="flag-buttons loading-placeholder">…</div>
                    {/if}
                  </div>
                  <button
                    class="cell memo-button sticky sticky-memo"
                    on:click={() => item.row && editMemo(item.row)}
                    title={
                      item.row
                        ? item.row.memo && item.row.memo.trim().length
                          ? item.row.memo
                          : 'Add memo'
                        : 'Loading…'
                    }
                    disabled={!item.row}
                  >
                    {#if item.row && item.row.memo && item.row.memo.trim().length}
                      <span class="memo-text">{item.row.memo}</span>
                    {:else if item.row}
                      <span class="memo-placeholder">Add memo</span>
                    {:else}
                      <span class="memo-placeholder">Loading…</span>
                    {/if}
                  </button>
                  {#each visibleColumns as column, columnIndex}
                    {@const cellValue = item.row ? item.row.displayCache[column] ?? '' : ''}
                    <button
                      type="button"
                      class="cell"
                      class:sticky={columnIndex === 0}
                      class:stickyData={columnIndex === 0}
                      title={item.row ? cellValue : 'Loading…'}
                      on:click={() => {
                        if (!item.row) return;
                        openCell(column, cellValue);
                      }}
                      on:keydown={(event) => {
                        if (!item.row) return;
                        handleCellKeydown(event, column, cellValue);
                      }}
                      disabled={!item.row}
                    >
                      <span class="cell-text">
                        {item.row ? (cellValue || '—') : 'Loading…'}
                      </span>
                    </button>
                  {/each}
                </div>
              {/each}
            </div>
          </div>
        </div>
      </div>
    {/if}
  </section>
</section>
