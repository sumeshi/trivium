<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { save } from '@tauri-apps/api/dialog';
  import type { Backend } from '../backend';
  import type { LoadProjectResponse, ProjectRow } from '../types';
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
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
  const INDEX_COL_WIDTH = 80;
  const FLAG_COL_WIDTH = 130;
  const MEMO_COL_WIDTH = 200;
  const MIN_DATA_WIDTH = 80;
  const WIDTH_LIMIT_CHARS = 100;
  const CHAR_PIXEL = 9;
  const COLUMN_PADDING = 32;
  const MAX_DATA_WIDTH = WIDTH_LIMIT_CHARS * CHAR_PIXEL + COLUMN_PADDING;
  const STICKY_COLUMNS_WIDTH = INDEX_COL_WIDTH + FLAG_COL_WIDTH + MEMO_COL_WIDTH;

  let currentProjectId: string | null = null;
  let lastProjectDetail: LoadProjectResponse | null = null;
  let lastRowsRef: ProjectRow[] | null = null;
  let lastHiddenColumnsRef: string[] | null = null;
  type CachedRow = ProjectRow & {
    memo: string;
    displayCache: Record<string, string>;
  };

  let rows: CachedRow[] = [];

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
  let filteredRows: CachedRow[] = [];
  let totalFlagged = 0;
  let flaggedCount = 0;
  let lastSummaryFlagged = -1;
  let lastSummaryHiddenKey = '';

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
  type ScheduledFilters = {
    search: string;
    flag: FlagFilterValue;
    columns: string[];
    resetScroll: boolean;
  };
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

  $: if (projectDetail) {
    const nextRowsRef = projectDetail.rows;
    const nextHiddenColumns = projectDetail.hidden_columns ?? [];
    const projectChanged = projectDetail.project.meta.id !== currentProjectId;
    const rowsChanged = nextRowsRef !== lastRowsRef;
    const shouldInitialize = !initialized || projectChanged || rowsChanged;

    if (shouldInitialize) {
      lastProjectDetail = projectDetail;
      currentProjectId = projectDetail.project.meta.id;
      rows = nextRowsRef.map((row) => normalizeRow(row));
      totalFlagged = projectDetail.project.flagged_records;
      lastAppliedFilters = '';
      lastSearchValue = null;
      lastFlagFilter = null;
      lastColumnsSignature = null;
      filterRequestId = 0;
      if (filterTimeout) {
        clearTimeout(filterTimeout);
        filterTimeout = null;
      }
      pendingFilters = null;
      hiddenColumns = new Set(nextHiddenColumns);
      lastSummaryFlagged = -1;
      lastSummaryHiddenKey = '';
      sortKey = null;
      sortDirection = 'asc';
      expandedCell = null;
      recomputeFilteredRows(true);
      memoEditor = null;
      memoDraft = '';
      memoError = null;
      initialized = true;
    } else if (nextHiddenColumns !== lastHiddenColumnsRef) {
      hiddenColumns = new Set(nextHiddenColumns);
      recomputeFilteredRows(false);
    }

    lastRowsRef = nextRowsRef;
    lastHiddenColumnsRef = nextHiddenColumns;
  }

  $: visibleColumns = projectDetail
    ? projectDetail.columns.filter((column) => !hiddenColumns.has(column))
    : [];

  $: columnsKey = visibleColumns.join('|');

  $: if (initialized) {
    columnsKey;
    sortKey;
    sortDirection;
    recomputeFilteredRows(false);
  }

  $: if (initialized) {
    const searchValue = search.trim();
    const flagValue = flagFilter;
    const columns = getSearchableColumns();
    const columnsSignature = columns.join('|');
    const searchChanged = lastSearchValue === null || searchValue !== lastSearchValue;
    const flagChanged = lastFlagFilter === null || flagValue !== lastFlagFilter;
    const columnsChanged =
      lastColumnsSignature === null || columnsSignature !== lastColumnsSignature;
    if (searchChanged || flagChanged || columnsChanged) {
      scheduleFilterRefresh(searchValue, flagValue, columns, searchChanged);
      lastSearchValue = searchValue;
      lastFlagFilter = flagValue;
      lastColumnsSignature = columnsSignature;
    }
  }

  $: visibleCount =
    Math.ceil((viewportHeight || ROW_HEIGHT) / ROW_HEIGHT) + BUFFER * 2;
  $: maxStart = Math.max(0, filteredRows.length - visibleCount);
  $: startIndex = Math.min(
    maxStart,
    Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - BUFFER)
  );
  $: endIndex = Math.min(filteredRows.length, startIndex + visibleCount);
  $: virtualRows = filteredRows.slice(startIndex, endIndex);
  $: offsetY = startIndex * ROW_HEIGHT;
  $: totalHeight = filteredRows.length * ROW_HEIGHT;

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

  const getComparableValue = (row: CachedRow, column: string): string | number => {
    const value = row.data[column];
    if (value === null || value === undefined) {
      return '';
    }
    if (typeof value === 'number') {
      return value;
    }
    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }
    if (typeof value === 'string') {
      return value;
    }
    return formatCell(value);
  };

  const compareRows = (a: CachedRow, b: CachedRow): number => {
    if (!sortKey) {
      return a.row_index - b.row_index;
    }
    const column = sortKey;
    const aValue = getComparableValue(a, column);
    const bValue = getComparableValue(b, column);
    if (typeof aValue === 'number' && typeof bValue === 'number') {
      const diff = aValue - bValue;
      if (diff !== 0) {
        return diff;
      }
    } else {
      const diff = collator.compare(String(aValue), String(bValue));
      if (diff !== 0) {
        return diff;
      }
    }
    return a.row_index - b.row_index;
  };

  const getFlagFilterDetails = (value: FlagSymbol | 'all' | 'none') => {
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

  const selectFlagFilter = (value: FlagSymbol | 'all' | 'none') => {
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
    recomputeFilteredRows(false);
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

  const setFlag = async (row: CachedRow, flag: FlagSymbol) => {
    const currentFlag = normalizeFlag(row.flag);
    const nextFlag = currentFlag === flag ? '' : flag;
    try {
      await backend.updateFlag({
        projectId: projectDetail.project.meta.id,
        rowIndex: row.row_index,
        flag: nextFlag || null,
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
    recomputeFilteredRows(false);
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
      recomputeFilteredRows(false);
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
        const selected = await save({
          filters: [{ name: 'CSV with flags', extensions: ['csv'] }],
          defaultPath: `${projectDetail.project.meta.name.replace(/\.[^.]+$/, '')}-trivium.csv`
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

  const recomputeFilteredRows = (resetScroll: boolean) => {
    if (rows.length === 0) {
      filteredRows = [];
      flaggedCount = totalFlagged;
      columnWidths = new Map();
      if (resetScroll && bodyScrollEl) bodyScrollEl.scrollTop = 0;
      if (resetScroll && bodyScrollEl) bodyScrollEl.scrollLeft = 0;
      if (resetScroll && headerScrollEl) headerScrollEl.scrollLeft = 0;
      scrollTop = 0;
      expandedCell = null;
      emitSummary();
      return;
    }

    const visibleForWidth = projectDetail
      ? projectDetail.columns.filter((column) => !hiddenColumns.has(column))
      : [];
    const columnMaxChars = projectDetail?.column_max_chars ?? {};

    const nextFiltered = [...rows];

    if (sortKey) {
      const direction = sortDirection === 'asc' ? 1 : -1;
      nextFiltered.sort((a, b) => direction * compareRows(a, b));
    } else {
      nextFiltered.sort((a, b) => a.row_index - b.row_index);
    }

    filteredRows = nextFiltered;
    flaggedCount = totalFlagged;

    if (resetScroll) {
      scrollTop = 0;
      if (bodyScrollEl) {
        bodyScrollEl.scrollTop = 0;
        bodyScrollEl.scrollLeft = 0;
      }
      if (headerScrollEl) headerScrollEl.scrollLeft = 0;
    } else if (viewportHeight > 0) {
      const maxScroll = Math.max(0, nextFiltered.length * ROW_HEIGHT - viewportHeight);
      if (scrollTop > maxScroll) {
        scrollTop = maxScroll;
        if (bodyScrollEl) bodyScrollEl.scrollTop = scrollTop;
      }
    }

    emitSummary();

    const nextWidthMap = new Map<string, number>();
    for (const column of visibleForWidth) {
      const headerChars = Math.min(column.length, WIDTH_LIMIT_CHARS);
      const dataChars = Math.min(columnMaxChars[column] ?? headerChars, WIDTH_LIMIT_CHARS);
      const maxChars = Math.max(headerChars, dataChars);
      const estimated = Math.round(maxChars * CHAR_PIXEL + COLUMN_PADDING);
      const width = Math.min(Math.max(estimated, MIN_DATA_WIDTH), MAX_DATA_WIDTH);
      nextWidthMap.set(column, width);
    }
    columnWidths = nextWidthMap;
  };

  const getSearchableColumns = () =>
    projectDetail ? projectDetail.columns.filter((column) => !hiddenColumns.has(column)) : [];

  const fetchFilteredRows = async (
    searchValue: string,
    flagValue: FlagFilterValue,
    columns: string[],
    resetScroll: boolean,
    force: boolean
  ) => {
    if (!projectDetail) return;
    const signature = `${searchValue}::${flagValue}::${columns.join('|')}`;
    if (!force && signature === lastAppliedFilters) {
      return;
    }
    filterRequestId += 1;
    const currentRequestId = filterRequestId;
    try {
      const response = await backend.queryProjectRows({
        projectId: projectDetail.project.meta.id,
        search: searchValue.length > 0 ? searchValue : undefined,
        flagFilter: flagValue === 'all' && searchValue.length === 0 ? undefined : flagValue,
        columns,
      });
      if (currentRequestId !== filterRequestId) {
        return;
      }
      totalFlagged = response.total_flagged;
      rows = response.rows.map((row) => normalizeRow(row));
      flaggedCount = totalFlagged;
      lastAppliedFilters = signature;
      recomputeFilteredRows(resetScroll);
    } catch (error) {
      console.error(error);
    }
  };

  const scheduleFilterRefresh = (
    searchValue: string,
    flagValue: FlagFilterValue,
    columns: string[],
    resetScroll: boolean
  ) => {
    pendingFilters = {
      search: searchValue,
      flag: flagValue,
      columns: [...columns],
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
      void fetchFilteredRows(
        scheduled.search,
        scheduled.flag,
        scheduled.columns,
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
    const columns = getSearchableColumns();
    return fetchFilteredRows(search.trim(), flagFilter, columns, resetScroll, true);
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
        if (memoEditor) {
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
    <div
      class="cell-dialog-backdrop"
      role="dialog"
      aria-modal="true"
      aria-label={`Edit memo for row ${memoEditor.row.row_index + 1}`}
      on:click={handleMemoBackdropClick}
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

  <section class="table-wrapper">
    <div class="meta">
      <span>{filteredRows.length} / {rows.length} rows</span>
      <span>{flaggedCount} flagged</span>
    </div>
    {#if filteredRows.length === 0}
      <div class="empty-rows">No rows match your filters.</div>
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
              {#each virtualRows as row (row.row_index)}
                <div
                  class="data-row"
                  class:alt-row={row.row_index % 2 === 1}
                  style={`grid-template-columns: ${gridTemplate}; ${stickyVariables}; --row-height: ${ROW_HEIGHT}px;`}
                >
                  <div class="cell index sticky sticky-index">{row.row_index + 1}</div>
                  <div class="cell flag sticky sticky-flag">
                  <div class="flag-buttons">
                    {#each FLAG_OPTIONS as option}
                      <button
                        type="button"
                        class:selected={normalizeFlag(row.flag) === option.value}
                        class:flag-safe={option.value === 'safe'}
                        class:flag-suspicious={option.value === 'suspicious'}
                        class:flag-critical={option.value === 'critical'}
                        on:click={() => setFlag(row, option.value)}
                      >
                        {option.hint}
                      </button>
                    {/each}
                  </div>
                  </div>
                  <button
                    class="cell memo-button sticky sticky-memo"
                    on:click={() => editMemo(row)}
                    title={row.memo && row.memo.trim().length ? row.memo : 'Add memo'}
                  >
                    {#if row.memo && row.memo.trim().length}
                      <span class="memo-text">{row.memo}</span>
                    {:else}
                      <span class="memo-placeholder">Add memo</span>
                    {/if}
                  </button>
                  {#each visibleColumns as column, columnIndex}
                    {@const cellValue = row.displayCache[column] ?? ''}
                    <button
                      type="button"
                      class="cell"
                      class:sticky={columnIndex === 0}
                      class:stickyData={columnIndex === 0}
                      title={cellValue}
                      on:click={() => openCell(column, cellValue)}
                      on:keydown={(event) => handleCellKeydown(event, column, cellValue)}
                    >
                      <span class="cell-text">{cellValue || '—'}</span>
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
