<script lang="ts">
  import { createEventDispatcher, onMount } from 'svelte';
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

  const FLAG_OPTIONS = ['◯', '?', '✗'] as const;
  type FlagSymbol = (typeof FLAG_OPTIONS)[number];
  const ROW_HEIGHT = 56;
  const BUFFER = 8;
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
  const INDEX_COL_WIDTH = 72;
  const FLAG_COL_WIDTH = 148;
  const MEMO_COL_WIDTH = 260;
  const MIN_DATA_WIDTH = 180;
  const WIDTH_LIMIT_CHARS = 100;
  const CHAR_PIXEL = 7;
  const COLUMN_PADDING = 32;
  const MAX_DATA_WIDTH = WIDTH_LIMIT_CHARS * CHAR_PIXEL + COLUMN_PADDING;
  const STICKY_COLUMNS_WIDTH = INDEX_COL_WIDTH + FLAG_COL_WIDTH + MEMO_COL_WIDTH;

  let currentProjectId: string | null = null;
  let lastProjectDetail: LoadProjectResponse | null = null;
  let lastRowsRef: ProjectRow[] | null = null;
  let lastHiddenColumnsRef: string[] | null = null;
  let rows: ProjectRow[] = [];

  let hiddenColumns = new Set<string>();
  let columnsOpen = false;
  let columnPickerEl: HTMLDivElement | null = null;

  let search = '';
  let flagFilter: FlagSymbol | 'all' | 'none' = 'all';

  let sortKey: string | null = null;
  let sortDirection: 'asc' | 'desc' = 'asc';

  let isExporting = false;
  let isUpdatingColumns = false;

  let visibleColumns: string[] = [];
  let filteredRows: ProjectRow[] = [];
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

  $: if (projectDetail) {
    const nextRowsRef = projectDetail.rows;
    const nextHiddenColumns = projectDetail.hidden_columns ?? [];
    const projectChanged = projectDetail.project.meta.id !== currentProjectId;
    const rowsChanged = nextRowsRef !== lastRowsRef;
    const shouldInitialize = !initialized || projectChanged || rowsChanged;

    if (shouldInitialize) {
      lastProjectDetail = projectDetail;
      currentProjectId = projectDetail.project.meta.id;
      rows = new Array(nextRowsRef.length);
      for (const row of nextRowsRef) {
        rows[row.row_index] = normalizeRow(row);
      }
      hiddenColumns = new Set(nextHiddenColumns);
      lastSummaryFlagged = -1;
      lastSummaryHiddenKey = '';
      sortKey = null;
      sortDirection = 'asc';
      expandedCell = null;
      recomputeFilteredRows(true);
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
    search;
    flagFilter;
    columnsKey;
    sortKey;
    sortDirection;
    recomputeFilteredRows(false);
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

  const expandColumnWidths = (baseWidths: number[], availableWidth: number) => {
    if (baseWidths.length === 0) {
      return baseWidths;
    }

    const normalized = baseWidths.map((width) => {
      if (width < MIN_DATA_WIDTH) return MIN_DATA_WIDTH;
      if (width > MAX_DATA_WIDTH) return MAX_DATA_WIDTH;
      return width;
    });

    if (availableWidth <= 0) {
      return normalized;
    }

    const minimumTotal = normalized.reduce((sum, width) => sum + width, 0);
    if (availableWidth <= minimumTotal) {
      return normalized;
    }

    const slack = normalized.map((width) => Math.max(0, MAX_DATA_WIDTH - width));
    const maximumTotal = minimumTotal + slack.reduce((sum, value) => sum + value, 0);
    const targetTotal = Math.min(availableWidth, maximumTotal);
    let remaining = targetTotal - minimumTotal;
    if (remaining <= 0) {
      return normalized;
    }

    const weights = normalized.map((width) => (width > 0 ? width : 1));
    const weightTotal = weights.reduce((sum, weight) => sum + weight, 0);
    const adjusted = [...normalized];

    for (let index = 0; index < adjusted.length && remaining > 0; index += 1) {
      if (slack[index] <= 0) continue;
      const share = (remaining * weights[index]) / weightTotal;
      const applied = Math.min(slack[index], share);
      adjusted[index] += applied;
      remaining -= applied;
      slack[index] -= applied;
    }

    if (remaining > 0) {
      for (let index = adjusted.length - 1; index >= 0 && remaining > 0; index -= 1) {
        if (slack[index] <= 0) continue;
        const applied = Math.min(slack[index], remaining);
        adjusted[index] += applied;
        remaining -= applied;
        slack[index] -= applied;
      }
    }

    const rounded = adjusted.map((width) => {
      if (width > MAX_DATA_WIDTH) return MAX_DATA_WIDTH;
      if (width < MIN_DATA_WIDTH) return MIN_DATA_WIDTH;
      return Math.round(width);
    });

    const roundedTotal = rounded.reduce((sum, width) => sum + width, 0);
    const desiredTotal = Math.round(targetTotal);
    let difference = desiredTotal - roundedTotal;
    let pass = 0;
    while (difference !== 0 && pass < rounded.length * 2) {
      const index = difference > 0 ? pass % rounded.length : rounded.length - 1 - (pass % rounded.length);
      const step = difference > 0 ? 1 : -1;
      const nextWidth = rounded[index] + step;
      if (nextWidth >= MIN_DATA_WIDTH && nextWidth <= MAX_DATA_WIDTH) {
        rounded[index] = nextWidth;
        difference -= step;
      }
      pass += 1;
    }

    return rounded;
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

  const normalizeRow = (incoming: ProjectRow): ProjectRow => ({
    ...incoming,
    memo: incoming.memo ?? ''
  });

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

  const getComparableValue = (row: ProjectRow, column: string): string | number => {
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

  const rowMatchesCurrentFilters = (row: ProjectRow) => {
    const trimmedFlag = row.flag.trim();
    if (flagFilter !== 'all') {
      if (flagFilter === 'none') {
        if (trimmedFlag.length > 0) {
          return false;
        }
      } else if (trimmedFlag !== flagFilter) {
        return false;
      }
    }
    const trimmed = search.trim();
    if (!trimmed) {
      return true;
    }
    const lower = trimmed.toLowerCase();
    return visibleColumns.some((column) =>
      formatCell(row.data[column]).toLowerCase().includes(lower)
    );
  };

  const compareRows = (a: ProjectRow, b: ProjectRow): number => {
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

  const updateRowState = (updated: ProjectRow) => {
    rows[updated.row_index] = normalizeRow(updated);
    recomputeFilteredRows(false);
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

  const setFlag = async (row: ProjectRow, flag: string) => {
    const nextFlag = row.flag === flag ? '' : flag;
    try {
      const updated = await backend.updateFlag({
        projectId: projectDetail.project.meta.id,
        rowIndex: row.row_index,
        flag: nextFlag,
        memo: row.memo && row.memo.trim().length ? row.memo : null
      });
      updateRowState(updated);
      dispatch('notify', {
        message: nextFlag ? `Marked row ${row.row_index + 1} as ${nextFlag}` : 'Cleared flag',
        tone: 'success'
      });
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to update flag.', tone: 'error' });
    }
  };

  const editMemo = async (row: ProjectRow) => {
    const nextMemo = window.prompt('Edit memo', row.memo ?? '');
    if (nextMemo === null) return;
    try {
      const updated = await backend.updateFlag({
        projectId: projectDetail.project.meta.id,
        rowIndex: row.row_index,
        flag: row.flag,
        memo: nextMemo.trim().length ? nextMemo : null
      });
      updateRowState(updated);
      dispatch('notify', { message: 'Memo updated.', tone: 'success' });
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to update memo.', tone: 'error' });
    }
  };

  const resetFilters = () => {
    search = '';
    flagFilter = 'all';
    sortKey = null;
    sortDirection = 'asc';
    recomputeFilteredRows(true);
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
    if (headerScrollEl && headerScrollEl.scrollLeft !== target.scrollLeft) {
      headerScrollEl.scrollLeft = target.scrollLeft;
    }
  };

  const handleHeaderScroll = () => {
    if (!headerScrollEl || !bodyScrollEl) return;
    if (bodyScrollEl.scrollLeft !== headerScrollEl.scrollLeft) {
      bodyScrollEl.scrollLeft = headerScrollEl.scrollLeft;
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
    const hasAnyRow = rows.some((row) => Boolean(row));
    if (!hasAnyRow) {
      filteredRows = [];
      flaggedCount = 0;
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
    const lengthTracker: Record<string, number> = {};
    for (const column of visibleForWidth) {
      lengthTracker[column] = column.length;
    }

    const nextFiltered: ProjectRow[] = [];
    let nextFlagged = 0;

    for (const row of rows) {
      if (!row) continue;
      for (const column of visibleForWidth) {
        const cellText = formatCell(row.data[column]);
        const candidateLength = Math.min(cellText.length, WIDTH_LIMIT_CHARS);
        if (candidateLength > (lengthTracker[column] ?? 0)) {
          lengthTracker[column] = candidateLength;
        }
      }
      if (row.flag.trim().length > 0) {
        nextFlagged += 1;
      }
      if (rowMatchesCurrentFilters(row)) {
        nextFiltered.push(row);
      }
    }

    if (sortKey) {
      const direction = sortDirection === 'asc' ? 1 : -1;
      nextFiltered.sort((a, b) => direction * compareRows(a, b));
    } else {
      nextFiltered.sort((a, b) => a.row_index - b.row_index);
    }

    filteredRows = nextFiltered;
    flaggedCount = nextFlagged;

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
      const maxChars = Math.min(lengthTracker[column] ?? column.length, WIDTH_LIMIT_CHARS);
      const estimated = Math.round(maxChars * CHAR_PIXEL + COLUMN_PADDING);
      const width = Math.min(Math.max(estimated, MIN_DATA_WIDTH), MAX_DATA_WIDTH);
      nextWidthMap.set(column, width);
    }
    columnWidths = nextWidthMap;
  };

  onMount(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (columnsOpen && columnPickerEl && !columnPickerEl.contains(event.target as Node)) {
        columnsOpen = false;
      }
    };
    const handleKeydown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        columnsOpen = false;
        closeCell();
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
  <header class="toolbar">
    <div>
      <h2>{projectDetail.project.meta.name}</h2>
      {#if projectDetail.project.meta.description}
        <p class="subtitle">{projectDetail.project.meta.description}</p>
      {/if}
    </div>
    <div class="toolbar-actions">
      <button class="ghost" on:click={() => dispatch('refresh')}>Refresh</button>
      <button class="primary" on:click={exportProject} disabled={isExporting}>
        {isExporting ? 'Exporting…' : 'Export CSV'}
      </button>
    </div>
  </header>

  <section class="filters">
    <label>
      <span>Search</span>
      <input
        placeholder="Search visible columns"
        bind:value={search}
        type="search"
      />
    </label>
    <label>
      <span>Flag</span>
      <select bind:value={flagFilter}>
        <option value="all">All</option>
        <option value="none">Unflagged</option>
        {#each FLAG_OPTIONS as option}
          <option value={option}>{option}</option>
        {/each}
      </select>
    </label>
    <button class="ghost" on:click={resetFilters}>Reset</button>

    <div class="column-picker" bind:this={columnPickerEl}>
      <button
        type="button"
        class="column-trigger"
        on:click={() => (columnsOpen = !columnsOpen)}
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
          <button type="button" class="ghost close-dialog" on:click={closeCell}>Close</button>
        </div>
        <pre class="cell-dialog-body">{expandedCell.value || '—'}</pre>
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
                          class:selected={row.flag === option}
                          class:positive={option === '◯'}
                          class:maybe={option === '?'}
                          class:negative={option === '✗'}
                          on:click={() => setFlag(row, option)}
                        >
                          {option}
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
                    {@const cellValue = formatCell(row.data[column])}
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
