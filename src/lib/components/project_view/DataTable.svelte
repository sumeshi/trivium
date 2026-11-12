<script lang="ts">
  import { createEventDispatcher, onMount } from 'svelte';
  import {
    ROW_HEIGHT, PAGE_SIZE, SELECT_COL_WIDTH, INDEX_COL_WIDTH, FLAG_COL_WIDTH, MEMO_COL_WIDTH,
    STICKY_COLUMNS_WIDTH, MIN_DATA_WIDTH, MAX_DATA_WIDTH, CHAR_PIXEL, COLUMN_PADDING, BUFFER,
    FLAG_OPTIONS, normalizeFlag,
  } from './state';
  import type { CachedRow, VirtualRow } from './state';
  import type { CellKind } from '../../types';
  import { headerSelectionState } from '../../selection';
  import CellContextMenu from './CellContextMenu.svelte';
  import ColumnContextMenu from './ColumnContextMenu.svelte';
  import { theme } from '../../theme';
  import { displayRowColor } from '../../skin';

  import {
    logicalRowAt,
    maxScrollTop,
    virtualWindow
  } from '../../virtualScroll';
  import { useWorkspace } from "../../workspaceContext";
  const {
    rowsCache, pendingPages, totalRows, totalFilteredRows, filteredFlaggedCount, visibleColumns,
    sortKey, sortDirection, bodyScrollEl, headerScrollEl, viewportHeight, scrollTop, scrollLeft,
    tableWidth, projectDetail, columnLabels, toggleSort, setFlag, editMemo, openCell,
    handleCellKeydown, positionToRowIndex, largePages, selectedRowIds, selectedCount,
    selectVisibleRows, toggleRowSelection, rememberOffset, searchColumns,
  } = useWorkspace();

  const _toggleSort = toggleSort;
  const _setFlag = setFlag;
  const _editMemo = editMemo;
  const _openCell = openCell;
  const _handleCellKeydown = handleCellKeydown;
  const _normalizeFlag = normalizeFlag;

  export let columnWidths: Map<string, number> = new Map();
  const dispatch = createEventDispatcher<{
    notify: { message: string; tone: 'success' | 'error' };
    columnRule: { columnId: string };
  }>();

  type CellMenuState = {
    projectId: string;
    rowIndex: number;
    columnId: string;
    preview: string;
    truncated: boolean;
    kind: CellKind;
    row: CachedRow;
    x: number;
    y: number;
  };

  let cellMenu: CellMenuState | null = null;
  let columnMenu: { columnId: string; x: number; y: number; trigger: HTMLElement } | null = null;
  let virtualRows: VirtualRow[] = [];
  let resizeObserver: ResizeObserver | null = null;
  let observedScrollEl: HTMLDivElement | null = null;
  let horizontalScrollEl: HTMLDivElement | null = null;
  let selectAllEl: HTMLInputElement | null = null;

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
  $: horizontalMaximum = Math.max(0, effectiveTableWidth - $tableWidth);
  $: if (horizontalScrollEl && horizontalScrollEl.scrollLeft !== $scrollLeft) {
    horizontalScrollEl.scrollLeft = $scrollLeft;
  }
  $: gridTemplate = [
    `${SELECT_COL_WIDTH}px`,
    `${INDEX_COL_WIDTH}px`,
    `${FLAG_COL_WIDTH}px`,
    `${MEMO_COL_WIDTH}px`,
    ...distributedDataWidths.map((width) => `${width}px`)
  ].join(' ');

  // Set CSS variables to specify sticky column positions
  $: if (typeof document !== 'undefined') {
    document.documentElement.style.setProperty('--sticky-index', `${SELECT_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-flag', `${SELECT_COL_WIDTH + INDEX_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-memo', `${SELECT_COL_WIDTH + INDEX_COL_WIDTH + FLAG_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-data', `${SELECT_COL_WIDTH + INDEX_COL_WIDTH + FLAG_COL_WIDTH + MEMO_COL_WIDTH}px`);
  }

  // The applied QuerySpec result is the only source for the table's row count.
  $: effectiveTotalRows = $totalFilteredRows;
  $: windowGeometry = virtualWindow(
    $scrollTop,
    effectiveTotalRows,
    $viewportHeight,
    BUFFER
  );
  $: startIndex = windowGeometry.start;
  $: endIndex = windowGeometry.end;
  $: offsetY = windowGeometry.innerTop;
  $: totalHeight = windowGeometry.height;
  // A filter can shrink the result set while the viewport is near the old end.
  // Pull both the store and the scroll element back inside the new physical
  // range so the space below the last row is not left as a blank spacer. Wait
  // until pending page requests settle: on project load the count is briefly 0
  // while the saved scroll offset is being restored, and clamping then would
  // lose it.
  $: {
    const maxScroll = maxScrollTop(effectiveTotalRows, $viewportHeight);
    if ($pendingPages.size === 0 && $scrollTop > maxScroll) {
      scrollTop.set(maxScroll);
      if ($bodyScrollEl && $bodyScrollEl.scrollTop !== maxScroll) {
        $bodyScrollEl.scrollTop = maxScroll;
      }
    }
  }
  $: loadedRowCount = $rowsCache.size;
  $: projectId = $projectDetail?.project.meta.id ?? '';
  $: if (cellMenu && cellMenu.projectId !== projectId) {
    cellMenu = null;
  }

  const openCellMenu = (event: MouseEvent, row: CachedRow, column: string) => {
    event.preventDefault();
    if (!projectId) return;
    columnMenu = null;
    cellMenu = {
      projectId,
      rowIndex: row.row_index,
      columnId: column,
      preview: row.previewCache[column] ?? '',
      truncated: row.cells?.[column]?.truncated === true,
      kind: row.cells?.[column]?.kind ?? 'string',
      row,
      x: event.clientX,
      y: event.clientY
    };
  };

  const openColumnMenu = (event: MouseEvent | KeyboardEvent, columnId: string) => {
    event.preventDefault();
    event.stopPropagation();
    const trigger = event.currentTarget as HTMLElement;
    const rect = trigger.getBoundingClientRect();
    cellMenu = null;
    columnMenu = {
      columnId, trigger,
      x: event instanceof MouseEvent && event.clientX ? event.clientX : rect.left,
      y: event instanceof MouseEvent && event.clientY ? event.clientY : rect.bottom,
    };
  };

  const columnMenuKey = (event: KeyboardEvent, columnId: string) => {
    if (event.key === 'ContextMenu' || event.key === 'ArrowDown' || (event.shiftKey && event.key === 'F10')) {
      openColumnMenu(event, columnId);
    }
  };

  const searchColumn = (columnId: string) => {
    searchColumns.set([columnId]);
    columnMenu = null;
    document.querySelector<HTMLInputElement>('input[aria-label="Search rows"]')?.focus();
  };

  const buildVirtualRows = (
    start: number,
    end: number,
    cache: Map<number, CachedRow>,
    posMap: Map<number, number>,
    pendingPages: Set<string>
  ): VirtualRow[] => {
    if (end <= start) {
      return [];
    }
    const result: VirtualRow[] = [];
    // Use backend-provided ordering via positionToRowIndex map
    
    // If posMap is empty and we have pending pages, return loading state rows
    // This handles project switching where we've reset posMap but first page is still loading
    if (posMap.size === 0 && pendingPages.size > 0) {
      for (let position = start; position < end; position += 1) {
        result.push({ position, row: null });
      }
      return result;
    }
    for (let position = start; position < end; position += 1) {
      const rowIndex = posMap.get(position) ?? null;
      const row = rowIndex !== null ? cache.get(rowIndex) ?? null : null;
      result.push({ position, row });
    }
    return result;
  };

  $: virtualRows = buildVirtualRows(startIndex, endIndex, $rowsCache, $positionToRowIndex, $pendingPages);
  // The header checkbox covers only rows that are rendered right now. Selecting
  // the whole result set is the bulk panel's "Current results" scope instead.
  $: visibleRowIds = virtualRows
    .map((item) => item.row?.row_index)
    .filter((rowIndex): rowIndex is number => rowIndex !== undefined);
  $: headerState = headerSelectionState(visibleRowIds, $selectedRowIds);
  $: if (selectAllEl) {
    selectAllEl.indeterminate = headerState === 'some';
  }

  const toggleVisibleRows = () => {
    if (visibleRowIds.length === 0) return;
    selectVisibleRows(visibleRowIds, headerState !== 'all');
  };

  const handleScroll = (event: Event) => {
    const target = event.currentTarget as HTMLDivElement;
    if (!target.isConnected) return;
    scrollTop.set(target.scrollTop);
    rememberOffset(logicalRowAt(target.scrollTop, $totalFilteredRows, $viewportHeight));
    syncHeader(target.scrollLeft);
  };

  // The body owns horizontal position. Feeding delayed header scroll events
  // back into it can undo a newer wheel gesture or a tab's restored position.
  const syncHeader = (left: number) => {
    scrollLeft.set(left);
    if ($headerScrollEl && $headerScrollEl.scrollLeft !== left) {
      $headerScrollEl.scrollLeft = left;
    }
    if (horizontalScrollEl && horizontalScrollEl.scrollLeft !== left) {
      horizontalScrollEl.scrollLeft = left;
    }
  };

  const handleHorizontalScroll = (event: Event) => {
    const target = event.currentTarget as HTMLDivElement;
    if (!$bodyScrollEl || !target.isConnected) return;
    $bodyScrollEl.scrollLeft = target.scrollLeft;
    syncHeader($bodyScrollEl.scrollLeft);
  };

  const handleHorizontalKey = (event: KeyboardEvent) => {
    if (!$bodyScrollEl) return;
    const previous = $bodyScrollEl.scrollLeft;
    const next = event.key === 'ArrowLeft' ? previous - ROW_HEIGHT
      : event.key === 'ArrowRight' ? previous + ROW_HEIGHT
      : event.key === 'PageUp' ? previous - $bodyScrollEl.clientWidth
      : event.key === 'PageDown' ? previous + $bodyScrollEl.clientWidth
      : event.key === 'Home' ? 0 : event.key === 'End' ? horizontalMaximum : null;
    if (next === null) return;
    event.preventDefault();
    $bodyScrollEl.scrollLeft = next;
    syncHeader($bodyScrollEl.scrollLeft);
  };

  const focusColumn = (index: number) => {
    if (!$bodyScrollEl || index === 0 || index >= distributedDataWidths.length) return;
    const left = STICKY_COLUMNS_WIDTH + distributedDataWidths.slice(0, index).reduce((a, b) => a + b, 0);
    const right = left + distributedDataWidths[index];
    const stickyWidth = STICKY_COLUMNS_WIDTH + (distributedDataWidths[0] ?? 0);
    if (left < $bodyScrollEl.scrollLeft + stickyWidth) {
      $bodyScrollEl.scrollLeft = Math.max(0, left - stickyWidth);
    } else if (right > $bodyScrollEl.scrollLeft + $bodyScrollEl.clientWidth) {
      $bodyScrollEl.scrollLeft = right - $bodyScrollEl.clientWidth;
    }
    syncHeader($bodyScrollEl.scrollLeft);
  };

  const forwardWheel = (event: WheelEvent) => {
    if (!$bodyScrollEl) return;
    const deltaX = event.deltaX || (event.shiftKey ? event.deltaY : 0);
    const deltaY = event.shiftKey && !event.deltaX ? 0 : event.deltaY;
    if (event.currentTarget === $bodyScrollEl && !deltaX) return;
    let handled = false;
    if (deltaY !== 0) {
      const previousTop = $bodyScrollEl.scrollTop;
      $bodyScrollEl.scrollTop += deltaY;
      handled = handled || $bodyScrollEl.scrollTop !== previousTop;
    }
    if (deltaX !== 0) {
      const previousLeft = $bodyScrollEl.scrollLeft;
      $bodyScrollEl.scrollLeft += deltaX;
      handled = handled || $bodyScrollEl.scrollLeft !== previousLeft;
      syncHeader($bodyScrollEl.scrollLeft);
    }
    if (handled) {
      event.preventDefault();
    }
  };

  onMount(() => {
    resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        tableWidth.set(entry.contentRect.width);
      }
    });
    if ($bodyScrollEl) {
      tableWidth.set($bodyScrollEl.clientWidth);
      resizeObserver.observe($bodyScrollEl);
      observedScrollEl = $bodyScrollEl;
    }

    return () => {
      if (resizeObserver && observedScrollEl) {
        resizeObserver.unobserve(observedScrollEl);
      }
      resizeObserver?.disconnect();
    };
  });

</script>

<section class="table-wrapper">
  <div class="table-meta">
    <div class="table-meta-left">
      <strong>{$totalFilteredRows.toLocaleString()}</strong>
      <span>of {$totalRows.toLocaleString()} rows</span>
      {#if $selectedCount > 0}
        <span class="meta-dot">·</span>
        <span><strong>{$selectedCount}</strong> selected</span>
      {/if}
    </div>
    <span class="table-meta-right">Flagged {$filteredFlaggedCount.toLocaleString()}</span>
  </div>
  {#if effectiveTotalRows === 0 && $pendingPages.size === 0}
    <div class="empty-rows">
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round">
        <circle cx="11" cy="11" r="7" />
        <path d="m20 20-3.5-3.5" />
      </svg>
      <p>No rows match your filters.</p>
      <span>Adjust the search or clear a filter to see more rows.</span>
    </div>
  {:else}
    <div class="table-scroll">
      <div
        class="table-header-scroll"
        bind:this={$headerScrollEl}
        on:wheel={forwardWheel}
      >
        <div
          class="data-header"
          style={`grid-template-columns: ${gridTemplate}; width: ${effectiveTableWidth}px;`}
        >
          <div class="header-cell sticky sticky-select">
            <input
              type="checkbox"
              bind:this={selectAllEl}
              checked={headerState === 'all'}
              disabled={visibleRowIds.length === 0}
              on:change={toggleVisibleRows}
              aria-label="Select visible rows"
              title="Select or clear the rows currently on screen"
            />
          </div>
          <div class="header-cell sticky sticky-index">#</div>
          <div class="header-cell sticky sticky-flag">Flag</div>
          <div class="header-cell sticky sticky-memo">Memo</div>
          {#each $visibleColumns as column, columnIndex}
            <div
              class="header-cell column-heading"
              class:sticky={columnIndex === 0}
              class:stickyData={columnIndex === 0}
              class:sorted={$sortKey === column}
            >
              <button type="button" class="column-sort"
                on:click={() => _toggleSort(column)}
                on:focus={() => focusColumn(columnIndex)}
                on:contextmenu={(event) => openColumnMenu(event, column)}
                on:keydown={(event) => columnMenuKey(event, column)}
                aria-pressed={$sortKey === column}
                title={`Sort by ${$columnLabels.get(column) ?? column}; right-click, Down or Shift+F10 for column actions`}>
                <span class="header-label">{$columnLabels.get(column) ?? column}</span>
                {#if $sortKey === column}
                  <span class="sort-indicator" aria-hidden="true">{$sortDirection === 'asc' ? '▲' : '▼'}</span>
                {/if}
              </button>
              <button type="button" class="column-actions-trigger"
                tabindex="-1"
                aria-label={`Actions for column ${$columnLabels.get(column) ?? column}`}
                title={`Search or filter ${$columnLabels.get(column) ?? column}`}
                aria-haspopup="menu" aria-expanded={columnMenu?.columnId === column}
                on:click={(event) => openColumnMenu(event, column)}
                on:focus={() => focusColumn(columnIndex)}>
                <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><circle cx="12" cy="5" r="2"/><circle cx="12" cy="12" r="2"/><circle cx="12" cy="19" r="2"/></svg>
              </button>
            </div>
          {/each}
        </div>
      </div>
      <div
        class="virtual-viewport"
        id="project-table-viewport"
        bind:this={$bodyScrollEl}
        bind:clientHeight={$viewportHeight}
        bind:clientWidth={$tableWidth}
        on:scroll={handleScroll}
        on:wheel={forwardWheel}
      >
        <div class="virtual-spacer" style={`height: ${totalHeight}px; width: ${effectiveTableWidth}px;`}>
          <div class="virtual-inner" style={`top: ${offsetY}px;`}>
            {#each virtualRows as item (item.position)}
              {@const largePage = !item.row && $largePages.has(Math.floor(item.position / PAGE_SIZE))}
              <div
                class="data-row"
                class:alt-row={item.position % 2 === 1}
                class:loading={!item.row && !largePage}
                style={`grid-template-columns: ${gridTemplate}; --row-height: ${ROW_HEIGHT}px;${
                  item.row?.color
                    ? ` --row-color: ${displayRowColor(item.row.color, $theme)};`
                    : ''
                }`}
              >
                {#if largePage}
                  <div class="cell select-cell sticky sticky-select" aria-hidden="true"></div>
                  <div class="cell index sticky sticky-index" aria-label="Original row not loaded">…</div>
                  {#if item.position % PAGE_SIZE === 0 || item.position === startIndex}
                    <div
                      class="cell large-page-notice"
                      title="This page exceeds the page-cache byte limit and is not retained in memory."
                    >
                      Page too large to cache. Scroll to another page or narrow the results.
                    </div>
                  {:else}
                    <div class="large-page-blank" aria-hidden="true"></div>
                  {/if}
                {:else}
                  <div class="cell select-cell sticky sticky-select">
                    {#if item.row}
                      <input
                        type="checkbox"
                        checked={$selectedRowIds.has(item.row.row_index)}
                        on:change={() => item.row && toggleRowSelection(item.row.row_index)}
                        aria-label={`Select row ${item.row.row_index + 1}`}
                      />
                    {/if}
                  </div>
                  <div class="cell index sticky sticky-index">{item.row ? item.row.row_index + 1 : '…'}</div>
                  <div class="cell flag sticky sticky-flag">
                    {#if item.row}
                      <div class="flag-buttons">
                        {#each FLAG_OPTIONS as option}
                          <button
                            type="button"
                            class:selected={item.row ? _normalizeFlag(item.row.flag) === option.value : false}
                            class:flag-safe={option.value === 'safe'}
                            class:flag-suspicious={option.value === 'suspicious'}
                            class:flag-critical={option.value === 'critical'}
                            title={option.label}
                            aria-label={option.label}
                            on:click={() => item.row && _setFlag(item.row, option.value)}
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
                    on:click={() =>
                      item.row &&
                      $projectDetail &&
                      _editMemo(item.row, $projectDetail.project.meta.id)}
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
                      <span class="memo-icon" aria-hidden="true">✎</span>
                      <span class="memo-text">{item.row.memo}</span>
                    {:else if item.row}
                      <span class="memo-icon muted" aria-hidden="true">✎</span>
                      <span class="memo-placeholder">Add memo</span>
                    {:else}
                      <span class="memo-placeholder">Loading…</span>
                    {/if}
                  </button>
                  {#each $visibleColumns as column, columnIndex}
                    {@const cellValue =
                      item.row && column in item.row.displayCache
                        ? item.row.displayCache[column]
                        : null}
                    {@const previewValue =
                      item.row && column in item.row.previewCache
                        ? item.row.previewCache[column]
                        : ''}
                    <button
                      type="button"
                      class="cell"
                      class:sticky={columnIndex === 0}
                      class:stickyData={columnIndex === 0}
                      title={
                        !item.row || cellValue === null
                          ? 'Loading…'
                          : cellValue
                      }
                      on:click={() => {
                        if (!item.row || !projectId) return;
                        _openCell(projectId, column, previewValue, item.row);
                      }}
                      on:contextmenu={(event) => {
                        if (!item.row) return;
                        openCellMenu(event, item.row, column);
                      }}
                      on:keydown={(event) => {
                        if (!item.row || !projectId) return;
                        _handleCellKeydown(
                          event,
                          projectId,
                          column,
                          previewValue,
                          item.row
                        );
                      }}
                      disabled={!item.row}
                    >
                      <span class="cell-text">
                        {#if !item.row || cellValue === null}
                          Loading…
                        {:else}
                          {cellValue}
                        {/if}
                      </span>
                    </button>
                  {/each}
                {/if}
              </div>
            {/each}
          </div>
        </div>
      </div>
      {#if $tableWidth > 0 && horizontalMaximum > 1}
        <div
          class="table-horizontal-scroll"
          bind:this={horizontalScrollEl}
          style={`width: ${$tableWidth}px;`}
          role="scrollbar"
          aria-label="Scroll table horizontally"
          aria-controls="project-table-viewport"
          aria-orientation="horizontal"
          aria-valuemin="0"
          aria-valuemax={horizontalMaximum}
          aria-valuenow={Math.round($scrollLeft)}
          title="Scroll table horizontally (Shift+wheel or arrow keys)"
          tabindex="0"
          on:scroll={handleHorizontalScroll}
          on:keydown={handleHorizontalKey}
        >
          <div style={`width: ${effectiveTableWidth}px; height: 1px;`} aria-hidden="true"></div>
        </div>
      {/if}
    </div>
  {/if}
</section>

{#if cellMenu}
  {#key cellMenu}
    <CellContextMenu
      {...cellMenu}
      on:close={() => (cellMenu = null)}
      on:notify={(event) => dispatch('notify', event.detail)}
    />
  {/key}
{/if}

{#if columnMenu}
  {#key columnMenu}
    <ColumnContextMenu columnId={columnMenu.columnId}
      label={$columnLabels.get(columnMenu.columnId) ?? columnMenu.columnId}
      x={columnMenu.x} y={columnMenu.y}
      on:search={(event) => searchColumn(event.detail.columnId)}
      on:rule={(event) => { columnMenu = null; dispatch('columnRule', event.detail); }}
      on:close={(event) => {
        if (event.detail.restoreFocus) columnMenu?.trigger.focus({ preventScroll: true });
        columnMenu = null;
      }} />
  {/key}
{/if}
