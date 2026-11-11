<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import {
    rowsCache,
    pendingPages,
    totalRows,
    totalFilteredRows,
    filteredFlaggedCount,
    visibleColumns,
    sortKey,
    sortDirection,
    bodyScrollEl,
    headerScrollEl,
    viewportHeight,
    scrollTop,
    tableWidth,
    ROW_HEIGHT,
    PAGE_SIZE,
    INDEX_COL_WIDTH,
    FLAG_COL_WIDTH,
    MEMO_COL_WIDTH,
    STICKY_COLUMNS_WIDTH,
    MIN_DATA_WIDTH,
    MAX_DATA_WIDTH,
    CHAR_PIXEL,
    COLUMN_PADDING,
    BUFFER,
    projectDetail,
    columnLabels,
    toggleSort,
    FLAG_OPTIONS,
    normalizeFlag,
    setFlag,
    editMemo,
    openCell,
    handleCellKeydown,
    positionToRowIndex,
    largePages
  } from './state';
  import type { CachedRow, VirtualRow } from './state';
  import { theme } from '../../theme';
  import { displayRowColor } from '../../skin';
  import {
    logicalRowAt,
    maxScrollTop,
    physicalHeight,
    scrollTopForRow
  } from '../../virtualScroll';

  const _toggleSort = toggleSort;
  const _setFlag = setFlag;
  const _editMemo = editMemo;
  const _openCell = openCell;
  const _handleCellKeydown = handleCellKeydown;
  const _normalizeFlag = normalizeFlag;

  export let columnWidths: Map<string, number> = new Map();
  let virtualRows: VirtualRow[] = [];
  let resizeObserver: ResizeObserver | null = null;
  let observedScrollEl: HTMLDivElement | null = null;
  let isSyncingHeaderScroll = false;
  let isSyncingBodyScroll = false;
  let scrollTimeout: number | null = null;
  let releaseHeaderSyncFrame: number | null = null;
  let releaseBodySyncFrame: number | null = null;

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

  // Set CSS variables to specify sticky column positions
  $: if (typeof document !== 'undefined') {
    document.documentElement.style.setProperty('--sticky-flag', `${INDEX_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-memo', `${INDEX_COL_WIDTH + FLAG_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-data', `${INDEX_COL_WIDTH + FLAG_COL_WIDTH + MEMO_COL_WIDTH}px`);
  }

  $: viewportRows = Math.ceil(($viewportHeight || ROW_HEIGHT) / ROW_HEIGHT);
  $: visibleCount = viewportRows + BUFFER * 2;
  // The applied QuerySpec result is the only source for the table's row count.
  $: effectiveTotalRows = $totalFilteredRows;
  // Leave enough rows to fill the viewport; the scroll offset alone cannot
  // position the final row inside a compressed scroll range.
  $: maxStart = Math.max(0, effectiveTotalRows - viewportRows);
  // The physical scroll position is mapped to a logical row in one place so
  // the displayed start and the page offsets use the same result position.
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
  // Rows keep the fixed ROW_HEIGHT; only the spacer height is scaled.
  $: offsetY =
    scrollTopForRow(startIndex, effectiveTotalRows, $viewportHeight) - $scrollTop;
  $: totalHeight = physicalHeight(effectiveTotalRows);
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

  const handleScroll = (event: Event) => {
    const target = event.currentTarget as HTMLDivElement;
    scrollTop.set(target.scrollTop);
    
    // Clear scroll timeout
    if (scrollTimeout !== null) {
      clearTimeout(scrollTimeout);
    }
    
    // Execute scroll synchronization with debounce
    scrollTimeout = setTimeout(() => {
      if (isSyncingBodyScroll) {
        isSyncingBodyScroll = false;
        return;
      }
      if ($headerScrollEl && $headerScrollEl.scrollLeft !== target.scrollLeft) {
        isSyncingHeaderScroll = true;
        $headerScrollEl.scrollLeft = target.scrollLeft;
        isSyncingHeaderScroll = false;
      }
      scrollTimeout = null;
    }, 16); // 約60FPS
  };

  const handleHeaderScroll = () => {
    if (!$headerScrollEl || !$bodyScrollEl) return;
    if (isSyncingHeaderScroll) {
      isSyncingHeaderScroll = false;
      return;
    }
    const nextLeft = $headerScrollEl.scrollLeft;
    if ($bodyScrollEl.scrollLeft !== nextLeft) {
      isSyncingBodyScroll = true;
      $bodyScrollEl.scrollLeft = nextLeft;
      // Reset flag immediately for more reliable synchronization
      isSyncingBodyScroll = false;
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

  onDestroy(() => {
    if (releaseHeaderSyncFrame !== null) {
      cancelAnimationFrame(releaseHeaderSyncFrame);
      releaseHeaderSyncFrame = null;
    }
    if (releaseBodySyncFrame !== null) {
      cancelAnimationFrame(releaseBodySyncFrame);
      releaseBodySyncFrame = null;
    }
  });
</script>

<section class="table-wrapper">
  <div class="meta">
    <span>
      {$totalFilteredRows} / {$totalRows} rows
    </span>
    <span>{$filteredFlaggedCount} flagged in results</span>
  </div>
  {#if effectiveTotalRows === 0 && $pendingPages.size === 0}
    <div class="empty-rows">
      No rows match your filters.
    </div>
  {:else}
    <div class="table-scroll">
      <div
        class="table-header-scroll"
        bind:this={$headerScrollEl}
        on:scroll={handleHeaderScroll}
        on:wheel={forwardWheel}
      >
        <div
          class="data-header"
          style={`grid-template-columns: ${gridTemplate}; width: ${effectiveTableWidth}px;`}
        >
          <div class="header-cell sticky sticky-index">#</div>
          <div class="header-cell sticky sticky-flag">Flag</div>
          <div class="header-cell sticky sticky-memo">Memo</div>
          {#each $visibleColumns as column, columnIndex}
            <button
              type="button"
              class="header-cell"
              class:sticky={columnIndex === 0}
              class:stickyData={columnIndex === 0}
              class:sorted={$sortKey === column}
              on:click={() => _toggleSort(column)}
              aria-pressed={$sortKey === column}
            >
              <span class="header-label">{$columnLabels.get(column) ?? column}</span>
              {#if $sortKey === column}
                <span class="sort-indicator" aria-hidden="true">
                  {$sortDirection === 'asc' ? '▲' : '▼'}
                </span>
              {/if}
            </button>
          {/each}
        </div>
      </div>
      <div
        class="virtual-viewport"
        bind:this={$bodyScrollEl}
        bind:clientHeight={$viewportHeight}
        on:scroll={handleScroll}
        on:wheel={forwardWheel}
      >
        <div class="virtual-spacer" style={`height: ${totalHeight}px; width: ${effectiveTableWidth}px;`}>
          <div class="virtual-inner" style={`transform: translateY(${offsetY}px);`}>
            {#each virtualRows as item (item.position)}
              {@const largePage = !item.row && $largePages.has(Math.floor(item.position / PAGE_SIZE))}
              <div
                class="data-row"
                class:alt-row={item.position % 2 === 1}
                class:loading={!item.row && !largePage}
                style={`grid-template-columns: ${gridTemplate}; --row-height: ${ROW_HEIGHT}px;${
                  item.row?.color
                    ? ` background-color: ${displayRowColor(item.row.color, $theme)};`
                    : ''
                }`}
              >
                {#if largePage}
                  <div class="cell index sticky sticky-index">{item.position + 1}</div>
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
                  <div class="cell index sticky sticky-index">{item.row ? item.row.row_index + 1 : item.position + 1}</div>
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
                      <span class="memo-text">{item.row.memo}</span>
                    {:else if item.row}
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
    </div>
  {/if}
</section>
