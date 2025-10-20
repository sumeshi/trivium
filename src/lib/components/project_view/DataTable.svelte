<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import {
    rowsCache,
    pendingPages,
    totalRows,
    flaggedCount,
    visibleColumns,
    sortKey,
    sortDirection,
    bodyScrollEl,
    headerScrollEl,
    viewportHeight,
    scrollTop,
    tableWidth,
    search,
    flagFilter,
    ROW_HEIGHT,
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
    toggleSort,
    FLAG_OPTIONS,
    normalizeFlag,
    setFlag,
    editMemo,
    openCell,
    handleCellKeydown
  } from './state';
  import type { CachedRow, VirtualRow } from './state';

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

  // CSS変数を設定してsticky列の位置を指定
  $: if (typeof document !== 'undefined') {
    document.documentElement.style.setProperty('--sticky-flag', `${INDEX_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-memo', `${INDEX_COL_WIDTH + FLAG_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-data', `${INDEX_COL_WIDTH + FLAG_COL_WIDTH + MEMO_COL_WIDTH}px`);
  }

  $: visibleCount =
    Math.ceil(($viewportHeight || ROW_HEIGHT) / ROW_HEIGHT) + BUFFER * 2;
  $: maxStart = Math.max(0, $totalRows - visibleCount);
  $: startIndex = Math.min(
    maxStart,
    Math.max(0, Math.floor($scrollTop / ROW_HEIGHT) - BUFFER)
  );
  $: endIndex = Math.min($totalRows, startIndex + visibleCount);
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

  $: virtualRows = buildVirtualRows(startIndex, endIndex, $rowsCache);

  const handleScroll = (event: Event) => {
    const target = event.currentTarget as HTMLDivElement;
    scrollTop.set(target.scrollTop);
    
    // スクロールタイムアウトをクリア
    if (scrollTimeout !== null) {
      clearTimeout(scrollTimeout);
    }
    
    // デバウンス処理でスクロール同期を実行
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
      // より確実な同期のため、フラグを即座にリセット
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
    <span>{$totalRows} rows</span>
    <span>{$flaggedCount} flagged</span>
  </div>
  {#if $totalRows === 0}
    <div class="empty-rows">
      {$pendingPages.size > 0 ? 'Loading rows…' : 'No rows match your filters.'}
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
              <span class="header-label">{column}</span>
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
              <div
                class="data-row"
                class:alt-row={item.position % 2 === 1}
                class:loading={!item.row}
                style={`grid-template-columns: ${gridTemplate}; --row-height: ${ROW_HEIGHT}px;`}
              >
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
                          on:click={() => _setFlag(item.row, option.value)}
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
                  on:click={() => item.row && _editMemo(item.row)}
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
                  {@const cellValue = item.row ? item.row.displayCache[column] ?? '' : ''}
                  <button
                    type="button"
                    class="cell"
                    class:sticky={columnIndex === 0}
                    class:stickyData={columnIndex === 0}
                    title={item.row ? cellValue : 'Loading…'}
                    on:click={() => {
                      if (!item.row) return;
                      _openCell(column, cellValue);
                    }}
                    on:keydown={(event) => {
                      if (!item.row) return;
                      _handleCellKeydown(event, column, cellValue);
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
