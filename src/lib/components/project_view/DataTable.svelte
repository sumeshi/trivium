<script lang="ts">
  import { createEventDispatcher, onMount, tick } from 'svelte';
  import { get } from 'svelte/store';
  import { tableColumnLayout, visibleTableColumns } from '../../columnWindow';
  import type { TableColumn } from '../../columnWindow';
  import { recycleSlots } from '../../renderSlots';
  import type { RenderSlot } from '../../renderSlots';
  import { isTabKey } from '../../keyboard';
  import { protectRecycledActivation } from '../../recycledActivation';
  import { moveDisplayColumn } from '../../columnLayoutActions';
  import TableRow from './TableRow.svelte';
  import ColumnResizeHandle from './ColumnResizeHandle.svelte';
  import { frozenColumnLayout, frozenColumnViewport, horizontalScrollForColumn } from '../../frozenColumns';
  import { errorMessage } from '../../errors';
  import {
    ROW_HEIGHT, PAGE_SIZE, SELECT_COL_WIDTH, INDEX_COL_WIDTH, FLAG_COL_WIDTH, MEMO_COL_WIDTH,
    STICKY_COLUMNS_WIDTH, MIN_DATA_WIDTH, MAX_DATA_WIDTH, BUFFER,
  } from './state';
  import type { CachedRow, VirtualRow } from './state';
  import type { CellKind } from '../../types';
  import { headerSelectionState } from '../../selection';
  import CellContextMenu from './CellContextMenu.svelte';
  import ColumnContextMenu from './ColumnContextMenu.svelte';

  import {
    logicalRowAt,
    maxScrollTop,
    virtualWindow
  } from '../../virtualScroll';
  import { useWorkspace } from "../../workspaceContext";
  const {
    rowsCache, pendingPages, totalRows, totalFilteredRows, filteredFlaggedCount, visibleColumns,
    orderedColumns, columnOrder, columnFormats,
    sortKey, sortDirection, bodyScrollEl, headerScrollEl, viewportHeight, scrollTop, scrollLeft,
    tableWidth, projectDetail, columnLabels, toggleSort,
    positionToRowIndex, largePages, selectedRowIds, selectedCount,
    selectVisibleRows, rememberOffset, searchColumns, hiddenColumns, frozenThrough, backend,
  } = useWorkspace();

  const _toggleSort = toggleSort;

  export let columnWidths: Map<string, number> = new Map();
  export let ensureRangeLoaded: (start: number, end: number) => void = () => {};
  const dispatch = createEventDispatcher<{
    notify: { message: string; tone: 'success' | 'error' };
    columnRule: { columnId: string };
    columnGroup: { columnId: string };
    columnWidth: { columnId: string; width: number };
    columnFormat: { columnId: string; format: 'original' | 'iso' };
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
  let focusedScrollingWidth: number | undefined;
  let renderedColumns: readonly TableColumn[] = [];
  let rowSlots: readonly RenderSlot<VirtualRow>[] = [];
  let focusedColumnId: string | null = null;
  let scrollFrame: number | null = null;
  let pendingScrollEl: HTMLDivElement | null = null;
  let draggedColumnId: string | null = null;
  let dropTarget: { id: string; after: boolean } | null = null;

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
  $: frozenLayout = frozenColumnLayout($orderedColumns, $visibleColumns, distributedDataWidths, $frozenThrough, STICKY_COLUMNS_WIDTH);
  $: focusedScrollingWidth = focusedColumnId && !frozenLayout.offsets.has(focusedColumnId)
    ? distributedDataWidths[$visibleColumns.indexOf(focusedColumnId)] : undefined;
  $: frozenViewport = frozenColumnViewport(frozenLayout, $visibleColumns.length, STICKY_COLUMNS_WIDTH,
    $tableWidth, $scrollLeft, focusedScrollingWidth);
  $: columnLayout = tableColumnLayout($visibleColumns, distributedDataWidths, frozenLayout.offsets, STICKY_COLUMNS_WIDTH);
  $: {
    const next = visibleTableColumns(columnLayout, $scrollLeft, $tableWidth, frozenViewport.edge, renderedColumns, 2, draggedColumnId ?? focusedColumnId);
    if (next !== renderedColumns) renderedColumns = next;
  }
  $: totalDataWidth = distributedDataWidths.reduce((sum, width) => sum + width, 0);
  $: totalTableWidth = STICKY_COLUMNS_WIDTH + totalDataWidth;
  $: effectiveTableWidth = Math.max(totalTableWidth, $tableWidth);
  $: horizontalMaximum = Math.max(0, effectiveTableWidth - $tableWidth);
  // Until rAF publishes the body's latest offset, the store is intentionally
  // older. Feeding it through the external scrollbar would undo that gesture.
  $: if (!pendingScrollEl && horizontalScrollEl && horizontalScrollEl.scrollLeft !== $scrollLeft) {
    const footer = horizontalScrollEl;
    footer.scrollLeft = $scrollLeft;
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
  $: ensureRangeLoaded(startIndex, endIndex);
  $: if (!pendingScrollEl && $bodyScrollEl
    && maxScrollTop(effectiveTotalRows, $viewportHeight) >= $scrollTop
    && Math.abs($bodyScrollEl.scrollTop - $scrollTop) > 1) {
    $bodyScrollEl.scrollTop = $scrollTop;
  }
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
  $: projectId = $projectDetail?.project.meta.id ?? '';
  const headerActivationIdentity = (target: EventTarget | null) => ({
    projectId, rowIndex: null,
    columnId: target instanceof Element ? target.closest<HTMLElement>('[data-column-id]')?.dataset.columnId ?? null : null,
  });
  $: if (cellMenu && cellMenu.projectId !== projectId) {
    cellMenu = null;
  }

  const openCellMenu = (event: MouseEvent, row: CachedRow, column: string) => {
    event.preventDefault();
    if (!projectId) return;
    focusedColumnId = column;
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
    focusedColumnId = columnId;
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

  const hideColumn = async (columnId: string) => {
    const service = get(backend), detail = get(projectDetail);
    if (!service || !detail?.columns.includes(columnId)) return;
    const previousIndex = get(visibleColumns).indexOf(columnId);
    const nextHidden = new Set(get(hiddenColumns));
    nextHidden.add(columnId);
    hiddenColumns.set(nextHidden);
    columnMenu = null;
    const pending = service.setHiddenColumns({ projectId: detail.project.meta.id, hiddenColumns: [...nextHidden] });
    await tick();
    if (get(projectDetail)?.project.meta.id === detail.project.meta.id) {
      const remaining = get(visibleColumns);
      const next = remaining[Math.min(previousIndex, remaining.length - 1)];
      if (next) await focusHeaderColumn(next);
      else document.querySelector<HTMLButtonElement>('[aria-label="Choose visible columns"]')?.focus();
    }
    try { await pending; }
    catch (error) { dispatch('notify', { message: errorMessage(error, 'Failed to save visible columns.'), tone: 'error' }); }
  };
  const freezeColumn = async (columnId: string) => {
    if (!$projectDetail?.columns.includes(columnId)) return;
    frozenThrough.set(columnId);
    columnMenu = null;
    await tick();
    await focusHeaderColumn(columnId);
  };
  const unfreezeColumns = async () => {
    const columnId = columnMenu?.columnId;
    frozenThrough.set(''); columnMenu = null;
    await tick();
    if (columnId) await focusHeaderColumn(columnId);
  };

  const moveColumn = async (columnId: string, target: string, after = false) => {
    const order = get(orderedColumns);
    const next = moveDisplayColumn(order, columnId, target, after);
    columnMenu = null;
    if (next === order) return;
    columnOrder.set([...next]);
    await tick();
    await focusHeaderColumn(columnId);
  };
  const moveColumnBy = (columnId: string, direction: -1 | 1) => {
    const visible = get(visibleColumns);
    const target = visible[visible.indexOf(columnId) + direction];
    if (target) void moveColumn(columnId, target, direction > 0);
  };
  const resizeColumn = async (columnId: string) => {
    columnMenu = null;
    await focusHeaderColumn(columnId);
    [...(get(headerScrollEl)?.querySelectorAll<HTMLElement>('.column-heading') ?? [])]
      .find((element) => element.dataset.columnId === columnId)
      ?.querySelector<HTMLElement>('.column-resize-handle')?.focus({ preventScroll: true });
  };
  const dragColumn = (event: DragEvent, columnId: string) => {
    if (!event.dataTransfer || !projectId) return;
    columnMenu = null;
    draggedColumnId = columnId;
    focusedColumnId = columnId;
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('application/x-trivium-column', JSON.stringify({ projectId, columnId }));
  };
  const dragOverColumn = (event: DragEvent, columnId: string) => {
    if (!draggedColumnId || !event.dataTransfer) return;
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
    const rect = (event.currentTarget as HTMLElement).getBoundingClientRect();
    dropTarget = { id: columnId, after: event.clientX >= rect.left + rect.width / 2 };
  };
  const scrollDuringDrag = (event: DragEvent) => {
    const body = get(bodyScrollEl), header = get(headerScrollEl);
    if (!draggedColumnId || !body || !header) return;
    const rect = header.getBoundingClientRect();
    const direction = event.clientX > rect.right - 40 ? 1
      : event.clientX < rect.left + Math.min(frozenViewport.edge, rect.width - 40) + 20 ? -1 : 0;
    if (direction) {
      body.scrollLeft += direction * 60;
      queueScroll(body);
    }
  };
  const finishDrag = () => { draggedColumnId = null; dropTarget = null; };
  const dropColumn = (event: DragEvent, target: string) => {
    if (!draggedColumnId || !event.dataTransfer) return;
    event.preventDefault();
    const after = dropTarget?.id === target && dropTarget.after;
    try {
      const source = JSON.parse(event.dataTransfer.getData('application/x-trivium-column'));
      if (source.projectId === projectId && source.columnId === draggedColumnId) {
        void moveColumn(source.columnId, target, after);
      }
    } catch { /* Only columns dragged from this table can change its order. */ }
    finishDrag();
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
  $: {
    const next = recycleSlots(virtualRows, (row) => row.position, rowSlots);
    if (next !== rowSlots) rowSlots = next;
  }
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

  const mirrorHorizontal = (left: number) => {
    const header = get(headerScrollEl), footer = horizontalScrollEl;
    if (header && header.scrollLeft !== left) header.scrollLeft = left;
    if (footer && footer.scrollLeft !== left) footer.scrollLeft = left;
  };

  const syncHeader = (left: number) => {
    mirrorHorizontal(left);
    if ($scrollLeft !== left) scrollLeft.set(left);
  };

  const flushScroll = () => {
    scrollFrame = null;
    const target = pendingScrollEl;
    pendingScrollEl = null;
    if (!target?.isConnected) return;
    const top = target.scrollTop;
    if ($scrollTop !== top) scrollTop.set(top);
    rememberOffset(logicalRowAt(top, $totalFilteredRows, $viewportHeight));
    syncHeader(target.scrollLeft);
  };

  const queueScroll = (target: HTMLDivElement) => {
    if (!target.isConnected) return;
    rememberOffset(logicalRowAt(target.scrollTop, $totalFilteredRows, $viewportHeight));
    mirrorHorizontal(target.scrollLeft);
    if (pendingScrollEl !== target) pendingScrollEl = target;
    if (scrollFrame === null) scrollFrame = requestAnimationFrame(flushScroll);
  };

  const handleScroll = (event: Event) => queueScroll(event.currentTarget as HTMLDivElement);

  // The body owns horizontal position. A delayed header event must never feed
  // an older offset back into it; only the body and external scrollbar publish.
  const handleHorizontalScroll = (event: Event) => {
    const target = event.currentTarget as HTMLDivElement;
    const body = get(bodyScrollEl);
    if (!body || !target.isConnected) return;
    body.scrollLeft = target.scrollLeft;
    queueScroll(body);
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
    if (!$bodyScrollEl) return;
    focusedColumnId = $visibleColumns[index] ?? null;
    $bodyScrollEl.scrollLeft = horizontalScrollForColumn(index, $visibleColumns, distributedDataWidths,
      frozenLayout, STICKY_COLUMNS_WIDTH, $bodyScrollEl.scrollLeft, $bodyScrollEl.clientWidth);
    syncHeader($bodyScrollEl.scrollLeft);
  };

  const trackFocus = (event: FocusEvent) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const cell = target.closest<HTMLElement>('[data-column-id]');
    if (cell && ($headerScrollEl?.contains(cell) || $bodyScrollEl?.contains(cell))) {
      focusedColumnId = cell.dataset.columnId ?? null;
    } else if (!cellMenu && !columnMenu && !target.closest('.cell-dialog')) {
      focusedColumnId = null;
    }
  };

  const focusHeaderColumn = async (column: string) => {
    const index = get(visibleColumns).indexOf(column);
    if (index < 0) return;
    focusColumn(index);
    await tick();
    [...($headerScrollEl?.querySelectorAll<HTMLElement>('.column-heading') ?? [])]
      .find((element) => element.dataset.columnId === column)
      ?.querySelector<HTMLButtonElement>('.column-sort')?.focus({ preventScroll: true });
  };

  const columnTab = (event: KeyboardEvent, index: number, position?: number) => {
    if (!isTabKey(event) || event.altKey || event.ctrlKey || event.metaKey) return;
    const next = index + (event.shiftKey ? -1 : 1);
    if (next < 0 || next >= $visibleColumns.length) return;
    event.preventDefault();
    const column = $visibleColumns[next];
    let destination = position !== undefined && position < startIndex ? undefined : position;
    if (event.shiftKey && index === $visibleColumns.length && position !== undefined) {
      // Native Tab skips unloaded/oversized rows, whose data controls cannot
      // receive focus. Keep that behavior when their columns are virtualized.
      destination = undefined;
      for (let index = virtualRows.length - 1; index >= 0; index -= 1) {
        if (virtualRows[index].position <= position && virtualRows[index].row) {
          destination = virtualRows[index].position;
          break;
        }
      }
    }
    focusColumn(next);
    void tick().then(() => {
      const root = destination === undefined ? $headerScrollEl : $bodyScrollEl;
      const selector = destination === undefined ? '.column-heading' : `.data-row[data-position="${destination}"] > [data-column-id]`;
      const target = [...(root?.querySelectorAll<HTMLElement>(selector) ?? [])]
        .find((element) => element.dataset.columnId === column);
      (destination === undefined ? target?.querySelector<HTMLButtonElement>('.column-sort') : target)?.focus({ preventScroll: true });
    });
  };

  const forwardWheel = (event: WheelEvent) => {
    const body = get(bodyScrollEl);
    if (!body) return;
    const deltaX = event.deltaX || (event.shiftKey ? event.deltaY : 0);
    const deltaY = event.shiftKey && !event.deltaX ? 0 : event.deltaY;
    if (event.currentTarget === body && !deltaX) return;
    let handled = false;
    if (deltaY !== 0) {
      const previousTop = body.scrollTop;
      body.scrollTop += deltaY;
      handled = handled || body.scrollTop !== previousTop;
    }
    if (deltaX !== 0) {
      const previousLeft = body.scrollLeft;
      body.scrollLeft += deltaX;
      handled = handled || body.scrollLeft !== previousLeft;
      queueScroll(body);
    }
    if (handled) {
      event.preventDefault();
    }
  };

  onMount(() => {
    const unsubscribeScroll = scrollTop.subscribe((top) => {
      const target = $bodyScrollEl;
      if (!target?.isConnected || top > maxScrollTop($totalFilteredRows, $viewportHeight)
        || Math.abs(target.scrollTop - top) <= 1) return;
      // The queued frame reads this live element, so a Go/history move wins
      // without discarding a horizontal gesture awaiting the same frame.
      target.scrollTop = top;
    });
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
      unsubscribeScroll();
      if (scrollFrame !== null) cancelAnimationFrame(scrollFrame);
      scrollFrame = null;
      pendingScrollEl = null;
      if (resizeObserver && observedScrollEl) {
        resizeObserver.unobserve(observedScrollEl);
      }
      resizeObserver?.disconnect();
    };
  });

</script>

<svelte:window on:focusin={trackFocus} />

<section class="table-wrapper" data-frozen-pan={frozenViewport.pan}
  data-frozen-max-pan={frozenViewport.maxPan} data-frozen-reserve={frozenViewport.reserve}>
  <div class="table-meta">
    <div class="table-meta-left">
      <strong>{$totalFilteredRows.toLocaleString()}</strong>
      <span>of {$totalRows.toLocaleString()} rows</span>
      {#if frozenViewport.overflow}
        <span class="meta-dot">·</span>
        <span class="frozen-overflow-hint" title="The frozen data columns are wider than the available space. Scroll horizontally or focus a column to reveal them; annotation columns stay fixed.">Wide frozen columns · scroll horizontally</span>
      {/if}
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
        role="presentation"
        bind:this={$headerScrollEl}
        on:wheel={forwardWheel}
        on:dragover={scrollDuringDrag}
      >
        <div
          class="data-header"
          use:protectRecycledActivation={headerActivationIdentity}
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
              on:keydown={(event) => {
                if (isTabKey(event) && !event.shiftKey) columnTab(event, -1);
              }}
            />
          </div>
          <div class="header-cell sticky sticky-index">#</div>
          <div class="header-cell sticky sticky-flag">Flag</div>
          <div class="header-cell sticky sticky-memo" class:frozenDataEdge={frozenLayout.offsets.size === 0}>Note</div>
          {#each renderedColumns as column (column.id)}
            <div
              class="header-cell column-heading" role="group"
              aria-label={`Column ${$columnLabels.get(column.id) ?? column.id}`}
              class:sticky={column.frozen}
              class:stickyData={column.frozen}
              class:lastFrozenData={frozenLayout.lastColumn === column.id}
              data-column-id={column.id}
              data-frozen={column.frozen}
              draggable="true"
              class:dragging={draggedColumnId === column.id}
              class:dropBefore={dropTarget?.id === column.id && !dropTarget.after}
              class:dropAfter={dropTarget?.id === column.id && dropTarget.after}
              on:dragstart={(event) => dragColumn(event, column.id)}
              on:dragover={(event) => dragOverColumn(event, column.id)}
              on:drop={(event) => dropColumn(event, column.id)}
              on:dragend={finishDrag}
              style={`grid-column: ${column.index + 5};${column.frozen ? ` --sticky-data-left: ${(frozenLayout.offsets.get(column.id) ?? 0) - frozenViewport.pan}px;` : ''}`}
              class:sorted={$sortKey === column.id}
            >
              <button type="button" class="column-sort"
                on:click={() => _toggleSort(column.id)}
                on:focus={() => focusColumn(column.index)}
                on:contextmenu={(event) => openColumnMenu(event, column.id)}
                on:keydown={(event) => { columnTab(event, column.index); if (!event.defaultPrevented) columnMenuKey(event, column.id); }}
                aria-pressed={$sortKey === column.id}
                title={`Sort ${$columnLabels.get(column.id) ?? column.id}; drag to move; right-click for actions`}>
                <span class="header-label">{$columnLabels.get(column.id) ?? column.id}</span>
                {#if $sortKey === column.id}
                  <span class="sort-indicator" aria-hidden="true">{$sortDirection === 'asc' ? '▲' : '▼'}</span>
                {/if}
              </button>
              <button type="button" class="column-actions-trigger"
                tabindex="-1"
                aria-label={`Actions for column ${$columnLabels.get(column.id) ?? column.id}`}
                title={`Search or filter ${$columnLabels.get(column.id) ?? column.id}`}
                aria-haspopup="menu" aria-expanded={columnMenu?.columnId === column.id}
                on:click={(event) => openColumnMenu(event, column.id)}
                on:focus={() => focusColumn(column.index)}>
                <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><circle cx="12" cy="5" r="2"/><circle cx="12" cy="12" r="2"/><circle cx="12" cy="19" r="2"/></svg>
              </button>
              <ColumnResizeHandle label={$columnLabels.get(column.id) ?? column.id} width={column.width}
                on:resize={(event) => dispatch('columnWidth', { columnId: column.id, width: event.detail.width })} />
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
            {#each rowSlots as entry (entry.slot)}
              {@const item = entry.value}
              {@const largePage = !item.row && $largePages.has(Math.floor(item.position / PAGE_SIZE))}
              <TableRow
                row={item.row} position={item.position} {projectId}
                selected={item.row ? $selectedRowIds.has(item.row.row_index) : false}
                {largePage} showLargeNotice={item.position === startIndex}
                columns={renderedColumns} columnCount={$visibleColumns.length} {gridTemplate}
                frozenOffsets={frozenLayout.offsets} frozenPan={frozenViewport.pan} lastFrozen={frozenLayout.lastColumn}
                {focusColumn} {openCellMenu} {columnTab}
              />
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
      frozenThrough={$frozenThrough}
      displayFormat={$columnFormats[columnMenu.columnId] ?? 'original'}
      canMoveLeft={$visibleColumns.indexOf(columnMenu.columnId) > 0}
      canMoveRight={$visibleColumns.indexOf(columnMenu.columnId) < $visibleColumns.length - 1}
      x={columnMenu.x} y={columnMenu.y}
      on:search={(event) => searchColumn(event.detail.columnId)}
      on:hide={(event) => void hideColumn(event.detail.columnId)}
      on:freeze={(event) => freezeColumn(event.detail.columnId)}
      on:unfreeze={unfreezeColumns}
      on:move={(event) => moveColumnBy(event.detail.columnId, event.detail.direction)}
      on:resize={(event) => resizeColumn(event.detail.columnId)}
      on:format={(event) => { columnMenu = null; dispatch('columnFormat', event.detail); }}
      on:rule={(event) => { columnMenu = null; dispatch('columnRule', event.detail); }}
      on:group={(event) => { columnMenu = null; dispatch('columnGroup', event.detail); }}
      on:close={(event) => {
        if (event.detail.restoreFocus) columnMenu?.trigger.focus({ preventScroll: true });
        columnMenu = null;
      }} />
  {/key}
{/if}
