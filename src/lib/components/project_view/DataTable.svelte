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
  import { columnKindLabel } from '../../columnTypes';
  import { visibleColumnWidth } from '../../columnAutoFit';
  import TableRow from './TableRow.svelte';
  import ColumnResizeHandle from './ColumnResizeHandle.svelte';
  import { frozenColumnLayout, frozenColumnViewport, horizontalScrollForColumn } from '../../frozenColumns';
  import { errorMessage } from '../../errors';
  import {
    ROW_HEIGHT, PAGE_SIZE, SELECT_COL_WIDTH, INDEX_COL_WIDTH, FLAG_COL_WIDTH, MEMO_COL_WIDTH,
    MIN_MEMO_WIDTH, MAX_MEMO_WIDTH,
    STICKY_COLUMNS_WIDTH, MIN_DATA_WIDTH, MAX_DATA_WIDTH, BUFFER,
  } from './state';
  import type { CachedRow, VirtualRow } from './state';
  import type { CellKind, ColumnKind } from '../../types';
  import { headerSelectionState } from '../../selection';
  import CellContextMenu from './CellContextMenu.svelte';
  import ColumnContextMenu from './ColumnContextMenu.svelte';
  import WallpaperLayer from '../app/WallpaperLayer.svelte';

  import {
    logicalRowAt,
    maxScrollTop,
    scrollTopForRow,
    virtualWindow
  } from '../../virtualScroll';
  import { useWorkspace } from "../../workspaceContext";
  import { tableTextSize } from '../../tableTextSize';
  const {
    rowsCache, pendingPages, totalRows, totalFilteredRows, filteredFlaggedCount, visibleColumns,
    orderedColumns, columnOrder, columnFormats,
    sortKey, sortDirection, bodyScrollEl, headerScrollEl, viewportHeight, scrollTop, scrollLeft, rowHeight,
    tableWidth, projectDetail, columnLabels, toggleSort,
    positionToRowIndex, largePages, selectedRowIds, selectedCount,
    selectVisibleRows, rememberOffset, searchColumns, hiddenColumns, frozenThrough, backend,
    appliedResultLoading, appliedFilters, viewGeneration, queryCountKnown, queryError,
  } = useWorkspace();

  export let columnWidths: Map<string, number> = new Map();
  export let noteColumnWidth = MEMO_COL_WIDTH;
  export let ensureRangeLoaded: (start: number, end: number) => void = () => {};
  const dispatch = createEventDispatcher<{
    notify: { message: string; tone: 'success' | 'error' };
    columnRule: { columnId: string };
    columnGroup: { columnId: string };
    columnWidth: { columnId: string; width: number };
    noteWidth: { width: number };
    columnFormat: { columnId: string; format: 'original' | 'iso' };
    retry: void;
    iocRule: { projectId: string; columnId: string; value: string };
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
  let columnDrag: { pointerId: number; projectId: string; generation: number; columnId: string;
    startX: number; startY: number; x: number; y: number; element: HTMLElement; active: boolean } | null = null;
  let columnDragFrame: number | null = null;
  let suppressSortClick = false;
  let previousRowHeight = get(rowHeight);
  let resizingRows = false;
  let rowResizeRevision = 0;
  const emptyColumnKinds: Readonly<Record<string, ColumnKind>> = {};
  $: columnKinds = $projectDetail?.column_kinds ?? emptyColumnKinds;

  const resizeRows = async (height: number) => {
    const previous = previousRowHeight;
    previousRowHeight = height;
    const ticket = ++rowResizeRevision;
    const body = get(bodyScrollEl), total = get(totalFilteredRows);
    if (!body?.isConnected || !total || get(appliedResultLoading)) {
      resizingRows = false;
      return;
    }
    const anchor = logicalRowAt(body.scrollTop, total, body.clientHeight, previous);
    const top = get(scrollTop), filters = get(appliedFilters), generation = get(viewGeneration);
    const sorting = [get(sortKey), get(sortDirection)].join(':');
    syncHeader(body.scrollLeft);
    if (scrollFrame !== null) cancelAnimationFrame(scrollFrame);
    scrollFrame = null;
    pendingScrollEl = null;
    resizingRows = true;
    try {
      // Commit the new spacer height before moving inside its scroll range.
      await tick();
      if (ticket !== rowResizeRevision || body !== get(bodyScrollEl) || !body.isConnected
        || total !== get(totalFilteredRows) || get(appliedResultLoading)
        || filters !== get(appliedFilters) || generation !== get(viewGeneration)
        || sorting !== [get(sortKey), get(sortDirection)].join(':') || top !== get(scrollTop)) return;
      body.scrollTop = scrollTopForRow(anchor, total, body.clientHeight, height);
      scrollTop.set(body.scrollTop);
      rememberOffset(logicalRowAt(body.scrollTop, total, body.clientHeight, height));
    } finally {
      if (ticket === rowResizeRevision) resizingRows = false;
    }
  };
  $: if ($rowHeight !== previousRowHeight) void resizeRows($rowHeight);

  const resolveColumnWidth = (column: string, widths: ReadonlyMap<string, number>) => {
    const width = widths.get(column) ?? MIN_DATA_WIDTH;
    return Math.round(Math.min(MAX_DATA_WIDTH, Math.max(MIN_DATA_WIDTH, width)));
  };

  $: dataWidths = $visibleColumns.map((column) => resolveColumnWidth(column, columnWidths));
  $: stickyWidth = STICKY_COLUMNS_WIDTH - MEMO_COL_WIDTH + noteColumnWidth;
  $: frozenLayout = frozenColumnLayout($orderedColumns, $visibleColumns, dataWidths, $frozenThrough, stickyWidth);
  $: focusedScrollingWidth = focusedColumnId && !frozenLayout.offsets.has(focusedColumnId)
    ? dataWidths[$visibleColumns.indexOf(focusedColumnId)] : undefined;
  $: frozenViewport = frozenColumnViewport(frozenLayout, $visibleColumns.length, stickyWidth,
    $tableWidth, $scrollLeft, focusedScrollingWidth);
  $: columnLayout = tableColumnLayout($visibleColumns, dataWidths, frozenLayout.offsets, stickyWidth);
  $: {
    const next = visibleTableColumns(columnLayout, $scrollLeft, $tableWidth, frozenViewport.edge, renderedColumns, 2, draggedColumnId ?? focusedColumnId);
    if (next !== renderedColumns) renderedColumns = next;
  }
  $: totalDataWidth = dataWidths.reduce((sum, width) => sum + width, 0);
  $: totalTableWidth = stickyWidth + totalDataWidth;
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
    `${noteColumnWidth}px`,
    ...dataWidths.map((width) => `${width}px`)
  ].join(' ');

  // Set CSS variables to specify sticky column positions
  $: if (typeof document !== 'undefined') {
    document.documentElement.style.setProperty('--sticky-index', `${SELECT_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-flag', `${SELECT_COL_WIDTH + INDEX_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-memo', `${SELECT_COL_WIDTH + INDEX_COL_WIDTH + FLAG_COL_WIDTH}px`);
    document.documentElement.style.setProperty('--sticky-data', `${stickyWidth}px`);
  }

  // The applied QuerySpec result is the only source for the table's row count.
  $: effectiveTotalRows = $totalFilteredRows;
  $: windowGeometry = virtualWindow(
    $scrollTop,
    effectiveTotalRows,
    $viewportHeight,
    BUFFER,
    $rowHeight,
  );
  $: startIndex = windowGeometry.start;
  $: endIndex = windowGeometry.end;
  $: offsetY = windowGeometry.innerTop;
  $: totalHeight = windowGeometry.height;
  $: if (!resizingRows && $queryCountKnown) {
    // The explicit first query establishes the result count. Once known, a
    // settled edit can unblock an adjacent page without changing this range.
    $largePages;
    ensureRangeLoaded(startIndex, endIndex);
  }
  $: if (!resizingRows && !pendingScrollEl && $bodyScrollEl
    && maxScrollTop(effectiveTotalRows, $viewportHeight, $rowHeight) >= $scrollTop
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
    const maxScroll = maxScrollTop(effectiveTotalRows, $viewportHeight, $rowHeight);
    if (!resizingRows && $queryCountKnown && !$appliedResultLoading && $pendingPages.size === 0 && $scrollTop > maxScroll) {
      scrollTop.set(maxScroll);
      if ($bodyScrollEl && $bodyScrollEl.scrollTop !== maxScroll) {
        $bodyScrollEl.scrollTop = maxScroll;
      }
    }
  }
  $: projectId = $projectDetail?.project.meta.id ?? '';
  $: if (columnDrag && (columnDrag.projectId !== projectId || columnDrag.generation !== $viewGeneration)) cancelColumnDrag();
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

  const columnHeaderKey = (event: KeyboardEvent, columnId: string) => {
    if (event.key === 'F2' && !event.altKey && !event.ctrlKey && !event.metaKey && !event.shiftKey) {
      event.preventDefault(); event.stopPropagation(); void focusColumnResize(columnId); return;
    }
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
  const focusColumnResize = async (columnId: string) => {
    await focusHeaderColumn(columnId);
    [...(get(headerScrollEl)?.querySelectorAll<HTMLElement>('.column-heading') ?? [])]
      .find((element) => element.dataset.columnId === columnId)
      ?.querySelector<HTMLElement>('.column-resize-handle')?.focus({ preventScroll: true });
  };
  const resizeKeydown = (event: KeyboardEvent, columnId: string, index: number) => {
    if (event.defaultPrevented) return;
    if (event.key === 'Escape') {
      event.preventDefault(); event.stopPropagation(); void focusHeaderColumn(columnId);
    } else columnTab(event, index);
  };
  const fitColumn = (columnId: string | null) => {
    const body = get(bodyScrollEl), header = get(headerScrollEl);
    if (!body?.isConnected || !header || !get(queryCountKnown)) return;
    const heading = columnId === null ? header.querySelector<HTMLElement>('.note-heading')
      : [...header.querySelectorAll<HTMLElement>('.column-heading')].find(element => element.dataset.columnId === columnId);
    if (!heading) return;
    const width = visibleColumnWidth(body, heading, columnId,
      columnId === null ? MIN_MEMO_WIDTH : MIN_DATA_WIDTH, columnId === null ? MAX_MEMO_WIDTH : MAX_DATA_WIDTH);
    if (columnId === null) dispatch('noteWidth', { width });
    else dispatch('columnWidth', { columnId, width });
  };
  const endColumnDrag = () => {
    const gesture = columnDrag;
    columnDrag = null;
    draggedColumnId = null;
    dropTarget = null;
    if (columnDragFrame !== null) cancelAnimationFrame(columnDragFrame);
    columnDragFrame = null;
    window.removeEventListener('pointermove', moveColumnPointer);
    window.removeEventListener('pointerup', dropColumnPointer);
    window.removeEventListener('pointercancel', cancelColumnPointer);
    window.removeEventListener('lostpointercapture', cancelColumnPointer);
    window.removeEventListener('keydown', columnDragKey, true);
    window.removeEventListener('blur', cancelColumnDrag);
    window.removeEventListener('trivium-pause-gestures', cancelColumnDrag);
    if (gesture?.element.hasPointerCapture(gesture.pointerId)) {
      gesture.element.releasePointerCapture(gesture.pointerId);
    }
  };
  const cancelColumnDrag = () => {
    if (!columnDrag) return;
    suppressSortClick = true;
    endColumnDrag();
  };
  const cancelColumnPointer = (event: PointerEvent) => {
    if (!columnDrag || columnDrag.pointerId !== event.pointerId) return;
    // Touch/pen implicitly captures the pressed button. Moving capture to its
    // heading is expected; only losing the active heading's capture cancels.
    if (event.type === 'lostpointercapture' && columnDrag.active && event.target !== columnDrag.element) return;
    cancelColumnDrag();
  };
  const columnDragKey = (event: KeyboardEvent) => {
    if (event.key !== 'Escape' && !isTabKey(event)) return;
    cancelColumnDrag();
    if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); }
  };
  const updateColumnDropTarget = () => {
    const gesture = columnDrag, header = get(headerScrollEl);
    if (!gesture?.active || !header) return;
    // Pointer capture keeps events on the source. Hit-test the actual pointer
    // position so virtualized/frozen headings remain valid drop targets.
    const heading = document.elementFromPoint(gesture.x, gesture.y)?.closest<HTMLElement>('.column-heading[data-column-id]');
    if (!heading || !header.contains(heading)) { dropTarget = null; return; }
    const rect = heading.getBoundingClientRect();
    const id = heading.dataset.columnId!, after = gesture.x >= rect.left + rect.width / 2;
    if (dropTarget?.id !== id || dropTarget.after !== after) dropTarget = { id, after };
  };
  const panColumnDrag = () => {
    columnDragFrame = null;
    const gesture = columnDrag, body = get(bodyScrollEl), header = get(headerScrollEl);
    if (!gesture?.active || !body || !header) return;
    const rect = header.getBoundingClientRect();
    const direction = gesture.y < rect.top || gesture.y > rect.bottom ? 0
      : gesture.x > rect.right - 40 ? 1
      : gesture.x < rect.left + Math.min(frozenViewport.edge, rect.width - 40) + 20 ? -1 : 0;
    const previousLeft = body.scrollLeft;
    if (direction) {
      body.scrollLeft += direction * 14;
      if (body.scrollLeft !== previousLeft) queueScroll(body);
    }
    updateColumnDropTarget();
    if (body.scrollLeft !== previousLeft) columnDragFrame = requestAnimationFrame(panColumnDrag);
  };
  const moveColumnPointer = (event: PointerEvent) => {
    const gesture = columnDrag;
    if (!gesture || event.pointerId !== gesture.pointerId) return;
    gesture.x = event.clientX;
    gesture.y = event.clientY;
    if (!gesture.active) {
      if (Math.hypot(gesture.x - gesture.startX, gesture.y - gesture.startY) < 5) return;
      gesture.active = true;
      draggedColumnId = focusedColumnId = gesture.columnId;
      cellMenu = columnMenu = null;
      gesture.element.setPointerCapture(gesture.pointerId);
    }
    event.preventDefault();
    updateColumnDropTarget();
    if (columnDragFrame === null) columnDragFrame = requestAnimationFrame(panColumnDrag);
  };
  const dropColumnPointer = (event: PointerEvent) => {
    const gesture = columnDrag;
    if (!gesture || event.pointerId !== gesture.pointerId) return;
    gesture.x = event.clientX;
    gesture.y = event.clientY;
    updateColumnDropTarget();
    const target = dropTarget;
    if (gesture.active) { event.preventDefault(); suppressSortClick = true; }
    endColumnDrag();
    if (gesture.active && target && gesture.projectId === projectId && gesture.generation === $viewGeneration) {
      void moveColumn(gesture.columnId, target.id, target.after);
    }
  };
  const beginColumnDrag = (event: PointerEvent, columnId: string) => {
    if (!event.isPrimary || event.button !== 0 || !projectId || event.altKey || event.ctrlKey || event.metaKey
      || !(event.target instanceof Element) || !event.target.closest('.column-sort')) return;
    endColumnDrag();
    suppressSortClick = false;
    columnDrag = { pointerId: event.pointerId, projectId, generation: $viewGeneration, columnId,
      startX: event.clientX, startY: event.clientY, x: event.clientX, y: event.clientY,
      element: event.currentTarget as HTMLElement, active: false };
    window.addEventListener('pointermove', moveColumnPointer, { passive: false });
    window.addEventListener('pointerup', dropColumnPointer);
    window.addEventListener('pointercancel', cancelColumnPointer);
    window.addEventListener('lostpointercapture', cancelColumnPointer);
    window.addEventListener('keydown', columnDragKey, true);
    window.addEventListener('blur', cancelColumnDrag);
    window.addEventListener('trivium-pause-gestures', cancelColumnDrag);
  };
  const sortColumn = (event: MouseEvent, columnId: string) => {
    if (suppressSortClick && event.detail > 0) {
      suppressSortClick = false;
      event.preventDefault(); event.stopPropagation(); return;
    }
    toggleSort(columnId);
  };

  const buildVirtualRows = (
    start: number,
    end: number,
    cache: Map<number, CachedRow>,
    posMap: Map<number, number>
  ): VirtualRow[] => {
    if (end <= start) {
      return [];
    }
    const result: VirtualRow[] = [];
    // Backend result positions identify the original rows, including after sorting.
    for (let position = start; position < end; position += 1) {
      const rowIndex = posMap.get(position) ?? null;
      const row = rowIndex !== null ? cache.get(rowIndex) ?? null : null;
      result.push({ position, row });
    }
    return result;
  };

  // A first response updates spacer geometry before its rows are published.
  // Keep that loading state free of evidence controls; normal paging stays live.
  $: virtualRows = $queryCountKnown ? buildVirtualRows(startIndex, endIndex, $rowsCache, $positionToRowIndex) : [];
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
    rememberOffset(logicalRowAt(top, $totalFilteredRows, $viewportHeight, $rowHeight));
    syncHeader(target.scrollLeft);
  };

  const queueScroll = (target: HTMLDivElement) => {
    if (!target.isConnected) return;
    rememberOffset(logicalRowAt(target.scrollTop, $totalFilteredRows, $viewportHeight, $rowHeight));
    mirrorHorizontal(target.scrollLeft);
    if (pendingScrollEl !== target) pendingScrollEl = target;
    if (scrollFrame === null) scrollFrame = requestAnimationFrame(flushScroll);
  };

  const handleScroll = (event: Event) => {
    const target = event.currentTarget as HTMLDivElement;
    // Native scrolling has already moved the viewport. Let Svelte update its
    // rows in this event's microtask instead of waiting for another frame.
    if (!resizingRows && target.isConnected && $scrollTop !== target.scrollTop) {
      scrollTop.set(target.scrollTop);
    }
    queueScroll(target);
  };

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
    $bodyScrollEl.scrollLeft = horizontalScrollForColumn(index, $visibleColumns, dataWidths,
      frozenLayout, stickyWidth, $bodyScrollEl.scrollLeft, $bodyScrollEl.clientWidth);
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
      if (resizingRows || !target?.isConnected || top > maxScrollTop($totalFilteredRows, $viewportHeight, $rowHeight)
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
      endColumnDrag();
      rowResizeRevision += 1;
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
      {#if $queryCountKnown}
        <strong>{$totalFilteredRows.toLocaleString()}</strong>
        <span>of {$totalRows.toLocaleString()} rows</span>
      {:else}
        <strong>{$queryError ? 'Result unavailable' : 'Loading rows…'}</strong>
        <span>· {$totalRows.toLocaleString()} source rows</span>
      {/if}
      {#if frozenViewport.overflow}
        <span class="meta-dot">·</span>
        <span class="frozen-overflow-hint" title="The frozen data columns are wider than the available space. Scroll horizontally or focus a column to reveal them; annotation columns stay fixed.">Wide frozen columns · scroll horizontally</span>
      {/if}
      {#if $selectedCount > 0}
        <span class="meta-dot">·</span>
        <span><strong>{$selectedCount}</strong> selected</span>
      {/if}
    </div>
    <span class="table-meta-right">Flagged {$queryCountKnown ? $filteredFlaggedCount.toLocaleString() : '—'}</span>
  </div>
  {#if $queryError && $queryCountKnown}
    <div class="query-page-error" role="alert">
      <span>Some rows could not be loaded. {$queryError.message}</span>
      <button type="button" on:click={() => dispatch('retry')}>Retry</button>
    </div>
  {/if}
  {#if $queryCountKnown && !$queryError && effectiveTotalRows === 0 && $pendingPages.size === 0}
    <WallpaperLayer>
    <div class="empty-rows">
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round">
        <circle cx="11" cy="11" r="7" />
        <path d="m20 20-3.5-3.5" />
      </svg>
      <p>No rows match your filters.</p>
      <span>Adjust the search or clear a filter to see more rows.</span>
    </div>
    </WallpaperLayer>
  {:else}
    <div class="table-scroll" style={`--table-text-scale: ${$tableTextSize / 100}; --row-height: ${$rowHeight}px;`}>
      <div
        class="table-header-scroll"
        role="presentation"
        bind:this={$headerScrollEl}
        on:wheel={forwardWheel}
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
            />
          </div>
          <div class="header-cell sticky sticky-index">#</div>
          <div class="header-cell sticky sticky-flag">Flag</div>
          <div class="header-cell sticky sticky-memo note-heading" class:frozenDataEdge={frozenLayout.offsets.size === 0}>
            <span class="header-label">Note</span>
            <ColumnResizeHandle label="Note" width={noteColumnWidth} minWidth={MIN_MEMO_WIDTH} maxWidth={MAX_MEMO_WIDTH}
              tabIndex={0} on:resize={(event) => dispatch('noteWidth', event.detail)}
              on:fit={() => fitColumn(null)}
              on:keydown={(event) => { if (isTabKey(event) && !event.shiftKey) columnTab(event, -1); }} />
          </div>
          {#each renderedColumns as column (column.id)}
            <div
              class="header-cell column-heading" role="group"
              aria-label={`Column ${$columnLabels.get(column.id) ?? column.id}`}
              class:sticky={column.frozen}
              class:stickyData={column.frozen}
              class:lastFrozenData={frozenLayout.lastColumn === column.id}
              data-column-id={column.id}
              data-frozen={column.frozen}
              class:dragging={draggedColumnId === column.id}
              class:dropBefore={dropTarget?.id === column.id && !dropTarget.after}
              class:dropAfter={dropTarget?.id === column.id && dropTarget.after}
              on:pointerdown={(event) => beginColumnDrag(event, column.id)}
              style={`grid-column: ${column.index + 5};${column.frozen ? ` --sticky-data-left: ${(frozenLayout.offsets.get(column.id) ?? 0) - frozenViewport.pan}px;` : ''}`}
              class:sorted={$sortKey === column.id}
            >
              <button type="button" class="column-sort"
                on:click={(event) => sortColumn(event, column.id)}
                on:focus={() => focusColumn(column.index)}
                on:contextmenu={(event) => openColumnMenu(event, column.id)}
                on:keydown={(event) => { columnTab(event, column.index); if (!event.defaultPrevented) columnHeaderKey(event, column.id); }}
                aria-pressed={$sortKey === column.id}
                aria-label={$columnLabels.get(column.id) ?? column.id}
                aria-describedby={`column-kind-${column.index}`}
                aria-keyshortcuts="F2"
                title={`Sort ${$columnLabels.get(column.id) ?? column.id}; drag to move; F2 to resize; right-click for actions`}>
                <span class="column-title">
                  <span class="header-label">{$columnLabels.get(column.id) ?? column.id}</span>
                  <span class="column-type" id={`column-kind-${column.index}`}
                    data-column-kind={columnKinds[column.id] ?? 'unknown'}
                    title={`Inferred type: ${columnKindLabel(columnKinds[column.id])}`}>
                    {columnKindLabel(columnKinds[column.id])}
                  </span>
                </span>
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
                on:keydown={(event) => resizeKeydown(event, column.id, column.index)}
                on:fit={() => fitColumn(column.id)}
                on:resize={(event) => dispatch('columnWidth', { columnId: column.id, width: event.detail.width })} />
            </div>
          {/each}
        </div>
      </div>
      {#if !$queryCountKnown}
        <div class="table-query-state" role={$queryError ? 'alert' : 'status'} aria-live="polite">
          {#if $queryError}
            <p>Could not load rows.</p>
            <span>{$queryError.message}</span>
            <button type="button" on:click={() => dispatch('retry')}>Retry</button>
          {:else}
            <p>Loading rows…</p>
          {/if}
        </div>
      {/if}
      <WallpaperLayer pinnedWidth={Math.max(stickyWidth, frozenViewport.edge)}>
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
                columns={renderedColumns} columnCount={$visibleColumns.length} {gridTemplate} {columnKinds}
                frozenOffsets={frozenLayout.offsets} frozenPan={frozenViewport.pan} lastFrozen={frozenLayout.lastColumn}
                {focusColumn} {openCellMenu} {columnTab}
              />
            {/each}
          </div>
        </div>
      </div>
      </WallpaperLayer>
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
      on:iocRule={(event) => dispatch('iocRule', event.detail)}
    />
  {/key}
{/if}

{#if columnMenu}
  {#key columnMenu}
    <ColumnContextMenu columnId={columnMenu.columnId}
      label={$columnLabels.get(columnMenu.columnId) ?? columnMenu.columnId}
      frozen={frozenLayout.offsets.has(columnMenu.columnId)}
      displayFormat={$columnFormats[columnMenu.columnId] ?? 'original'}
      columnKind={columnKinds[columnMenu.columnId]}
      x={columnMenu.x} y={columnMenu.y}
      on:search={(event) => searchColumn(event.detail.columnId)}
      on:hide={(event) => void hideColumn(event.detail.columnId)}
      on:freeze={(event) => freezeColumn(event.detail.columnId)}
      on:unfreeze={unfreezeColumns}
      on:format={(event) => { columnMenu = null; dispatch('columnFormat', event.detail); }}
      on:rule={(event) => { columnMenu = null; dispatch('columnRule', event.detail); }}
      on:group={(event) => { columnMenu = null; dispatch('columnGroup', event.detail); }}
      on:close={(event) => {
        if (event.detail.restoreFocus) columnMenu?.trigger.focus({ preventScroll: true });
        columnMenu = null;
      }} />
  {/key}
{/if}

<style>
  .column-title { display: inline-flex; align-items: center; gap: 5px; min-width: 0; }
  .column-title .header-label { min-width: 0; }
  .column-type { flex: 0 0 auto; color: var(--muted); font-size: .9em; font-weight: 400; letter-spacing: 0; text-transform: none; }
  .column-sort { user-select: none; -webkit-user-select: none; touch-action: none; }
  .column-heading.dragging .column-sort { cursor: grabbing; }
  .table-query-state { position: absolute; inset: 38px 0 0; z-index: 20; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 8px; padding: 18px; background: var(--surface); text-align: center; }
  .table-query-state p { margin: 0; color: var(--fg); font-size: 13px; }
  .table-query-state span { max-width: 100%; white-space: pre-wrap; overflow-wrap: anywhere; color: var(--muted); font-size: 11px; }
  .query-page-error { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 5px 8px; color: var(--fg); background: var(--surface-weak); border: 1px solid var(--border); font-size: 11px; }
  .query-page-error span { min-width: 0; overflow-wrap: anywhere; }
  .query-page-error button, .table-query-state button { flex-shrink: 0; padding: 4px 9px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); font-size: 11px; }
</style>
