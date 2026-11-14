/** Stable source-column geometry shared by the table header and virtual rows. */
export interface TableColumn {
  id: string;
  index: number;
  left: number;
  width: number;
  frozen: boolean;
}

export function tableColumnLayout(
  columns: readonly string[], widths: readonly number[], frozen: ReadonlyMap<string, number>, annotationWidth: number,
): TableColumn[] {
  let left = annotationWidth;
  return columns.map((id, index) => {
    const width = Number.isFinite(widths[index]) ? Math.max(0, widths[index]) : 0;
    const column = { id, index, left, width, frozen: frozen.has(id) };
    left += width;
    return column;
  });
}

/**
 * Keep fixed columns and only the scrolling columns intersecting the uncovered
 * viewport, with a small column overscan. Their original grid indexes remain
 * authoritative: omitted cells never change widths or the frozen boundary.
 * Returning the previous array for the same window keeps immutable row props
 * stable during vertical scrolling and small horizontal movements.
 */
export function visibleTableColumns(
  layout: readonly TableColumn[], scrollLeft: number, viewportWidth: number, frozenEdge: number,
  previous: readonly TableColumn[] = [], overscan = 2, focusedId: string | null = null,
): readonly TableColumn[] {
  const scrolling = layout.filter((column) => !column.frozen);
  const left = Math.max(0, scrollLeft) + Math.min(Math.max(0, viewportWidth), Math.max(0, frozenEdge));
  const right = Math.max(0, scrollLeft) + Math.max(0, viewportWidth);
  let start = 0;
  while (start < scrolling.length && scrolling[start].left + scrolling[start].width <= left) start += 1;
  let end = start;
  while (end < scrolling.length && scrolling[end].left < right) end += 1;
  const buffer = Number.isFinite(overscan) ? Math.max(0, Math.floor(overscan)) : 0;
  start = Math.max(0, start - buffer);
  end = Math.min(scrolling.length, end + buffer);
  const included = new Set(scrolling.slice(start, end));
  const next = layout.filter((column) => column.frozen || included.has(column) || column.id === focusedId);
  return next.length === previous.length && next.every((column, index) => column === previous[index]) ? previous : next;
}
