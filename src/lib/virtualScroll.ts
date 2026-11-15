/**
 * Physical/logical row mapping for the fixed-height virtual table.
 *
 * Rows have one shared height at each table text size. The spacer height is
 * capped, so a result with millions of rows does not request a DOM element
 * taller than the browser can represent. All conversions between a physical
 * `scrollTop` and a result-relative logical row live here; other components
 * must not re-derive the formula.
 */

/** Height of one rendered row. Row height is presentation, not scroll scale. */
export const ROW_HEIGHT = 32;

/** Upper bound for the physical scroll range (pixels). */
export const MAX_PHYSICAL_HEIGHT = 8_000_000;

const clamp = (value: number, min: number, max: number) =>
  Math.min(max, Math.max(min, value));

/** Number of rows in the result set, normalized to a non-negative integer. */
function normalizeTotal(totalRows: number): number {
  if (!Number.isFinite(totalRows) || totalRows <= 0) {
    return 0;
  }
  return Math.floor(totalRows);
}

function normalizeViewport(height: number): number {
  return Number.isFinite(height) ? Math.max(0, height) : 0;
}

/** Physical spacer height for a result set. */
export function physicalHeight(totalRows: number, rowHeight = ROW_HEIGHT): number {
  const total = normalizeTotal(totalRows);
  return Math.min(total * rowHeight, MAX_PHYSICAL_HEIGHT);
}

/** Largest useful `scrollTop` for the current result and viewport. */
export function maxScrollTop(
  totalRows: number,
  viewportHeight: number,
  rowHeight = ROW_HEIGHT,
): number {
  const viewport = normalizeViewport(viewportHeight);
  return Math.max(0, physicalHeight(totalRows, rowHeight) - viewport);
}

/** Clamps a zero-based result row to the current result set. */
export function clampRow(row: number, totalRows: number): number {
  const total = normalizeTotal(totalRows);
  if (total === 0) {
    return 0;
  }
  if (!Number.isFinite(row)) {
    return 0;
  }
  return clamp(Math.floor(row), 0, total - 1);
}

/** Full-size pixel offset represented by the bounded physical scroll range. */
export function logicalOffsetAt(
  scrollTop: number,
  totalRows: number,
  viewportHeight: number,
  rowHeight = ROW_HEIGHT,
): number {
  const total = normalizeTotal(totalRows);
  const physicalMax = maxScrollTop(total, viewportHeight, rowHeight);
  if (physicalMax === 0) return 0;
  const top = Number.isFinite(scrollTop) ? clamp(scrollTop, 0, physicalMax) : 0;
  const logicalMax = Math.max(0, total * rowHeight - normalizeViewport(viewportHeight));
  if (logicalMax === physicalMax) return top;
  return (top / physicalMax) * logicalMax;
}

/** First logical row intersecting the top of the viewport. */
export function logicalRowAt(
  scrollTop: number,
  totalRows: number,
  viewportHeight: number,
  rowHeight = ROW_HEIGHT,
): number {
  const total = normalizeTotal(totalRows);
  if (total === 0) {
    return 0;
  }
  return clampRow(logicalOffsetAt(scrollTop, total, viewportHeight, rowHeight) / rowHeight, total);
}

/**
 * Physical scroll position for the top of a logical row. The result is
 * bounded by the viewport so the last viewport can be reached; callers can
 * also use it directly to position a row at the top when possible.
 */
export function scrollTopForRow(
  rowIndex: number,
  totalRows: number,
  viewportHeight: number,
  rowHeight = ROW_HEIGHT,
): number {
  const total = normalizeTotal(totalRows);
  if (total === 0) {
    return 0;
  }
  const logicalMax = Math.max(0, total * rowHeight - normalizeViewport(viewportHeight));
  if (logicalMax === 0) return 0;
  const logicalTop = Math.min(clampRow(rowIndex, total) * rowHeight, logicalMax);
  const physicalMax = maxScrollTop(total, viewportHeight, rowHeight);
  return logicalMax === physicalMax ? logicalTop : (logicalTop / logicalMax) * physicalMax;
}

/**
 * One geometry contract for DOM placement and backend page selection.
 *
 * Rows remain rowHeight pixels apart. The absolute inner top compensates for
 * the difference between physical and logical offsets; the browser itself
 * subtracts physical scrollTop exactly once. The extra end row handles a
 * partially visible row when the viewport height is not a multiple of rowHeight.
 */
export function virtualWindow(
  scrollTop: number,
  totalRows: number,
  viewportHeight: number,
  bufferRows = 0,
  rowHeight = ROW_HEIGHT,
): { start: number; end: number; innerTop: number; height: number } {
  const total = normalizeTotal(totalRows);
  const viewport = normalizeViewport(viewportHeight) || rowHeight;
  const buffer = Number.isFinite(bufferRows) ? Math.max(0, Math.floor(bufferRows)) : 0;
  const physicalTop = Number.isFinite(scrollTop)
    ? clamp(scrollTop, 0, maxScrollTop(total, viewport, rowHeight)) : 0;
  const logicalTop = logicalOffsetAt(physicalTop, total, viewport, rowHeight);
  const start = Math.max(0, Math.floor(logicalTop / rowHeight) - buffer);
  const end = Math.min(total, Math.ceil((logicalTop + viewport) / rowHeight) + buffer);
  return {
    start,
    end,
    innerTop: physicalTop + start * rowHeight - logicalTop,
    height: physicalHeight(total, rowHeight),
  };
}
