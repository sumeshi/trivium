/**
 * Physical/logical row mapping for the fixed-height virtual table.
 *
 * The rendered row height never changes. Only the scrollable spacer height is
 * capped, so a result with millions of rows does not request a DOM element
 * taller than the browser can represent. All conversions between a physical
 * `scrollTop` and a result-relative logical row live here; other components
 * must not re-derive the formula.
 */

/** Height of one rendered row. Row height is presentation, not scroll scale. */
export const ROW_HEIGHT = 56;

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

/** True when the full logical height would exceed the physical cap. */
function isCompressed(totalRows: number): boolean {
  return totalRows * ROW_HEIGHT > MAX_PHYSICAL_HEIGHT;
}

/** Physical spacer height for a result set. */
export function physicalHeight(totalRows: number): number {
  const total = normalizeTotal(totalRows);
  return Math.min(total * ROW_HEIGHT, MAX_PHYSICAL_HEIGHT);
}

/** Largest useful `scrollTop` for the current result and viewport. */
export function maxScrollTop(
  totalRows: number,
  viewportHeight: number
): number {
  const viewport = Number.isFinite(viewportHeight)
    ? Math.max(0, viewportHeight)
    : 0;
  return Math.max(0, physicalHeight(totalRows) - viewport);
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

/**
 * Logical row used as the anchor at the current physical scroll position.
 *
 * For an uncompressed result this is `scrollTop / ROW_HEIGHT`. For a
 * compressed result the scroll fraction is expanded across the logical rows.
 * At the very bottom of a compressed result the anchor is the final row, so
 * the last page is requested and the final rows are reachable; the table also
 * clamps its display start to keep the last viewport full.
 */
export function logicalRowAt(
  scrollTop: number,
  totalRows: number,
  viewportHeight: number
): number {
  const total = normalizeTotal(totalRows);
  if (total === 0) {
    return 0;
  }
  const top = Number.isFinite(scrollTop) ? Math.max(0, scrollTop) : 0;
  if (
    isCompressed(total) &&
    top > 0 &&
    top >= maxScrollTop(total, viewportHeight)
  ) {
    return total - 1;
  }
  return clampRow((top * total) / physicalHeight(total), total);
}

/**
 * Physical scroll position for the top of a logical row. The result is
 * bounded by the viewport so the last viewport can be reached; callers can
 * also use it directly to position a row at the top when possible.
 */
export function scrollTopForRow(
  rowIndex: number,
  totalRows: number,
  viewportHeight: number
): number {
  const total = normalizeTotal(totalRows);
  if (total === 0) {
    return 0;
  }
  const row = clampRow(rowIndex, total);
  const height = physicalHeight(total);
  const rowTop = isCompressed(total)
    ? Math.floor((row * height) / total)
    : row * ROW_HEIGHT;
  return Math.min(rowTop, maxScrollTop(total, viewportHeight));
}
