/** Missing means the legacy first-visible default; empty means no frozen data. */
export type FrozenThrough = string | undefined;

export interface FrozenColumnLayout {
  offsets: Map<string, number>;
  boundary: string | null;
  lastColumn: string | null;
  width: number;
}

/** Freeze the configured display-order prefix; hidden columns contribute no physical width. */
export function frozenColumnLayout(
  columns: readonly string[], visible: readonly string[], widths: readonly number[],
  frozenThrough: FrozenThrough, annotationWidth: number,
): FrozenColumnLayout {
  const boundary = frozenThrough === undefined ? visible[0] ?? null : frozenThrough || null;
  const sourceIndexes = new Map(columns.map((column, index) => [column, index]));
  const boundaryIndex = boundary === null ? -1 : sourceIndexes.get(boundary) ?? -1;
  const offsets = new Map<string, number>();
  let width = annotationWidth;
  let lastColumn: string | null = null;
  for (let index = 0; index < visible.length; index += 1) {
    const sourceIndex = sourceIndexes.get(visible[index]) ?? -1;
    if (sourceIndex < 0 || sourceIndex > boundaryIndex) continue;
    offsets.set(visible[index], width);
    width += Number.isFinite(widths[index]) ? Math.max(0, widths[index]) : 0;
    lastColumn = visible[index];
  }
  return { offsets, boundary, lastColumn, width };
}

export interface FrozenViewport {
  pan: number;
  maxPan: number;
  edge: number;
  reserve: number;
  overflow: boolean;
}

/** Oversized data prefixes pan beneath the always-fixed annotation columns. */
export function frozenColumnViewport(
  frozen: FrozenColumnLayout, visibleCount: number, annotationWidth: number,
  viewport: number, current: number, focusedScrollingWidth?: number,
): FrozenViewport {
  const available = Math.max(0, viewport - annotationWidth);
  const hasScrollingColumns = visibleCount > frozen.offsets.size;
  const minimumReserve = hasScrollingColumns ? Math.min(80, available / 2) : 0;
  const overflow = frozen.offsets.size > 0 && frozen.width > viewport - minimumReserve;
  // A focus override only affects an already-oversized prefix; ordinary
  // prefixes retain exactly the same fixed positions as before.
  const reserve = hasScrollingColumns && overflow && focusedScrollingWidth !== undefined
    ? Math.min(available, Math.max(minimumReserve, focusedScrollingWidth)) : minimumReserve;
  const maxPan = overflow ? Math.max(0, frozen.width - (viewport - reserve)) : 0;
  const pan = Math.min(Math.max(0, current), maxPan);
  return { pan, maxPan, edge: frozen.width - pan, reserve, overflow };
}

/** Reveal a frozen or scrolling column without discarding the requested boundary. */
export function horizontalScrollForColumn(
  index: number, visible: readonly string[], widths: readonly number[],
  frozen: FrozenColumnLayout, annotationWidth: number, current: number, viewport: number,
): number {
  if (index < 0 || index >= visible.length) return current;
  const isFrozen = frozen.offsets.has(visible[index]);
  const geometry = frozenColumnViewport(frozen, visible.length, annotationWidth, viewport, current,
    isFrozen ? undefined : widths[index]);
  const left = annotationWidth + widths.slice(0, index).reduce((sum, width) => sum + width, 0);
  const right = left + widths[index];
  if (isFrozen) {
    if (!geometry.overflow) return current;
    const targetRight = viewport - geometry.reserve;
    const usableWidth = Math.max(0, targetRight - annotationWidth);
    if (widths[index] > usableWidth || left - geometry.pan < annotationWidth) {
      return Math.min(geometry.maxPan, Math.max(0, left - annotationWidth));
    }
    if (right - geometry.pan > targetRight) {
      return Math.min(geometry.maxPan, Math.max(0, right - targetRight));
    }
    return current;
  }
  const edge = frozen.width - geometry.maxPan;
  const position = Math.max(current, geometry.maxPan);
  if (left < position + edge || widths[index] > Math.max(0, viewport - edge)) {
    return Math.max(geometry.maxPan, left - edge, 0);
  }
  return right > position + viewport ? Math.max(geometry.maxPan, right - viewport, 0) : position;
}
