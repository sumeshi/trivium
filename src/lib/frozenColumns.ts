/** Missing means the legacy first-visible default; empty means no frozen data. */
export type FrozenThrough = string | undefined;

export interface FrozenColumnLayout {
  offsets: Map<string, number>;
  boundary: string | null;
  lastColumn: string | null;
  width: number;
}

/** Freeze the source-order prefix; hidden columns contribute no physical width. */
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

/** Reveal a scrolling column beside the actual frozen prefix, including wide cells. */
export function horizontalScrollForColumn(
  index: number, visible: readonly string[], widths: readonly number[],
  frozen: FrozenColumnLayout, annotationWidth: number, current: number, viewport: number,
): number {
  if (index < 0 || index >= visible.length || frozen.offsets.has(visible[index])) return current;
  const left = annotationWidth + widths.slice(0, index).reduce((sum, width) => sum + width, 0);
  const right = left + widths[index];
  if (left < current + frozen.width || widths[index] > Math.max(0, viewport - frozen.width)) {
    return Math.max(0, left - frozen.width);
  }
  return right > current + viewport ? Math.max(0, right - viewport) : current;
}
