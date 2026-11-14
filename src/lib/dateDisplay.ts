import type { Cell } from './types';
import { displayText } from './ipcValues.ts';
import { parseTimelineNanos, formatTimelineNanos } from './timelineRange.ts';

export type ColumnDisplayFormat = 'iso';
export interface FormattedCell { text: string; error?: string }
const cache = new WeakMap<Cell, FormattedCell>();

/** Presentation only. Source text remains the copy, filter and export value. */
export function formatIsoCell(cell: Cell | null | undefined): FormattedCell {
  if (!cell) return { text: '' };
  const previous = cache.get(cell);
  if (previous) return previous;
  const original = displayText(cell);
  let result: FormattedCell;
  if (cell.kind === 'null' || cell.kind === 'missing' || !original.trim()) {
    result = { text: original };
  } else if (cell.truncated) {
    result = { text: `${original}…`, error: 'Timestamp preview is truncated.' };
  } else {
    // Year-first slash dates are unambiguous; locale-dependent dates and epoch
    // units are deliberately not guessed.
    const source = original.trim().replace(/^(\d{4})\/(\d{2})\/(\d{2})(?=$|[T ])/, '$1-$2-$3');
    const nanos = parseTimelineNanos(source);
    if (nanos === null) result = { text: original, error: 'Unrecognized timestamp.' };
    else {
      const iso = formatTimelineNanos(nanos);
      result = /^\d{4}-\d{2}-\d{2}T/.test(iso)
        ? { text: `${iso}Z` }
        : { text: original, error: 'Timestamp is outside the display range.' };
    }
  }
  cache.set(cell, result);
  return result;
}

