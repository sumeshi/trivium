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
    // Normalize engine-supported year-first dates and naive time separators.
    // Keep timezone syntax strict; padding is accepted by the naive parser.
    const source = original.trim()
      .replace(/^(\d{4})\/(\d{2})\/(\d{2})(?=$|T|\p{White_Space})/u, '$1-$2-$3')
      .replace(/^(\d{4}-\d{2}-\d{2})\p{White_Space}+(\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?)$/u, '$1T$2');
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

