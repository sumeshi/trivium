import type { Cell } from "./types";

/**
 * Text shown in the table and detail view.
 *
 * Empty strings stay blank, explicit nulls read "null", and a key absent from
 * the source record reads "—". The exact value is never parsed as a number.
 */
export function displayText(cell: Cell | null | undefined): string {
  if (!cell) return "";
  if (cell.kind === "null") return "null";
  if (cell.kind === "missing") return "—";
  return cell.text ?? "";
}

/**
 * Text copied from a full cell value and used as a filter value.
 *
 * This is the source representation, not the display decoration: an explicit
 * null copies as "null", while a missing key has no value to copy. A list
 * preview must never be passed here; it comes from `getCell` instead.
 */
export function copyText(cell: Cell | null | undefined): string {
  if (!cell) return "";
  if (cell.kind === "null") return "null";
  if (cell.kind === "missing") return "";
  return cell.text ?? "";
}

export type CellCaches = {
  displayCache: Record<string, string>;
  /** Preview text only; the detail dialog fetches the full value on demand. */
  previewCache: Record<string, string>;
  /** Conservative text estimate for both views and exact datetime metadata. */
  cellBytes: number;
};

/**
 * Builds the display/preview caches from the backend cell DTOs.
 *
 * A truncated display value gets a trailing "…" so the table never presents a
 * preview as a complete value. The preview cache stores the raw prefix without
 * decoration; it is an initial hint, not a copy/filter source.
 */
export function buildCellCaches(
  cells: Record<string, Cell> | null | undefined
): CellCaches {
  const displayCache: Record<string, string> = Object.create(null);
  const previewCache: Record<string, string> = Object.create(null);
  let cellBytes = 0;
  for (const [column, cell] of Object.entries(cells ?? {})) {
    const text = displayText(cell);
    const display = cell.truncated ? `${text}…` : text;
    const preview = copyText(cell);
    displayCache[column] = display;
    previewCache[column] = preview;
    cellBytes += display.length + preview.length + (cell.datetime_nanos?.length ?? 0);
  }
  return { displayCache, previewCache, cellBytes };
}
