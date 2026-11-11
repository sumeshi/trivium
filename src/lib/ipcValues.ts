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
  const displayCache: Record<string, string> = {};
  const previewCache: Record<string, string> = {};
  for (const [column, cell] of Object.entries(cells ?? {})) {
    const text = displayText(cell);
    displayCache[column] = cell.truncated ? `${text}…` : text;
    previewCache[column] = copyText(cell);
  }
  return { displayCache, previewCache };
}
