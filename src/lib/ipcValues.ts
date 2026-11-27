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
 * Source text used for a dialog hint, copying, or filtering.
 *
 * This is the source representation, not the display decoration: an explicit
 * null copies as "null", while a missing key has no value to copy. Truncated
 * previews are initial hints only; copying and filtering require the full value.
 */
export function copyText(cell: Cell | null | undefined): string {
  if (!cell) return "";
  if (cell.kind === "null") return "null";
  if (cell.kind === "missing") return "";
  return cell.text ?? "";
}

export type CellCaches = {
  displayCache: Record<string, string>;
  /** Conservative estimate for source/display text and exact datetime metadata. */
  cellBytes: number;
};

/**
 * Builds the display cache from the backend cell DTOs.
 *
 * A truncated display value gets a trailing "…" so the table never presents a
 * preview as a complete value. The source cells retain the raw prefix for the
 * dialog's initial hint; copying and filtering fetch the full value on demand.
 */
export function buildCellCaches(
  cells: Record<string, Cell> | null | undefined
): CellCaches {
  const displayCache: Record<string, string> = Object.create(null);
  let cellBytes = 0;
  for (const [column, cell] of Object.entries(cells ?? {})) {
    const text = displayText(cell);
    const display = cell.truncated ? `${text}…` : text;
    const preview = copyText(cell);
    displayCache[column] = display;
    cellBytes += display.length + preview.length + (cell.datetime_nanos?.length ?? 0);
  }
  return { displayCache, cellBytes };
}
