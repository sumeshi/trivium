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
 * Text copied from a cell and used as a filter value.
 *
 * This is the source representation, not the display decoration: an explicit
 * null copies as "null", while a missing key has no value to copy.
 */
export function copyText(cell: Cell | null | undefined): string {
  if (!cell) return "";
  if (cell.kind === "null") return "null";
  if (cell.kind === "missing") return "";
  return cell.text ?? "";
}

export type CellCaches = {
  displayCache: Record<string, string>;
  copyCache: Record<string, string>;
};

/** Builds the display/copy caches from the backend cell DTOs. */
export function buildCellCaches(
  cells: Record<string, Cell> | null | undefined
): CellCaches {
  const displayCache: Record<string, string> = {};
  const copyCache: Record<string, string> = {};
  for (const [column, cell] of Object.entries(cells ?? {})) {
    displayCache[column] = displayText(cell);
    copyCache[column] = copyText(cell);
  }
  return { displayCache, copyCache };
}
