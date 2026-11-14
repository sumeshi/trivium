import { get, writable } from "svelte/store";

export const TABLE_TEXT_SIZES = [85, 100, 115, 130, 145] as const;
export type TableTextSize = (typeof TABLE_TEXT_SIZES)[number];
export const DEFAULT_TABLE_TEXT_SIZE = 100;
const STORAGE_KEY = "trivium:table-text-size";

const current = writable<TableTextSize>(DEFAULT_TABLE_TEXT_SIZE);
export const tableTextSize = { subscribe: current.subscribe };

export function parseTableTextSize(value: unknown): TableTextSize | null {
  return TABLE_TEXT_SIZES.find((size) => size === value) ?? null;
}

export function initTableTextSize(): void {
  let saved: TableTextSize | null = null;
  try {
    saved = parseTableTextSize(JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "null"));
  } catch {
    // Missing or unavailable browser storage leaves the standard text size.
  }
  current.set(saved ?? DEFAULT_TABLE_TEXT_SIZE);
}

export function setTableTextSize(value: unknown): void {
  const next = parseTableTextSize(value);
  if (next === null) return;
  current.set(next);
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  } catch {
    // The current session remains usable when storage is unavailable.
  }
}

export function adjustTableTextSize(direction: -1 | 1): void {
  const index = TABLE_TEXT_SIZES.indexOf(get(tableTextSize));
  const next = Math.max(0, Math.min(TABLE_TEXT_SIZES.length - 1, index + direction));
  setTableTextSize(TABLE_TEXT_SIZES[next]);
}
