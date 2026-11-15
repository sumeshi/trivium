import { derived, get, writable } from "svelte/store";
import { appLocalDataDir, join } from "@tauri-apps/api/path";
import { mkdir, readTextFile, writeTextFile } from "@tauri-apps/plugin-fs";
import { acceptSave, trackAcceptedSave } from "./saveDrain.ts";
import { ROW_HEIGHT } from "./virtualScroll.ts";

export const TABLE_TEXT_SIZES = [55, 70, 85, 100, 115, 130, 145] as const;
export type TableTextSize = (typeof TABLE_TEXT_SIZES)[number];
export const DEFAULT_TABLE_TEXT_SIZE = 100;
const STORAGE_KEY = "trivium:table-text-size";
const STORAGE_FILE = "table-text-size.json";

const current = writable<TableTextSize>(DEFAULT_TABLE_TEXT_SIZE);
export const tableTextSize = { subscribe: current.subscribe };
export const tableRowHeight = derived(current, (size) => Math.round(ROW_HEIGHT * size / 100));
let revision = 0;
let pendingWrite = Promise.resolve();

export function parseTableTextSize(value: unknown): TableTextSize | null {
  return TABLE_TEXT_SIZES.find((size) => size === value) ?? null;
}

function cache(next: TableTextSize): void {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(next)); }
  catch { /* The live setting remains usable when browser storage is blocked. */ }
}

export async function initTableTextSize(): Promise<void> {
  const ticket = ++revision;
  let saved: TableTextSize | null = null;
  try {
    saved = parseTableTextSize(JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "null"));
  } catch {
    // Missing or unavailable browser storage leaves the standard text size.
  }
  current.set(saved ?? DEFAULT_TABLE_TEXT_SIZE);
  try {
    await pendingWrite;
    // The browser cache can lag after a forced WebView shutdown; disk wins.
    const path = await join(await appLocalDataDir(), STORAGE_FILE);
    saved = parseTableTextSize(JSON.parse(await readTextFile(path)));
    if (saved !== null && ticket === revision) {
      current.set(saved);
      cache(saved);
    }
  } catch {
    // First run and web-only mode keep the cached or default size.
  }
}

export function setTableTextSize(value: unknown): Promise<void> {
  const next = parseTableTextSize(value);
  if (next === null) return pendingWrite;
  return acceptSave(() => {
    revision += 1;
    current.set(next);
    cache(next);
    // Serialize native writes so a slower old choice cannot replace the latest.
    const write = pendingWrite.then(async () => {
      const base = await appLocalDataDir();
      await mkdir(base, { recursive: true });
      await writeTextFile(await join(base, STORAGE_FILE), JSON.stringify(next));
    });
    trackAcceptedSave(write);
    // Native storage is optional for the live UI; close still checks failure.
    pendingWrite = write.catch(() => {});
    return pendingWrite;
  });
}

export function adjustTableTextSize(direction: -1 | 1): Promise<void> {
  const index = TABLE_TEXT_SIZES.indexOf(get(tableTextSize));
  const next = Math.max(0, Math.min(TABLE_TEXT_SIZES.length - 1, index + direction));
  return setTableTextSize(TABLE_TEXT_SIZES[next]);
}
