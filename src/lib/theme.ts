import { mkdir, readTextFile, writeTextFile } from "@tauri-apps/plugin-fs";
import { appLocalDataDir, join } from "@tauri-apps/api/path";
import { writable } from "svelte/store";
import { createThemePreference, DEFAULT_THEME, parseSavedTheme, parseTheme } from "./themeModel.ts";
import type { Theme } from "./themeModel";

export { THEME_OPTIONS } from "./themeModel";
export type { Theme } from "./themeModel";

const THEME_KEY = "trivium:theme";
const THEME_FILE = "theme.json";
let themeRevision = 0;
let pendingWrite = Promise.resolve();

export const theme = writable<Theme>(DEFAULT_THEME);

function applyThemeAttr(next: Theme) {
  const root = document.documentElement;
  root.setAttribute("data-theme", next);
}

export async function initTheme() {
  const revision = ++themeRevision;
  let loaded: Theme | null = null;
  try {
    loaded = parseSavedTheme(localStorage.getItem(THEME_KEY));
  } catch (_err) {
    // ignore
  }

  if (!loaded) {
    try {
      const base = await appLocalDataDir();
      const path = await join(base, THEME_FILE);
      const text = await readTextFile(path);
      loaded = parseSavedTheme(text);
    } catch (_err) {
      // ignore (first run or not available)
    }
  }

  // A theme chosen while the file was loading takes precedence over that read.
  if (revision !== themeRevision) return;
  const next = loaded ?? DEFAULT_THEME;
  applyThemeAttr(next);
  theme.set(next);
}

function persistTheme(next: Theme): Promise<void> {
  try {
    localStorage.setItem(THEME_KEY, JSON.stringify(createThemePreference(next)));
  } catch (_err) {
    // ignore
  }
  // Serialize writes so rapid changes cannot leave an older choice on disk.
  pendingWrite = pendingWrite.then(async () => {
    try {
      const base = await appLocalDataDir();
      await mkdir(base, { recursive: true });
      const path = await join(base, THEME_FILE);
      await writeTextFile(path, JSON.stringify(createThemePreference(next), null, 2));
    } catch (_err) {
      // localStorage remains available when filesystem access is unavailable.
    }
  });
  return pendingWrite;
}

export async function setTheme(next: Theme) {
  const selected = parseTheme(next);
  if (!selected) return;
  themeRevision += 1;
  applyThemeAttr(selected);
  theme.set(selected);
  await persistTheme(selected);
}
