import { createDir, readTextFile, writeTextFile } from "@tauri-apps/api/fs";
import { appLocalDataDir, join } from "@tauri-apps/api/path";
import { writable } from "svelte/store";

export type Theme = "light" | "dark";

const THEME_KEY = "trivium:theme";
const THEME_FILE = "theme.json";

export const theme = writable<Theme>("dark");

function applyThemeAttr(next: Theme) {
  const root = document.documentElement;
  root.setAttribute("data-theme", next);
}

export async function initTheme() {
  let loaded: Theme | null = null;
  try {
    const stored = localStorage.getItem(THEME_KEY);
    if (stored === "light" || stored === "dark") {
      loaded = stored;
    }
  } catch (_err) {
    // ignore
  }

  if (!loaded) {
    try {
      const base = await appLocalDataDir();
      const path = await join(base, THEME_FILE);
      const text = await readTextFile(path);
      const data = JSON.parse(text) as { theme?: string };
      if (data && (data.theme === "light" || data.theme === "dark")) {
        loaded = data.theme;
      }
    } catch (_err) {
      // ignore (first run or not available)
    }
  }

  const next = loaded ?? "dark";
  applyThemeAttr(next);
  theme.set(next);
}

async function persistTheme(next: Theme) {
  try {
    localStorage.setItem(THEME_KEY, next);
  } catch (_err) {
    // ignore
  }
  try {
    const base = await appLocalDataDir();
    // ensure dir exists (no-op if already exists)
    await createDir(base, { recursive: true });
    const path = await join(base, THEME_FILE);
    await writeTextFile(path, JSON.stringify({ theme: next }, null, 2));
  } catch (_err) {
    // ignore if filesystem not available
  }
}

export async function setTheme(next: Theme) {
  applyThemeAttr(next);
  theme.set(next);
  await persistTheme(next);
}

export async function toggleTheme() {
  let current: Theme = "dark";
  theme.update((t) => {
    current = t;
    return t;
  });
  const next: Theme = current === "dark" ? "light" : "dark";
  await setTheme(next);
}
