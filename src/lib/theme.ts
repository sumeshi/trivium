import { mkdir, readDir, readTextFile, stat, writeTextFile } from "@tauri-apps/plugin-fs";
import { appLocalDataDir, join } from "@tauri-apps/api/path";
import { writable } from "svelte/store";
import { createThemePreference, DEFAULT_THEME, EDITABLE_THEME_COLORS, getUserTheme, parseSavedTheme,
  parseTheme, parseUserTheme, registerUserTheme, THEME_OPTIONS, themeBase } from "./themeModel.ts";
import type { Theme, UserTheme } from "./themeModel";
import { acceptSave, trackAcceptedSave } from "./saveDrain.ts";

export { THEME_OPTIONS } from "./themeModel.ts";
export type { Theme } from "./themeModel";
const THEME_KEY = "trivium:theme";
const MAX_USER_THEMES = 128;
let themeRevision = 0;
let catalogRevision = 0;
let catalogLoad = 0;
const modifiedThemes = new Map<string, number>();
let pendingWrite = Promise.resolve();
export const theme = writable<Theme>(DEFAULT_THEME);
export const themeOptions = writable<Array<{ value: Theme; label: string }>>([...THEME_OPTIONS]);
export const themeCatalogWarning = writable("");

function applyThemeAttr(next: Theme) {
  const root = document.documentElement;
  for (const key of EDITABLE_THEME_COLORS) root.style.removeProperty(`--${key}`);
  root.setAttribute("data-theme", themeBase(next));
  for (const [key, value] of Object.entries(getUserTheme(next)?.colors ?? {})) root.style.setProperty(`--${key}`, value);
}
function addCatalogTheme(definition: UserTheme, readAt?: number) {
  const value = `user:${definition.id}` as Theme;
  if (readAt !== undefined && (modifiedThemes.get(definition.id) ?? 0) > readAt) return value;
  if (readAt === undefined) modifiedThemes.set(definition.id, ++catalogRevision);
  registerUserTheme(definition);
  themeOptions.update(options => [...options.filter(option => option.value !== value), { value, label: definition.name }]);
  return value;
}
function queueWrite(work: () => Promise<void>) {
  const write = pendingWrite.then(work);
  trackAcceptedSave(write);
  pendingWrite = write.catch(() => {});
  return write;
}
async function themeDirectory() {
  return join(await appLocalDataDir(), "themes");
}
async function readThemeFile(path: string) {
  const info = await stat(path);
  if (info.size > 65_536) throw new Error("Theme JSON must be 64 KiB or smaller.");
  return parseUserTheme(await readTextFile(path));
}
export async function initTheme() {
  const revision = ++themeRevision;
  const load = ++catalogLoad, readAt = catalogRevision;
  try {
    const directory = await themeDirectory();
    await mkdir(directory, { recursive: true });
    const files = (await readDir(directory)).filter(entry => entry.isFile && entry.name.endsWith(".json")).sort((a, b) => a.name.localeCompare(b.name));
    if (files.length > MAX_USER_THEMES) throw new Error(`The theme folder contains more than ${MAX_USER_THEMES} theme files.`);
    const invalid: string[] = [];
    for (const entry of files) {
      try {
        const definition = await readThemeFile(await join(directory, entry.name));
        if (load === catalogLoad) addCatalogTheme(definition, readAt);
      }
      catch { invalid.push(entry.name); }
    }
    if (load === catalogLoad) themeCatalogWarning.set(invalid.length ? `Could not load themes: ${invalid.join(", ")}` : "");
  } catch (error) {
    if (load === catalogLoad) themeCatalogWarning.set(error instanceof Error ? error.message : "Could not load custom themes.");
  }
  let loaded: Theme | null = null;
  try { loaded = parseSavedTheme(localStorage.getItem(THEME_KEY)); } catch {}
  if (!loaded) {
    try { loaded = parseSavedTheme(await readTextFile(await join(await appLocalDataDir(), "theme.json"))); } catch {}
  }
  if (revision !== themeRevision) return;
  const next = loaded ?? DEFAULT_THEME;
  applyThemeAttr(next); theme.set(next);
}
function persistTheme(next: Theme) {
  const preference = JSON.stringify(createThemePreference(next), null, 2);
  try { localStorage.setItem(THEME_KEY, preference); } catch {}
  const write = queueWrite(async () => {
    const base = await appLocalDataDir();
    await mkdir(base, { recursive: true });
    await writeTextFile(await join(base, "theme.json"), preference);
  });
  // Browser fallback remains usable; the raw failure stays in the close drain.
  return write.catch(() => {});
}
export function setTheme(next: Theme): Promise<void> {
  const selected = parseTheme(next);
  if (!selected) return Promise.resolve();
  return acceptSave(() => {
    themeRevision += 1; applyThemeAttr(selected); theme.set(selected);
    return persistTheme(selected);
  });
}
export function importThemeFile(path: string, confirmReplace: (name: string) => boolean = () => false): Promise<Theme | null> {
  return acceptSave(async () => {
    const definition = await readThemeFile(path);
    const selected = `user:${definition.id}` as Theme;
    if (parseTheme(selected) && !confirmReplace(definition.name)) return null;
    await queueWrite(async () => {
      const directory = await themeDirectory();
      await mkdir(directory, { recursive: true });
      const filename = `${definition.id}.json`;
      // Check admission in the write queue: concurrent imports must observe
      // earlier accepted files, while replacing an existing ID needs no slot.
      const files = (await readDir(directory)).filter(entry => entry.isFile && entry.name.endsWith(".json"));
      if (!files.some(entry => entry.name === filename) && files.length >= MAX_USER_THEMES) {
        throw new Error(`The theme folder already contains ${MAX_USER_THEMES} theme files. Replace an existing theme to import another.`);
      }
      await writeTextFile(await join(directory, filename), JSON.stringify(definition, null, 2));
    });
    const value = addCatalogTheme(definition);
    await setTheme(value);
    return value;
  });
}
