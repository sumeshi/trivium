import { mkdir, readDir, readTextFile, writeTextFile } from "@tauri-apps/plugin-fs";
import { invoke } from "@tauri-apps/api/core";
import { appLocalDataDir, join } from "@tauri-apps/api/path";
import { get, writable } from "svelte/store";
import { createThemePreference, DEFAULT_THEME, EDITABLE_THEME_COLORS, getUserTheme, parseSavedTheme,
  parseTheme, parseUserTheme, registerUserTheme, THEME_OPTIONS, themeBase } from "./themeModel.ts";
import type { Theme, UserTheme } from "./themeModel";
import { acceptSave, trackAcceptedSave } from "./saveDrain.ts";
import { readImportText } from "./boundedText.ts";

const THEME_KEY = "trivium:theme";
const MAX_USER_THEMES = 128;
let themeRevision = 0;
let explicitSelectionRevision = 0;
let writeIntent = 0;
let lastSelectedWrite = 0;
let catalogRevision = 0;
let catalogLoad = 0;
const modifiedThemes = new Map<string, number>();
let pendingWrite = Promise.resolve();
export const theme = writable<Theme>(DEFAULT_THEME);
export const themeOptions = writable<Array<{ value: Theme; label: string }>>([...THEME_OPTIONS]);
export const themeCatalogWarning = writable("");

function applyThemeAttr(next: Theme) {
  applyDefinition(themeBase(next), getUserTheme(next)?.colors ?? {});
}
function applyDefinition(base: UserTheme["base"], colors: UserTheme["colors"]) {
  const root = document.documentElement;
  for (const key of EDITABLE_THEME_COLORS) root.style.removeProperty(`--${key}`);
  root.setAttribute("data-theme", base);
  for (const [key, value] of Object.entries(colors)) root.style.setProperty(`--${key}`, value);
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
  return parseUserTheme(await readImportText(path, "theme"));
}
export async function initTheme() {
  const revision = ++themeRevision;
  explicitSelectionRevision += 1;
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
    themeRevision += 1; explicitSelectionRevision += 1;
    applyThemeAttr(selected); theme.set(selected);
    return persistTheme(selected);
  });
}
export function importThemeFile(path: string, confirmReplace: (name: string) => boolean = () => false): Promise<Theme | null> {
  return acceptSave(async () => {
    const selectionRevision = explicitSelectionRevision, intent = ++writeIntent;
    const definition = await readThemeFile(path);
    const selected = `user:${definition.id}` as Theme;
    if (parseTheme(selected) && !confirmReplace(definition.name)) return null;
    return storeUserTheme(definition, selectionRevision, intent);
  });
}

/** Serialize file admission and selection with the existing preference writer. */
async function storeUserTheme(definition: UserTheme, selectionRevision: number, intent: number): Promise<Theme> {
  const value = `user:${definition.id}` as Theme;
  const preference = JSON.stringify(createThemePreference(value), null, 2);
  let selected = false;
  const maySelect = () => selectionRevision === explicitSelectionRevision && intent >= lastSelectedWrite;
  await queueWrite(async () => {
    const directory = await themeDirectory();
    await mkdir(directory, { recursive: true });
    const filename = `${definition.id}.json`;
    const files = (await readDir(directory)).filter(entry => entry.isFile && entry.name.endsWith(".json"));
    if (!files.some(entry => entry.name === filename) && files.length >= MAX_USER_THEMES) {
      throw new Error(`The theme folder already contains ${MAX_USER_THEMES} theme files. Replace an existing theme to import another.`);
    }
    await invoke("save_theme_profile", { id: definition.id, text: JSON.stringify(definition, null, 2) });
    // The profile is committed even if storing its selection later fails.
    addCatalogTheme(definition);
    // A different theme chosen during the write owns the selection, even if
    // this accepted save/import still completes its catalog entry.
    if (maySelect()) {
      try {
        await writeTextFile(await join(await appLocalDataDir(), "theme.json"), preference);
      } catch {
        if (maySelect() && get(theme) === value) applyThemeAttr(value);
        throw new Error("Theme saved, but its selection preference could not be stored.");
      }
      selected = maySelect();
    }
  });
  if (selected && maySelect()) {
    themeRevision += 1;
    lastSelectedWrite = intent;
    applyThemeAttr(value); theme.set(value);
    try { localStorage.setItem(THEME_KEY, preference); } catch {}
  }
  return value;
}

/** A preview changes paint only. Its owner can neither cancel nor save over a
 * later explicit theme choice, startup restore, or another editor preview. */
export function beginThemePreview() {
  const owner = ++themeRevision;
  const selectionRevision = ++explicitSelectionRevision;
  let ended = false;
  let saving = false;
  applyThemeAttr(get(theme));
  const current = () => !ended && owner === themeRevision;
  return {
    apply(input: UserTheme): boolean {
      if (!current() || saving) return false;
      const definition = parseUserTheme(input);
      applyDefinition(definition.base, definition.colors);
      return true;
    },
    cancel(): void {
      if (current()) { themeRevision += 1; explicitSelectionRevision += 1; applyThemeAttr(get(theme)); }
      ended = true;
    },
    save(input: UserTheme): Promise<Theme> {
      if (!current()) return Promise.reject(new Error("The selected theme changed. Reopen the color editor."));
      if (saving) return Promise.reject(new Error("The theme is already being saved."));
      const definition = parseUserTheme(input);
      saving = true;
      return acceptSave(() => storeUserTheme(definition, selectionRevision, ++writeIntent))
        .then(value => { ended = true; return value; }).finally(() => { saving = false; });
    },
  };
}
