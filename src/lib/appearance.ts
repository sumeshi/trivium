import { invoke } from "@tauri-apps/api/core";
import { appLocalDataDir, join } from "@tauri-apps/api/path";
import { mkdir, readTextFile, writeTextFile } from "@tauri-apps/plugin-fs";
import { get, writable } from "svelte/store";
import { acceptSave, trackAcceptedSave } from "./saveDrain.ts";
import { DEFAULT_WALLPAPER, parseWallpaperImage, parseWallpaperSettings } from "./appearanceModel.ts";
import type { WallpaperImage, WallpaperSettings } from "./appearanceModel";

const KEY = "trivium:wallpaper";
let revision = 0;
let enabledRevision = 0;
let pendingWrite = Promise.resolve();
let pendingImage = Promise.resolve();
let scheduled: { settings: WallpaperSettings; timer: ReturnType<typeof setTimeout>; release: () => void; operation: Promise<void> } | null = null;
export const wallpaper = writable<{ settings: WallpaperSettings; image: WallpaperImage | null }>({ settings: { ...DEFAULT_WALLPAPER }, image: null });
export const wallpaperWarning = writable("");
export async function initWallpaper() {
  const atStart = ++revision;
  let settings: WallpaperSettings | null = null;
  try { settings = parseWallpaperSettings(localStorage.getItem(KEY)); } catch {}
  if (!settings) {
    try { settings = parseWallpaperSettings(await readTextFile(await join(await appLocalDataDir(), "appearance.json"))); } catch {}
  }
  const operation = pendingImage.then(async () => {
    try {
      const loaded = await invoke<WallpaperImage | null>("load_wallpaper");
      const image = loaded ? parseWallpaperImage(loaded) : null;
      wallpaper.update(current => ({
        settings: atStart === revision ? settings ?? { ...DEFAULT_WALLPAPER } : current.settings,
        image,
      }));
    } catch { wallpaperWarning.set("Could not load the background image. Choose it again."); }
  });
  // Startup reads share image ordering too: an old load response can never
  // arrive after a newly imported managed copy has been committed to the UI.
  pendingImage = operation.catch(() => {});
  await operation;
}
async function writeSettings(settings: WallpaperSettings) {
    const base = await appLocalDataDir(); await mkdir(base, { recursive: true });
    await writeTextFile(await join(base, "appearance.json"), JSON.stringify(settings, null, 2));
}
function persist(settings: WallpaperSettings, coalesce = false) {
  try { localStorage.setItem(KEY, JSON.stringify(settings)); } catch {}
  if (coalesce && scheduled) {
    scheduled.settings = settings;
    clearTimeout(scheduled.timer);
    scheduled.timer = setTimeout(scheduled.release, 100);
    return scheduled.operation;
  }
  let operation: Promise<void>;
  if (coalesce) {
    let release!: () => void;
    const ready = new Promise<void>(resolve => { release = resolve; });
    const entry = { settings, release, timer: setTimeout(release, 100), operation: Promise.resolve() };
    operation = pendingWrite.then(async () => {
      await ready;
      if (scheduled === entry) scheduled = null;
      await writeSettings(entry.settings);
    });
    entry.operation = operation; scheduled = entry;
  } else {
    // An image commit is an ordering boundary. Later slider input must queue
    // after it, rather than updating an older slot ahead of this snapshot.
    if (scheduled) {
      clearTimeout(scheduled.timer);
      scheduled.release();
      scheduled = null;
    }
    operation = pendingWrite.then(() => writeSettings(settings));
  }
  trackAcceptedSave(operation);
  pendingWrite = operation.catch(() => {});
  return operation;
}
export function setWallpaperSettings(next: WallpaperSettings): Promise<void> {
  const checked = parseWallpaperSettings(next);
  if (!checked) return Promise.reject(new Error("Invalid background settings."));
  return acceptSave(() => {
    if (checked.enabled !== get(wallpaper).settings.enabled) enabledRevision += 1;
    revision += 1; wallpaper.update(value => ({ ...value, settings: checked }));
    return persist(checked, true);
  });
}
export function importWallpaper(path: string): Promise<void> {
  return acceptSave(() => {
    revision += 1;
    const enabledAtStart = enabledRevision;
    // Commit each managed-file response in request order, including an earlier
    // successful import followed by a failed one. Settings edits stay independent.
    const operation = pendingImage.then(async () => {
      const image = parseWallpaperImage(await invoke("import_wallpaper", { path }));
      const current = get(wallpaper).settings;
      const settings = { ...current, enabled: enabledAtStart === enabledRevision ? true : current.enabled };
      wallpaper.set({ settings, image }); wallpaperWarning.set("");
      await persist(settings);
    });
    pendingImage = operation.catch(() => {});
    return operation;
  });
}
export function clearWallpaper(): Promise<void> {
  return acceptSave(() => {
    revision += 1;
    const operation = pendingImage.then(async () => {
      await invoke("clear_wallpaper");
      const settings = { ...get(wallpaper).settings, enabled: false };
      wallpaper.set({ settings, image: null }); wallpaperWarning.set("");
      await persist(settings);
    });
    pendingImage = operation.catch(() => {});
    return operation;
  });
}
