import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import { get } from "svelte/store";
import { EDITABLE_THEME_COLORS, exportTheme, getUserTheme, isLightTheme, parseSavedTheme, parseUserTheme,
  registerUserTheme, THEME_OPTIONS, themeTokens } from "./themeModel.ts";
import type { UserTheme } from "./themeModel.ts";
import { initTheme, importThemeFile, setTheme, theme } from "./theme.ts";
import { clearWallpaper, importWallpaper, initWallpaper, setWallpaperSettings, wallpaper } from "./appearance.ts";
import { DEFAULT_WALLPAPER, parseWallpaperImage, parseWallpaperSettings, wallpaperVisible } from "./appearanceModel.ts";
import { flushAllSaves, reopenSaveAdmission, sealSaveAdmission } from "./saveDrain.ts";

const turn = () => new Promise<void>(resolve => setImmediate(resolve));
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(yes => { resolve = yes; });
  return { promise, resolve };
}
const custom = (id = "custom-test"): UserTheme => ({ type: "trivium-theme", version: 1, id, name: "Custom test", base: "dark", colors: { accent: "#bbccee" } });
const image = { dataUrl: "data:image/png;base64,AA==", width: 64, height: 32 };

afterEach(async () => { reopenSaveAdmission(); await flushAllSaves().catch(() => {}); });

test("theme templates round-trip all offered bases without exposing semantic or geometry tokens", () => {
  for (const option of THEME_OPTIONS) {
    const definition = exportTheme(option.value, `${option.value}-test-copy`);
    assert.deepEqual(parseUserTheme(JSON.stringify(definition)), definition);
    assert.equal(definition.base, option.value);
    assert.deepEqual(Object.keys(definition.colors).sort(), [...EDITABLE_THEME_COLORS].sort());
    const registered = registerUserTheme(definition);
    assert.equal(isLightTheme(registered), isLightTheme(option.value));
    assert.equal(parseSavedTheme({ version: 2, theme: registered }), registered);
    const inherited = themeTokens(registered), original = themeTokens(option.value);
    for (const key of ["positive", "warning", "danger", "flag-safe-selected-bg", "rule-red"]) assert.equal(inherited[key], original[key]);
  }
});

test("theme validation rejects other profile kinds, CSS expressions, transparency and unsafe IDs", () => {
  for (const bad of [
    {}, { ...custom(), type: "trivium-view" }, { ...custom(), version: 2 }, { ...custom(), base: "user:any" },
    { ...custom(), id: "../other" }, { ...custom(), id: "__proto__" }, { ...custom(), id: "CON" },
    { ...custom(), id: "con" }, { ...custom(), id: "lpt1" }, { ...custom(), id: "MixedCase" }, { ...custom(), name: "" },
    { ...custom(), wallpaper: "file:///source/image.jpg" }, { ...custom(), colors: { positive: "#fff" } },
    { ...custom(), colors: { "font-ui": "Arial" } }, { ...custom(), colors: { surface: "transparent" } },
    { ...custom(), colors: { surface: "#ffffff00" } }, { ...custom(), colors: { accent: "url(https://example.invalid)" } },
    { ...custom(), colors: { accent: "var(--fg)" } }, { ...custom(), colors: { accent: "red;display:none" } },
  ]) assert.throws(() => parseUserTheme(bad));
  assert.throws(() => parseUserTheme(" ".repeat(65_537)), /64 KiB/);
  assert.equal(parseUserTheme({ ...custom(), colors: { surface: "#ABC" } }).colors.surface, "#abc");
});

test("wallpaper defaults do not change paint, and invalid assets/preferences cannot enable it", () => {
  assert.equal(wallpaperVisible(DEFAULT_WALLPAPER, image), false);
  assert.equal(wallpaperVisible({ ...DEFAULT_WALLPAPER, enabled: true }, null), false);
  assert.equal(wallpaperVisible({ ...DEFAULT_WALLPAPER, enabled: true, strength: 0 }, image), false);
  assert.deepEqual(parseWallpaperSettings(DEFAULT_WALLPAPER), DEFAULT_WALLPAPER);
  for (const bad of [null, [], custom(), { ...DEFAULT_WALLPAPER, version: 2 }, { ...DEFAULT_WALLPAPER, strength: 101 },
    { ...DEFAULT_WALLPAPER, strength: -1 }, { ...DEFAULT_WALLPAPER, fit: "repeat" }, { ...DEFAULT_WALLPAPER, enabled: "true" }]) assert.equal(parseWallpaperSettings(bad), null);
  assert.deepEqual(parseWallpaperImage(image), image);
  for (const bad of [{ ...image, dataUrl: "https://example.invalid/image.png" }, { ...image, dataUrl: "data:image/svg+xml;base64,AA==" },
    { ...image, dataUrl: "data:image/webp;base64,AA==" }, { ...image, width: 3841 }, { ...image, width: 3840, height: 3840 }]) assert.throws(() => parseWallpaperImage(bad));
});

async function environment(run: (f: { files: Map<string, string>; cache: Map<string, string>; writes: Array<{ path: string; text: string }>; calls: string[];
  holdWrite: (value: Promise<void> | null) => void; failWrites: (value: boolean) => void;
  readHandler: (handler: (path: string, value: number[]) => Promise<number[]>) => void;
  imageHandler: (handler: (command: string, args: unknown) => Promise<unknown>) => void }) => Promise<void>) {
  reopenSaveAdmission(); await flushAllSaves().catch(() => {});
  const previous = new Map(["window", "document", "localStorage"].map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]));
  const files = new Map<string, string>(), cache = new Map<string, string>();
  const writes: Array<{ path: string; text: string }> = [], calls: string[] = [];
  let hold: Promise<void> | null = null, fail = false;
  let handleImage = async (_command: string, _args: unknown): Promise<unknown> => image;
  let handleRead = async (_path: string, value: number[]) => value;
  Object.defineProperty(globalThis, "document", { configurable: true, value: { documentElement: {
    setAttribute() {}, style: { removeProperty() {}, setProperty() {} },
  } } });
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: { getItem: (key: string) => cache.get(key) ?? null,
    setItem: (key: string, value: string) => { cache.set(key, value); } } });
  Object.defineProperty(globalThis, "window", { configurable: true, value: { __TAURI_INTERNALS__: {
    invoke: async (command: string, args: unknown, options?: { headers?: { path?: string } }) => {
      calls.push(command);
      if (command === "plugin:path|resolve_directory") return "/managed";
      if (command === "plugin:path|join") return (args as { paths: string[] }).paths.join("/");
      if (command === "plugin:fs|mkdir") return;
      if (command === "plugin:fs|read_dir") return [...files.keys()].filter(path => path.startsWith("/managed/themes/"))
        .map(path => ({ name: path.split("/").at(-1), isFile: true }));
      if (command === "plugin:fs|stat") return { size: files.get((args as { path: string }).path)?.length ?? 0 };
      if (command === "plugin:fs|read_text_file") {
        const path = (args as { path: string }).path;
        return handleRead(path, [...new TextEncoder().encode(files.get(path) ?? "")]);
      }
      if (command === "plugin:fs|write_text_file") {
        const path = decodeURIComponent(options?.headers?.path ?? "");
        const text = new TextDecoder().decode(args as Uint8Array);
        writes.push({ path, text }); await hold;
        if (fail) throw new Error("Appearance disk failure");
        files.set(path, text); return;
      }
      if (["load_wallpaper", "import_wallpaper", "clear_wallpaper"].includes(command)) return handleImage(command, args);
      throw new Error(`Unexpected command: ${command}`);
    },
  } } });
  try { await run({ files, cache, writes, calls, holdWrite: value => { hold = value; }, failWrites: value => { fail = value; },
    readHandler: handler => { handleRead = handler; }, imageHandler: handler => { handleImage = handler; } }); }
  finally {
    reopenSaveAdmission(); await flushAllSaves().catch(() => {});
    for (const [key, descriptor] of previous) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor); else Reflect.deleteProperty(globalThis, key);
    }
  }
}

test("custom theme import saves an owned copy before registering and restores it on startup", async () => {
  await environment(async f => {
    const definition = custom("roundtrip-theme");
    f.files.set("/source/theme.json", JSON.stringify(definition));
    const imported = await importThemeFile("/source/theme.json");
    assert.equal(imported, "user:roundtrip-theme");
    assert.equal(get(theme), imported);
    assert.equal(getUserTheme(imported!)?.base, "dark");
    assert.equal(f.files.get("/source/theme.json"), JSON.stringify(definition));
    assert.ok(f.files.has("/managed/themes/roundtrip-theme.json"));
    assert.equal(await importThemeFile("/source/theme.json"), null, "duplicate IDs require explicit replacement");
    await setTheme("light");
    const diskOnly = custom("restored-from-disk");
    assert.equal(getUserTheme("user:restored-from-disk"), null);
    f.files.set("/managed/themes/restored-from-disk.json", JSON.stringify(diskOnly));
    f.files.set("/managed/theme.json", JSON.stringify({ version: 2, theme: "user:restored-from-disk" }));
    f.cache.delete("trivium:theme");
    await initTheme();
    assert.equal(get(theme), "user:restored-from-disk");
  });
});

test("failed custom theme writes never replace the active palette or register unsaved definitions", async () => {
  await environment(async f => {
    await setTheme("dark");
    f.files.set("/source/fail.json", JSON.stringify(custom("failed-import-theme")));
    f.failWrites(true);
    await assert.rejects(importThemeFile("/source/fail.json"), /Appearance disk failure/);
    assert.equal(get(theme), "dark");
    assert.equal(getUserTheme("user:failed-import-theme"), null);
    await assert.rejects(flushAllSaves(), /save or export failed/);
  });
});

test("wallpaper slider coalesces file writes and normal close drains the last accepted setting", async () => {
  await environment(async f => {
    await initWallpaper();
    const changes = [10, 20, 65].map(strength => setWallpaperSettings({ ...DEFAULT_WALLPAPER, enabled: true, strength }));
    assert.equal(get(wallpaper).settings.strength, 65);
    sealSaveAdmission();
    let closed = false;
    const closing = flushAllSaves().then(() => { closed = true; });
    await turn(); assert.equal(closed, false);
    await assert.rejects(setWallpaperSettings(DEFAULT_WALLPAPER), /app is closing/);
    await Promise.all([...changes, closing]);
    const preferences = f.writes.filter(write => write.path === "/managed/appearance.json");
    assert.equal(preferences.length, 1);
    assert.equal(JSON.parse(preferences[0].text).strength, 65);
  });
});

test("image import/clear use native managed-image commands and persist no source paths or data URLs", async () => {
  await environment(async f => {
    await importWallpaper("/source/photo.webp");
    assert.equal(get(wallpaper).settings.enabled, true);
    assert.deepEqual(get(wallpaper).image, image);
    await clearWallpaper();
    assert.equal(get(wallpaper).image, null);
    assert.equal(get(wallpaper).settings.enabled, false);
    for (const write of f.writes) {
      assert.ok(!write.text.includes("/source") && !write.text.includes("data:image"));
    }
    assert.ok(f.calls.includes("import_wallpaper") && f.calls.includes("clear_wallpaper"));
  });
});

test("pending image imports merge later settings and a failed second import retains the first committed copy", async () => {
  await environment(async f => {
    await initWallpaper();
    const gate = deferred(), nextImage = { ...image, dataUrl: "data:image/png;base64,AQ==" };
    let managed = image, imports = 0;
    f.imageHandler(async (command, args) => {
      if (command !== "import_wallpaper") return managed;
      imports += 1;
      if ((args as { path: string }).path === "/source/second") throw new Error("Image decode failed");
      await gate.promise; managed = nextImage; return managed;
    });
    const first = importWallpaper("/source/first");
    const second = assert.rejects(importWallpaper("/source/second"), /Image decode failed/);
    await turn(); assert.equal(imports, 1, "the second decoder must wait for the first response to commit");
    await setWallpaperSettings({ ...get(wallpaper).settings, fit: "contain", strength: 85 });
    gate.resolve(); await Promise.all([first, second]);
    assert.deepEqual(get(wallpaper).image, managed);
    assert.equal(get(wallpaper).settings.strength, 85);
    assert.equal(get(wallpaper).settings.fit, "contain");
    assert.equal(imports, 2);
    await assert.rejects(flushAllSaves(), /save or export failed/);
  });
});

test("explicitly disabling a background while an image is importing wins over automatic enable", async () => {
  await environment(async f => {
    await initWallpaper();
    await setWallpaperSettings({ ...get(wallpaper).settings, enabled: true });
    const gate = deferred();
    f.imageHandler(async () => { await gate.promise; return image; });
    const importing = importWallpaper("/source/next");
    await turn();
    await setWallpaperSettings({ ...get(wallpaper).settings, enabled: false });
    gate.resolve(); await importing;
    assert.deepEqual(get(wallpaper).image, image);
    assert.equal(get(wallpaper).settings.enabled, false);
  });
});

test("late catalog startup reads cannot replace a newly imported definition with the same ID", async () => {
  await environment(async f => {
    const old = custom("catalog-race-theme");
    f.files.set("/managed/themes/catalog-race-theme.json", JSON.stringify(old));
    const gate = deferred();
    let reading = false;
    f.readHandler(async (path, value) => {
      if (path === "/managed/themes/catalog-race-theme.json") { reading = true; await gate.promise; }
      return value;
    });
    const initial = initTheme();
    await turn(); assert.equal(reading, true);
    const latest = { ...old, name: "New definition", colors: { accent: "#334455" } };
    f.files.set("/source/latest.json", JSON.stringify(latest));
    await importThemeFile("/source/latest.json", () => true);
    gate.resolve(); await initial;
    assert.equal(get(theme), "user:catalog-race-theme");
    assert.equal(getUserTheme("user:catalog-race-theme")?.colors.accent, "#334455");
  });
});

test("a full theme catalog rejects a new ID without changing the palette but permits confirmed replacement", async () => {
  await environment(async f => {
    for (let index = 0; index < 128; index += 1) {
      f.files.set(`/managed/themes/catalog-slot-${index}.json`, JSON.stringify(custom(`catalog-slot-${index}`)));
    }
    await setTheme("dark");
    const overflow = custom("catalog-overflow");
    f.files.set("/source/overflow.json", JSON.stringify(overflow));
    await assert.rejects(importThemeFile("/source/overflow.json"), /already contains 128/);
    assert.equal(get(theme), "dark");
    assert.equal(getUserTheme("user:catalog-overflow"), null);
    assert.equal(f.files.has("/managed/themes/catalog-overflow.json"), false);
    await assert.rejects(flushAllSaves(), /save or export failed/);
    registerUserTheme(custom("catalog-slot-0"));
    const replacement = { ...custom("catalog-slot-0"), name: "Replacement", colors: { accent: "#665544" } };
    f.files.set("/source/replacement.json", JSON.stringify(replacement));
    const writesBefore = f.writes.length;
    assert.equal(await importThemeFile("/source/replacement.json", () => false), null);
    assert.equal(f.writes.length, writesBefore, "cancelling replacement must not write the managed file");
    assert.equal(await importThemeFile("/source/replacement.json", () => true), "user:catalog-slot-0");
    assert.equal(getUserTheme("user:catalog-slot-0")?.colors.accent, "#665544");
    assert.equal([...f.files.keys()].filter(path => path.startsWith("/managed/themes/")).length, 128);
  });
});

test("concurrent unique theme imports cannot both take the last remaining catalog slot", async () => {
  await environment(async f => {
    for (let index = 0; index < 127; index += 1) {
      f.files.set(`/managed/themes/concurrent-slot-${index}.json`, JSON.stringify(custom(`concurrent-slot-${index}`)));
    }
    f.files.set("/source/last-a.json", JSON.stringify(custom("last-slot-a")));
    f.files.set("/source/last-b.json", JSON.stringify(custom("last-slot-b")));
    const gate = deferred();
    f.holdWrite(gate.promise);
    const first = importThemeFile("/source/last-a.json");
    const rejected = assert.rejects(importThemeFile("/source/last-b.json"), /already contains 128/);
    await turn();
    assert.deepEqual(f.writes.map(write => write.path), ["/managed/themes/last-slot-a.json"]);
    gate.resolve(); await Promise.all([first, rejected]);
    assert.equal(get(theme), "user:last-slot-a");
    assert.equal(f.files.has("/managed/themes/last-slot-b.json"), false);
    assert.equal(getUserTheme("user:last-slot-b"), null);
    assert.equal([...f.files.keys()].filter(path => path.startsWith("/managed/themes/")).length, 128);
    await assert.rejects(flushAllSaves(), /save or export failed/);
  });
});
