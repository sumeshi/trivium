import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { beginThemePreview, importThemeFile, setTheme, theme } from "./theme.ts";
import { createThemeDraft, themeContrastWarning, validThemeDraft, withPreviewColor } from "./themeEditor.ts";
import { EDITABLE_THEME_COLORS, getUserTheme, normalizeThemeHex, themeTokens } from "./themeModel.ts";
import { flushAllSaves, reopenSaveAdmission, sealSaveAdmission } from "./saveDrain.ts";

const turn = () => new Promise<void>(resolve => setImmediate(resolve));
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(yes => { resolve = yes; });
  return { promise, resolve };
}

async function environment(run: (f: {
  files: Map<string, string>; cache: Map<string, string>; styles: Map<string, string>; attrs: Map<string, string>;
  writes: string[]; hold: (value: Promise<void> | null) => void;
  fail: (value: boolean | ((destination: string) => boolean)) => void;
  holdRead: (path: string, value: Promise<void>) => void;
}) => Promise<void>) {
  reopenSaveAdmission(); await flushAllSaves().catch(() => {});
  const previous = new Map(["window", "document", "localStorage"].map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]));
  const files = new Map<string, string>(), cache = new Map<string, string>(), styles = new Map<string, string>(), attrs = new Map<string, string>();
  const writes: string[] = [], reads = new Map<string, Promise<void>>();
  let gate: Promise<void> | null = null;
  let fail: boolean | ((destination: string) => boolean) = false;
  Object.defineProperty(globalThis, "document", { configurable: true, value: { documentElement: {
    setAttribute: (key: string, value: string) => attrs.set(key, value),
    style: { setProperty: (key: string, value: string) => styles.set(key, value), removeProperty: (key: string) => styles.delete(key) },
  } } });
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: {
    getItem: (key: string) => cache.get(key) ?? null, setItem: (key: string, value: string) => cache.set(key, value),
  } });
  Object.defineProperty(globalThis, "window", { configurable: true, value: { __TAURI_INTERNALS__: {
    invoke: async (command: string, args: unknown, options?: { headers?: { path?: string } }) => {
      if (command === "plugin:path|resolve_directory") return "/managed";
      if (command === "plugin:path|join") return (args as { paths: string[] }).paths.join("/");
      if (command === "plugin:fs|mkdir") return;
      if (command === "plugin:fs|read_dir") return [...files.keys()].filter(path => path.startsWith("/managed/themes/"))
        .map(path => ({ name: path.split("/").at(-1), isFile: true }));
      const path = (args as { path: string })?.path;
      if (command === "plugin:fs|read_text_file" || command === "read_import_text") {
        await reads.get(path);
        const text = files.get(path) ?? "";
        return command === "read_import_text" ? text : [...new TextEncoder().encode(text)];
      }
      if (command === "plugin:fs|write_text_file" || command === "save_theme_profile") {
        const profile = args as { id: string; text: string };
        const destination = command === "save_theme_profile" ? `/managed/themes/${profile.id}.json`
          : decodeURIComponent(options?.headers?.path ?? "");
        writes.push(destination); await gate;
        if (typeof fail === "function" ? fail(destination) : fail) throw new Error("Theme disk failure");
        files.set(destination, command === "save_theme_profile" ? profile.text : new TextDecoder().decode(args as Uint8Array)); return;
      }
      throw new Error(`Unexpected theme operation: ${command}`);
    },
  } } });
  try { await run({ files, cache, styles, attrs, writes, hold: value => { gate = value; }, fail: value => { fail = value; },
    holdRead: (path, value) => { reads.set(path, value); } }); }
  finally {
    reopenSaveAdmission(); await flushAllSaves().catch(() => {});
    for (const [key, descriptor] of previous) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor); else Reflect.deleteProperty(globalThis, key);
    }
  }
}

test("theme drafts expose inherited editable colors without changing builtin or semantic values", () => {
  const original = themeTokens("dark");
  const draft = createThemeDraft("dark", "editor-model-copy");
  assert.deepEqual(Object.keys(draft.colors).sort(), [...EDITABLE_THEME_COLORS].sort());
  const painted = withPreviewColor(draft, "surface", "#AbC");
  assert.equal(painted.colors.surface, "#aabbcc");
  assert.equal(draft.colors.surface, original.surface);
  assert.equal(themeTokens("dark").positive, original.positive);
  assert.equal(themeTokens("dark").surface, original.surface);
  assert.equal(withPreviewColor(painted, "surface", "#a"), painted);
  assert.equal(normalizeThemeHex("#12345678"), null);
  assert.equal(validThemeDraft({ ...draft, colors: { ...draft.colors, fg: "#1" } }), null);
  assert.equal(validThemeDraft({ ...draft, name: " " }), null);
  assert.equal(themeContrastWarning(draft), "");
  assert.match(themeContrastWarning({ ...draft, colors: { ...draft.colors, fg: draft.colors.surface } }), /Text.*low contrast/);
});

test("editing previews paint only and cancel restores the selected theme without writing preferences", async () => {
  await environment(async f => {
    await setTheme("dark");
    const files = [...f.files], cache = [...f.cache], writes = f.writes.length;
    const preview = beginThemePreview(), draft = createThemeDraft("dark", "editor-cancel-copy");
    preview.apply(withPreviewColor(draft, "accent", "#123456"));
    assert.equal(f.styles.get("--accent"), "#123456");
    assert.equal(get(theme), "dark");
    await turn();
    assert.deepEqual([...f.files], files); assert.deepEqual([...f.cache], cache);
    assert.equal(f.writes.length, writes); assert.equal(getUserTheme("user:editor-cancel-copy"), null);
    assert.throws(() => preview.save({ ...draft, colors: { ...draft.colors, accent: "#1" } }));
    preview.cancel();
    assert.equal(f.styles.size, 0); assert.equal(f.attrs.get("data-theme"), "dark");
    assert.equal(f.writes.length, writes);
    await flushAllSaves();
  });
});

test("saving a builtin copy and editing the saved profile retain its ID and persisted colors", async () => {
  await environment(async f => {
    await setTheme("dark");
    const draft = createThemeDraft("dark", "editor-roundtrip-copy");
    draft.name = "Investigation palette"; draft.colors.accent = "#345678";
    const preview = beginThemePreview(); preview.apply(draft);
    assert.equal(await preview.save(draft), "user:editor-roundtrip-copy");
    assert.equal(get(theme), "user:editor-roundtrip-copy");
    assert.equal(JSON.parse(f.files.get("/managed/theme.json")!).theme, get(theme));
    assert.equal(JSON.parse(f.cache.get("trivium:theme")!).theme, get(theme));
    const saved = JSON.parse(f.files.get("/managed/themes/editor-roundtrip-copy.json")!);
    assert.equal(saved.name, draft.name); assert.equal(saved.colors.accent, "#345678");
    const editing = createThemeDraft(get(theme), "unused-new-id");
    assert.equal(editing.id, draft.id);
    editing.colors.accent = "#654321";
    const next = beginThemePreview(); await next.save(editing);
    assert.equal(get(theme), "user:editor-roundtrip-copy");
    assert.equal(getUserTheme(get(theme))?.colors.accent, "#654321");
    assert.equal([...f.files.keys()].filter(path => path.startsWith("/managed/themes/")).length, 1);
  });
});

test("a newer explicit theme selection wins over a pending save and its eventual cancel", async () => {
  await environment(async f => {
    await setTheme("dark");
    const gate = deferred(); f.hold(gate.promise);
    const preview = beginThemePreview(), draft = createThemeDraft("dark", "editor-late-copy");
    const saving = preview.save(draft);
    await turn();
    const selected = setTheme("light");
    assert.equal(get(theme), "light");
    gate.resolve(); await Promise.all([saving, selected]);
    preview.cancel();
    assert.equal(get(theme), "light"); assert.equal(f.attrs.get("data-theme"), "light");
    assert.equal(f.styles.size, 0);
    assert.equal(JSON.parse(f.files.get("/managed/theme.json")!).theme, "light");
    assert.ok(f.files.has("/managed/themes/editor-late-copy.json"));
  });
});

test("cancelled and superseded previews cannot save or repaint over a newer choice", async () => {
  await environment(async f => {
    await setTheme("dark");
    const draft = createThemeDraft("dark", "editor-superseded-copy");
    const old = beginThemePreview(); old.apply(draft);
    const latest = beginThemePreview(); latest.apply(withPreviewColor(draft, "accent", "#112233"));
    old.cancel(); assert.equal(f.styles.get("--accent"), "#112233");
    assert.equal(old.apply(draft), false);
    await assert.rejects(old.save(draft), /selected theme changed/);
    latest.cancel(); assert.equal(f.styles.size, 0);
    await flushAllSaves();
  });
});

test("normal close waits for an accepted theme save including its selected preference", async () => {
  await environment(async f => {
    await setTheme("dark");
    const gate = deferred(); f.hold(gate.promise);
    const preview = beginThemePreview(), draft = createThemeDraft("dark", "editor-drained-copy");
    const saving = preview.save(draft); sealSaveAdmission();
    let finished = false;
    const closing = flushAllSaves().then(() => { finished = true; });
    await turn(); assert.equal(finished, false);
    gate.resolve(); await Promise.all([saving, closing]);
    assert.equal(JSON.parse(f.files.get("/managed/theme.json")!).theme, "user:editor-drained-copy");
    assert.ok(f.files.has("/managed/themes/editor-drained-copy.json"));
  });
});

test("failed saves retain the current palette and never register a draft as a saved theme", async () => {
  await environment(async f => {
    await setTheme("dark"); f.fail(true);
    const preview = beginThemePreview(), draft = createThemeDraft("dark", "editor-failed-copy");
    preview.apply(withPreviewColor(draft, "accent", "#445566"));
    await assert.rejects(preview.save(draft), /Theme disk failure/);
    assert.equal(get(theme), "dark"); assert.equal(getUserTheme("user:editor-failed-copy"), null);
    preview.cancel(); assert.equal(f.styles.size, 0);
    await assert.rejects(flushAllSaves(), /save or export failed/);
  });
});

test("a failed replacement retains the saved profile and its selected palette", async () => {
  await environment(async f => {
    await setTheme("dark");
    const draft = createThemeDraft("dark", "editor-preserved-copy");
    const first = beginThemePreview(); await first.save(draft);
    const path = "/managed/themes/editor-preserved-copy.json", original = f.files.get(path);
    const editing = withPreviewColor(createThemeDraft(get(theme), "unused"), "accent", "#654321");
    f.fail(true);
    const preview = beginThemePreview(); preview.apply(editing);
    await assert.rejects(preview.save(editing), /Theme disk failure/);
    preview.cancel();
    assert.equal(f.files.get(path), original);
    assert.equal(getUserTheme(get(theme))?.colors.accent, draft.colors.accent);
    assert.equal(f.styles.get("--accent"), draft.colors.accent);
    await assert.rejects(flushAllSaves(), /save or export failed/);
  });
});

test("a preference-only failure reports the committed profile and keeps its palette consistent", async () => {
  await environment(async f => {
    await setTheme("dark");
    const draft = createThemeDraft("dark", "editor-preference-failure");
    await beginThemePreview().save(draft);
    const changed = withPreviewColor(draft, "accent", "#654321");
    f.fail(destination => destination === "/managed/theme.json");
    const preview = beginThemePreview(); preview.apply(changed);
    await assert.rejects(preview.save(changed), /Theme saved, but its selection preference could not be stored/);
    assert.equal(JSON.parse(f.files.get("/managed/themes/editor-preference-failure.json")!).colors.accent, "#654321");
    assert.equal(getUserTheme(get(theme))?.colors.accent, "#654321");
    assert.equal(f.styles.get("--accent"), "#654321");
    preview.cancel();
    assert.equal(f.styles.get("--accent"), "#654321", "Cancel cannot paint a stale definition over the committed profile");
    await assert.rejects(flushAllSaves(), /save or export failed/);
  });
});

test("out-of-order file reads cannot make an older import replace a newer successful one", async () => {
  await environment(async f => {
    await setTheme("dark");
    const slow = createThemeDraft("dark", "editor-import-old"), fast = createThemeDraft("dark", "editor-import-new");
    f.files.set("/source/slow.json", JSON.stringify(slow)); f.files.set("/source/fast.json", JSON.stringify(fast));
    const read = deferred(); f.holdRead("/source/slow.json", read.promise);
    const old = importThemeFile("/source/slow.json"); await turn();
    await importThemeFile("/source/fast.json");
    read.resolve(); await old;
    assert.equal(get(theme), "user:editor-import-new");
    assert.equal(JSON.parse(f.files.get("/managed/theme.json")!).theme, "user:editor-import-new");
    assert.ok(f.files.has("/managed/themes/editor-import-old.json"));
  });
});
