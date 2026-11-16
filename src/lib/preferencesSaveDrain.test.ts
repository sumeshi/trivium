import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { DEFAULT_THEME } from "./themeModel.ts";
import { setTheme, theme } from "./theme.ts";
import { DEFAULT_TABLE_TEXT_SIZE, setTableTextSize, tableTextSize } from "./tableTextSize.ts";
import { flushAllSaves, reopenSaveAdmission, sealSaveAdmission } from "./saveDrain.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(yes => { resolve = yes; });
  return { promise, resolve };
}
const turn = () => new Promise<void>(resolve => setImmediate(resolve));

async function withPreferences(write: (value: unknown) => Promise<void>, run: (cache: Map<string, string>) => Promise<void>) {
  reopenSaveAdmission();
  await flushAllSaves().catch(() => {});
  const previous = new Map(["window", "document", "localStorage"].map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]));
  const values = new Map<string, string>();
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => { values.set(key, value); },
  } });
  Object.defineProperty(globalThis, "document", { configurable: true, value: { documentElement: {
    setAttribute() {}, style: { removeProperty() {}, setProperty() {} },
  } } });
  Object.defineProperty(globalThis, "window", { configurable: true, value: { __TAURI_INTERNALS__: {
    invoke: async (command: string, args: unknown) => {
      if (command === "plugin:path|resolve_directory") return "/synthetic";
      if (command === "plugin:path|join") return (args as { paths: string[] }).paths.join("/");
      if (command === "plugin:fs|mkdir") return;
      assert.equal(command, "plugin:fs|write_text_file");
      assert.ok(args instanceof Uint8Array);
      await write(JSON.parse(new TextDecoder().decode(args)));
    },
  } } });
  try { await run(values); }
  finally {
    reopenSaveAdmission();
    await Promise.all([setTheme(DEFAULT_THEME), setTableTextSize(DEFAULT_TABLE_TEXT_SIZE)]);
    await flushAllSaves().catch(() => {});
    for (const [key, descriptor] of previous) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor);
      else Reflect.deleteProperty(globalThis, key);
    }
  }
}

test("normal close drains theme and text preference files before exiting", async () => {
  const gate = deferred();
  const writes: unknown[] = [];
  await withPreferences(async value => { writes.push(value); await gate.promise; }, async cache => {
    const choices = [setTheme("dark"), setTableTextSize(130)];
    sealSaveAdmission();
    let closed = false;
    const close = flushAllSaves().then(() => { closed = true; });
    try {
      await turn();
      assert.equal(writes.length, 2);
      assert.equal(closed, false);
      await assert.rejects(setTheme("yukari"), /app is closing/);
      await assert.rejects(setTableTextSize(145), /app is closing/);
      assert.equal(get(theme), "dark");
      assert.equal(get(tableTextSize), 130);
      assert.equal(JSON.parse(cache.get("trivium:theme")!).theme, "dark");
      assert.equal(cache.get("trivium:table-text-size"), "130");
    } finally {
      gate.resolve();
      await Promise.all([...choices, close]);
    }
    assert.equal(closed, true);
  });
});

test("already accepted queued preference changes complete after admission seals", async () => {
  const gate = deferred();
  const writes: unknown[] = [];
  await withPreferences(async value => { writes.push(value); if (writes.length === 1) await gate.promise; }, async () => {
    const choices = [setTableTextSize(115), setTableTextSize(130), setTableTextSize(145)];
    sealSaveAdmission();
    const close = flushAllSaves();
    try {
      await turn();
      assert.deepEqual(writes, [115]);
    } finally {
      gate.resolve();
      await Promise.all([...choices, close]);
    }
    assert.deepEqual(writes, [115, 130, 145]);
  });
});

test("native preference failures keep the live fallback but are visible to normal close", async () => {
  let fail = true;
  await withPreferences(async () => { if (fail) throw new Error("synthetic preference disk failure"); }, async cache => {
    await Promise.all([setTheme("dark"), setTableTextSize(115)]);
    assert.equal(get(theme), "dark");
    assert.equal(get(tableTextSize), 115);
    assert.equal(cache.get("trivium:table-text-size"), "115");
    await assert.rejects(flushAllSaves(), /save or export failed/);
    fail = false;
    await Promise.all([setTheme("dark"), setTableTextSize(115)]);
    await flushAllSaves();
  });
});
