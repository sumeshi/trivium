import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import {
  adjustTableTextSize, DEFAULT_TABLE_TEXT_SIZE, initTableTextSize,
  parseTableTextSize, setTableTextSize, TABLE_TEXT_SIZES, tableTextSize,
} from "./tableTextSize.ts";

type NativeInvoke = (command: string, args: unknown, options?: { headers?: Record<string, string> }) => Promise<unknown>;

async function withStorage(storage: Pick<Storage, "getItem" | "setItem">, run: () => Promise<void>, invoke?: NativeInvoke) {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
  const previousWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: storage });
  if (invoke) Object.defineProperty(globalThis, "window", { configurable: true, value: { __TAURI_INTERNALS__: { invoke } } });
  try {
    await run();
  } finally {
    await setTableTextSize(DEFAULT_TABLE_TEXT_SIZE);
    if (previous) Object.defineProperty(globalThis, "localStorage", previous);
    else Reflect.deleteProperty(globalThis, "localStorage");
    if (previousWindow) Object.defineProperty(globalThis, "window", previousWindow);
    else Reflect.deleteProperty(globalThis, "window");
  }
}

function memoryStorage(initial?: string) {
  const values = new Map<string, string>(initial ? [["trivium:table-text-size", initial]] : []);
  return { values, getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => { values.set(key, value); } };
}

function nativeStorage(read: () => Promise<string>, write: (size: number) => Promise<void>): NativeInvoke {
  return async (command, args, options) => {
    if (command === "plugin:path|resolve_directory") return "/native";
    if (command === "plugin:path|join") return (args as { paths: string[] }).paths.join("/");
    if (command === "plugin:fs|mkdir") return;
    if (command === "plugin:fs|read_text_file") {
      assert.equal((args as { path: string }).path, "/native/table-text-size.json");
      return new TextEncoder().encode(await read());
    }
    assert.equal(command, "plugin:fs|write_text_file");
    assert.equal(decodeURIComponent(options?.headers?.path ?? ""), "/native/table-text-size.json");
    assert.ok(args instanceof Uint8Array);
    await write(JSON.parse(new TextDecoder().decode(args)));
  };
}

test("table text preferences accept only supported sizes without coercion", () => {
  for (const size of TABLE_TEXT_SIZES) assert.equal(parseTableTextSize(size), size);
  for (const invalid of [null, undefined, "115", 0, 99, 150, 100.5, NaN, Infinity, [], {}, true]) {
    assert.equal(parseTableTextSize(invalid), null);
  }
});

test("size changes are bounded, resettable and restored from the saved preference", async () => {
  const storage = memoryStorage(), { values } = storage;
  await withStorage(storage, async () => {
    await initTableTextSize();
    assert.equal(get(tableTextSize), 100);
    for (let i = 0; i < 10; i++) adjustTableTextSize(1);
    assert.equal(get(tableTextSize), 145);
    assert.equal(values.get("trivium:table-text-size"), "145");
    for (let i = 0; i < 10; i++) adjustTableTextSize(-1);
    assert.equal(get(tableTextSize), 85);
    await setTableTextSize(DEFAULT_TABLE_TEXT_SIZE);
    assert.equal(get(tableTextSize), 100);
    values.set("trivium:table-text-size", "130");
    await initTableTextSize();
    assert.equal(get(tableTextSize), 130);
    await setTableTextSize(900);
    assert.equal(get(tableTextSize), 130);
    assert.equal(values.get("trivium:table-text-size"), "130");
    for (const invalid of ["bad json", '"145"', "null", "{}", "151"]) {
      values.set("trivium:table-text-size", invalid);
      await initTableTextSize();
      assert.equal(get(tableTextSize), 100);
    }
  });
});

test("unavailable storage preserves a usable live setting", async () => {
  await withStorage({ getItem: () => { throw new Error("blocked"); }, setItem: () => { throw new Error("blocked"); } }, async () => {
    await assert.doesNotReject(initTableTextSize);
    assert.equal(get(tableTextSize), 100);
    const changed = setTableTextSize(115);
    assert.equal(get(tableTextSize), 115);
    await assert.doesNotReject(changed);
    await assert.doesNotReject(adjustTableTextSize(-1));
    assert.equal(get(tableTextSize), 100);
  });
});

test("native preference replaces a stale browser cache and refreshes it", async () => {
  const storage = memoryStorage("130");
  await withStorage(storage, async () => {
    const loaded = initTableTextSize();
    assert.equal(get(tableTextSize), 130, "The cache provides immediate startup text size");
    await loaded;
    assert.equal(get(tableTextSize), 145);
    assert.equal(storage.getItem("trivium:table-text-size"), "145");
  }, nativeStorage(async () => "145", async () => {}));
});

test("a delayed native read cannot undo a newly selected size or its cache", async () => {
  const storage = memoryStorage("100");
  let release: (text: string) => void = () => {};
  let started: () => void = () => {};
  const reading = new Promise<void>(resolve => { started = resolve; });
  const file = new Promise<string>(resolve => { release = resolve; });
  await withStorage(storage, async () => {
    const loaded = initTableTextSize();
    try {
      await reading;
      const changed = setTableTextSize(115);
      assert.equal(get(tableTextSize), 115);
      release("145");
      await Promise.all([loaded, changed]);
      assert.equal(get(tableTextSize), 115);
      assert.equal(storage.getItem("trivium:table-text-size"), "115");
    } finally { release("145"); await loaded; }
  }, nativeStorage(() => { started(); return file; }, async () => {}));
});

test("native writes are serialized and finish with the latest synchronous choice", async () => {
  const storage = memoryStorage(), writes: number[] = [];
  let release: () => void = () => {}, started: () => void = () => {};
  const writing = new Promise<void>(resolve => { started = resolve; });
  const first = new Promise<void>(resolve => { release = resolve; });
  await withStorage(storage, async () => {
    const pending = [setTableTextSize(115), setTableTextSize(130), setTableTextSize(145)];
    try {
      assert.equal(get(tableTextSize), 145);
      assert.equal(storage.getItem("trivium:table-text-size"), "145");
      await writing;
      assert.deepEqual(writes, [115], "Later native writes wait for the in-flight save");
      release();
      await Promise.all(pending);
      assert.deepEqual(writes, [115, 130, 145]);
    } finally { release(); await Promise.all(pending); }
  }, nativeStorage(async () => "100", async size => { writes.push(size); if (writes.length === 1) { started(); await first; } }));
});
