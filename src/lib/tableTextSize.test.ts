import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import {
  adjustTableTextSize, DEFAULT_TABLE_TEXT_SIZE, initTableTextSize,
  parseTableTextSize, setTableTextSize, TABLE_TEXT_SIZES, tableTextSize,
} from "./tableTextSize.ts";

function withStorage(storage: Pick<Storage, "getItem" | "setItem">, run: () => void) {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: storage });
  try {
    run();
  } finally {
    if (previous) Object.defineProperty(globalThis, "localStorage", previous);
    else Reflect.deleteProperty(globalThis, "localStorage");
    setTableTextSize(DEFAULT_TABLE_TEXT_SIZE);
  }
}

test("table text preferences accept only supported sizes without coercion", () => {
  for (const size of TABLE_TEXT_SIZES) assert.equal(parseTableTextSize(size), size);
  for (const invalid of [null, undefined, "115", 0, 99, 150, 100.5, NaN, Infinity, [], {}, true]) {
    assert.equal(parseTableTextSize(invalid), null);
  }
});

test("size changes are bounded, resettable and restored from the saved preference", () => {
  const values = new Map<string, string>();
  withStorage({ getItem: (key) => values.get(key) ?? null, setItem: (key, value) => { values.set(key, value); } }, () => {
    initTableTextSize();
    assert.equal(get(tableTextSize), 100);
    for (let i = 0; i < 10; i++) adjustTableTextSize(1);
    assert.equal(get(tableTextSize), 145);
    assert.equal(values.get("trivium:table-text-size"), "145");
    for (let i = 0; i < 10; i++) adjustTableTextSize(-1);
    assert.equal(get(tableTextSize), 85);
    setTableTextSize(DEFAULT_TABLE_TEXT_SIZE);
    assert.equal(get(tableTextSize), 100);
    values.set("trivium:table-text-size", "130");
    initTableTextSize();
    assert.equal(get(tableTextSize), 130);
    setTableTextSize(900);
    assert.equal(get(tableTextSize), 130);
    assert.equal(values.get("trivium:table-text-size"), "130");
    for (const invalid of ["bad json", '"145"', "null", "{}", "151"]) {
      values.set("trivium:table-text-size", invalid);
      initTableTextSize();
      assert.equal(get(tableTextSize), 100);
    }
  });
});

test("unavailable storage preserves a usable live setting", () => {
  withStorage({ getItem: () => { throw new Error("blocked"); }, setItem: () => { throw new Error("blocked"); } }, () => {
    assert.doesNotThrow(initTableTextSize);
    assert.equal(get(tableTextSize), 100);
    assert.doesNotThrow(() => setTableTextSize(115));
    assert.equal(get(tableTextSize), 115);
    assert.doesNotThrow(() => adjustTableTextSize(-1));
    assert.equal(get(tableTextSize), 100);
  });
});
