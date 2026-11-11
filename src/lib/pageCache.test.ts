import assert from "node:assert/strict";
import { test } from "node:test";

import { estimatePageBytes, selectEvictions } from "./pageCache.ts";
import type { PageCacheEntry } from "./pageCache.ts";
import type { CachedRow } from "./components/project_view/state.ts";

const MIB = 1024 * 1024;
const MAX_BYTES = 16 * MIB;

function rowWithText(text: string): CachedRow {
  return {
    row_index: 0,
    data: {},
    cells: {},
    flag: "",
    tags: [],
    bookmarked: false,
    memo: "",
    displayCache: { column: text },
    previewCache: { column: text },
  } as CachedRow;
}

function cachePages(entries: Array<[number, number]>): Map<number, PageCacheEntry> {
  const pages = new Map<number, PageCacheEntry>();
  for (const [page, at] of entries) {
    pages.set(page, { at, bytes: 1024 });
  }
  return pages;
}

test("estimatePageBytes counts the rendered cell strings", () => {
  const text = "x".repeat(10_000);
  const bytes = estimatePageBytes([rowWithText(text)]);
  // displayCache and previewCache each retain the text, plus row overhead.
  assert.ok(bytes >= 20_000);
});

test("six pages with the oldest access evicts the oldest, keeping current", () => {
  const pages = cachePages([
    [0, 0],
    [1, 1],
    [2, 2],
    [3, 3],
    [4, 4],
    [5, 5],
  ]);

  assert.deepEqual(selectEvictions(pages, 5, 5, MAX_BYTES), [0]);
});

test("total bytes over the bound evict oldest pages first and keep current", () => {
  const pages = new Map<number, PageCacheEntry>([
    [0, { at: 0, bytes: 7 * MIB }],
    [1, { at: 1, bytes: 7 * MIB }],
    [2, { at: 2, bytes: 7 * MIB }],
  ]);

  assert.deepEqual(selectEvictions(pages, 2, 5, MAX_BYTES), [0]);
});

test("a page larger than the bound is evicted even when it is current", () => {
  const pages = new Map<number, PageCacheEntry>([
    [0, { at: 0, bytes: 1024 }],
    [1, { at: 1, bytes: MAX_BYTES + 1 }],
  ]);

  assert.deepEqual(selectEvictions(pages, 1, 5, MAX_BYTES), [1]);
});

test("a fresh access keeps a page that would otherwise be oldest", () => {
  const pages = cachePages([
    [0, 0],
    [1, 1],
    [2, 2],
    [3, 3],
    [4, 4],
    [5, 5],
  ]);
  pages.set(0, { at: 99, bytes: 1024 });

  assert.deepEqual(selectEvictions(pages, 5, 5, MAX_BYTES), [1]);
});
