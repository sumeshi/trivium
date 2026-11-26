import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";

import { estimatePageBytes, selectEvictions } from "./pageCache.ts";
import type { PageCacheEntry } from "./pageCache.ts";
import type { CachedRow } from "./components/project_view/state.ts";
import { applyAnnotationEntry, PAGE_SIZE } from "./components/project_view/state.ts";
import { createWorkspacePages } from "./workspacePages.ts";

const MIB = 1024 * 1024;
const MAX_BYTES = 16 * MIB;

function rowWithText(text: string): CachedRow {
  return {
    row_index: 0,

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

test("page estimates count exact datetime metadata once per retained cell", () => {
  const source = { text: "preview", kind: "string" as const, truncated: true };
  const row = { ...rowWithText("preview"), cells: { first: source, second: source, ordinary: source } };
  const initial = estimatePageBytes([row]);
  const datetimeRow = {
    ...row,
    cells: {
      first: { ...source, datetime_nanos: "1794625445123456789" },
      second: { ...source, datetime_nanos: "-9223372036854775808" },
      ordinary: source,
    },
  };
  const retainedNanosBytes = 19 + 20;
  assert.equal(estimatePageBytes([datetimeRow]), initial + retainedNanosBytes);
  assert.deepEqual(selectEvictions(new Map([[0, { at: 1, bytes: estimatePageBytes([datetimeRow]) }]]),
    0, 5, initial + retainedNanosBytes - 1), [0], "datetime metadata participates in the page byte bound");
});

test("page estimates include notes and tags independently of source cells", () => {
  const row = rowWithText("source");
  const initial = estimatePageBytes([row]);
  assert.equal(estimatePageBytes([{ ...row, memo: "note", tags: ["one", "two"] }]), initial + 10);
});

test("oversized notes and tags cannot bypass the page cache bound on load", () => {
  for (const field of ["memo", "tags"] as const) {
    const pages = createWorkspacePages(() => 0);
    const text = "x".repeat(MAX_BYTES + 1);
    const row = { ...rowWithText(""), [field]: field === "memo" ? text : [text] };
    pages.registerLoadedPage(0, 0, [row]);
    assert.equal(get(pages.rowsCache).size, 0);
    assert.equal(get(pages.positionToRowIndex).size, 0);
    assert.equal(get(pages.loadedPages).size, 0);
    assert.deepEqual([...get(pages.largePages)], [0]);
  }
});

test("growing annotations enforce the same bound and preserve the complete update", () => {
  for (const field of ["memo", "tags"] as const) {
    const pages = createWorkspacePages(() => 0);
    pages.registerLoadedPage(0, 0, [rowWithText("")]);
    const text = "x".repeat(MAX_BYTES + 1);
    const updated = pages.updateCachedRow(0, (row) => applyAnnotationEntry(row, {
      flag: "safe", memo: field === "memo" ? text : "", tags: field === "tags" ? [text] : [], bookmarked: false,
    }));
    assert.equal(updated?.memo, field === "memo" ? text : "");
    assert.deepEqual(updated?.tags, field === "tags" ? [text] : []);
    assert.equal(field === "memo" ? updated?.memo.length : updated?.tags[0].length, text.length);
    assert.equal(get(pages.rowsCache).size, 0);
    assert.equal(get(pages.positionToRowIndex).size, 0);
    assert.equal(get(pages.loadedPages).size, 0);
    assert.deepEqual([...get(pages.largePages)], [0]);
  }
});

test("annotation shrink and growth update retained page bytes before LRU eviction", () => {
  const pages = createWorkspacePages(() => 0);
  const register = (page: number, length: number) => pages.registerLoadedPage(page, page * PAGE_SIZE,
    [{ ...rowWithText(""), row_index: page, memo: "n".repeat(length * MIB) }]);
  register(0, 6);
  register(1, 7);
  pages.updateCachedRow(0, (row) => ({ ...row, memo: "n".repeat(MIB) }));
  register(2, 7);
  assert.deepEqual([...get(pages.loadedPages)], [0, 1, 2], "shrunk notes release their byte estimate");
  pages.updateCachedRow(0, (row) => ({ ...row, memo: "n".repeat(7 * MIB) }));
  assert.deepEqual([...get(pages.loadedPages)], [0, 2], "growth evicts the oldest other page");
  assert.deepEqual([...get(pages.rowsCache).keys()], [0, 2]);
  assert.deepEqual([...get(pages.positionToRowIndex).keys()], [0, 2 * PAGE_SIZE]);
});

test("flag and note updates preserve cell caches without scanning wide source columns", () => {
  let scans = 0;
  const cells = new Proxy({ memo: "source note field", c1: "source value" }, {
    ownKeys(target) { scans += 1; return Reflect.ownKeys(target); },
  });
  const pages = createWorkspacePages(() => 0);
  const initial = { ...rowWithText(""), displayCache: cells, previewCache: cells };
  pages.registerLoadedPage(0, 0, [initial]);
  const loadScans = scans;
  for (const memo of ["", "changed note", ""]) {
    const updated = pages.updateCachedRow(0, (row) => applyAnnotationEntry(row, {
      flag: "critical", memo, tags: ["tag"], bookmarked: true,
    }));
    assert.equal(updated?.displayCache, cells);
    assert.equal(updated?.previewCache, cells);
    assert.equal(updated?.displayCache.memo, "source note field");
    assert.equal(updated?.memo, memo);
  }
  assert.equal(scans, loadScans, "annotation updates must not enumerate source cells");
  assert.deepEqual([...get(pages.loadedPages)], [0]);
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
