import assert from "node:assert/strict";
import { test } from "node:test";

import {
  MAX_PHYSICAL_HEIGHT,
  ROW_HEIGHT,
  clampRow,
  logicalRowAt,
  maxScrollTop,
  physicalHeight,
  scrollTopForRow,
} from "./virtualScroll.ts";

const VIEWPORT = 800;
const TWO_MILLION = 2_000_000;
// Keep in sync with the table's window: one viewport of rows plus both buffers.
const BUFFER = 8;
const PAGE_SIZE = 250;

const viewportRows = (viewport: number) => Math.ceil(viewport / ROW_HEIGHT);
const visibleCount = (viewport: number) => viewportRows(viewport) + BUFFER * 2;

test("2M rows keep the physical spacer at the 8,000,000px cap", () => {
  assert.equal(physicalHeight(TWO_MILLION), MAX_PHYSICAL_HEIGHT);
  assert.ok(physicalHeight(TWO_MILLION) <= MAX_PHYSICAL_HEIGHT);
});

test("2M rows round-trip first, middle and last result rows", () => {
  for (const row of [0, 999_999, 1_999_999]) {
    const top = scrollTopForRow(row, TWO_MILLION, VIEWPORT);
    const back = logicalRowAt(top, TWO_MILLION, VIEWPORT);
    // Compressed positions are integer pixels, so allow the documented ±1 row
    // rounding tolerance. These three rows happen to land exactly.
    assert.ok(
      Math.abs(back - row) <= 1,
      `row ${row} -> scrollTop ${top} -> row ${back}`
    );
  }
});

test("a 1,000-row result keeps the identity mapping", () => {
  const total = 1_000;
  assert.equal(physicalHeight(total), total * ROW_HEIGHT);

  for (const row of [0, 1, 500, 900]) {
    const top = scrollTopForRow(row, total, VIEWPORT);
    assert.equal(top, row * ROW_HEIGHT);
    assert.equal(logicalRowAt(top, total, VIEWPORT), row);
  }
});

test("a shrink clamps the current row into the new result count", () => {
  assert.equal(clampRow(1_500_000, 1_000), 999);
  assert.equal(clampRow(-1, 1_000), 0);
  assert.equal(clampRow(5, 3), 2);
  assert.equal(clampRow(5, 0), 0);
});

test("the last page window contains the final row without duplicates", () => {
  const total = TWO_MILLION;
  const maxScroll = maxScrollTop(total, VIEWPORT);
  const anchor = logicalRowAt(maxScroll, total, VIEWPORT);
  const maxStart = Math.max(0, total - viewportRows(VIEWPORT));
  const start = Math.min(maxStart, Math.max(0, anchor - BUFFER));
  const end = Math.min(total, start + visibleCount(VIEWPORT));

  assert.equal(anchor, total - 1);
  assert.equal(end, total);
  assert.equal(start, total - viewportRows(VIEWPORT));

  const positions = new Set<number>();
  for (let position = start; position < end; position += 1) {
    positions.add(position);
  }
  assert.equal(positions.size, end - start);
  assert.ok(positions.has(total - 1));

  // The visible window belongs to the final backend page.
  assert.equal(Math.floor(start / PAGE_SIZE), Math.floor((total - 1) / PAGE_SIZE));
});

test("empty results have no scroll range", () => {
  assert.equal(physicalHeight(0), 0);
  assert.equal(maxScrollTop(0, VIEWPORT), 0);
  assert.equal(scrollTopForRow(0, 0, VIEWPORT), 0);
  assert.equal(logicalRowAt(0, 0, VIEWPORT), 0);
});
