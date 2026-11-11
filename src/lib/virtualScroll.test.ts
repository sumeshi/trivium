import assert from "node:assert/strict";
import { test } from "node:test";
import {
  MAX_PHYSICAL_HEIGHT, ROW_HEIGHT, clampRow, logicalOffsetAt, logicalRowAt,
  maxScrollTop, physicalHeight, scrollTopForRow, virtualWindow,
} from "./virtualScroll.ts";

const BUFFER = 8;
const close = (actual: number, expected: number) =>
  assert.ok(Math.abs(actual - expected) < 0.00001, `${actual} != ${expected}`);

function assertCoverage(top: number, total: number, viewport: number) {
  const window = virtualWindow(top, total, viewport, BUFFER);
  const physicalTop = Math.max(0, Math.min(top, maxScrollTop(total, viewport)));
  const positions: number[] = [];
  const bounds: Array<{ top: number; bottom: number }> = [];
  for (let row = window.start; row < window.end; row += 1) {
    const y = window.innerTop + (row - window.start) * ROW_HEIGHT - physicalTop;
    if (y < viewport && y + ROW_HEIGHT > 0) {
      positions.push(row);
      bounds.push({ top: y, bottom: y + ROW_HEIGHT });
    }
  }
  assert.ok(bounds.length > 0, `empty viewport: ${JSON.stringify(window)}`);
  assert.ok(bounds[0].top <= 0.00001, "blank area above the first visible row");
  assert.ok(bounds.at(-1)!.bottom >= viewport - 0.00001, "blank area below the last visible row");
  assert.equal(new Set(positions).size, positions.length);
  for (let index = 1; index < positions.length; index += 1) {
    assert.equal(positions[index], positions[index - 1] + 1);
    close(bounds[index].top, bounds[index - 1].bottom);
  }
  assert.ok(window.end - window.start <= Math.ceil(viewport / ROW_HEIGHT) + BUFFER * 2 + 1);
  assert.ok(window.height <= MAX_PHYSICAL_HEIGHT);
  return window;
}

test("uncompressed offsets remain exact pixel identities", () => {
  for (const total of [1_000, 5_199]) {
    assert.equal(physicalHeight(total), total * ROW_HEIGHT);
    for (const row of [0, 1, 499, 500, 900]) {
      const top = scrollTopForRow(row, total, 517);
      assert.equal(top, row * ROW_HEIGHT);
      assert.equal(logicalOffsetAt(top, total, 517), top);
      assert.equal(logicalRowAt(top, total, 517), row);
    }
  }
});

test("5,199 and 2M rows cover arbitrary viewports without gaps or duplicate rows", () => {
  for (const total of [5_199, 2_000_000]) {
    for (const viewport of [512, 517, 517.25, 803]) {
      const maximum = maxScrollTop(total, viewport);
      for (const top of [0, 0.5, 1, 31, 32, maximum * 0.1, maximum / 2,
        maximum * 0.9, maximum - 1, maximum - 0.5, maximum]) {
        assertCoverage(top, total, viewport);
      }
    }
  }
});

test("First and Last expose full endpoint rows at non-row-aligned viewport heights", () => {
  for (const total of [5_199, 2_000_000]) {
    for (const viewport of [512, 517, 517.25]) {
      const firstTop = scrollTopForRow(0, total, viewport);
      const first = assertCoverage(firstTop, total, viewport);
      assert.equal(first.start, 0);
      close(first.innerTop - firstTop, 0);
      const lastTop = scrollTopForRow(total - 1, total, viewport);
      const last = assertCoverage(lastTop, total, viewport);
      assert.equal(last.end, total);
      close(last.innerTop + (total - last.start) * ROW_HEIGHT - lastTop, viewport);
      assert.equal(Math.floor((last.end - 1) / 250), Math.floor((total - 1) / 250));
      // At the bottom, the anchor is the first visible row, not the last row.
      assert.equal(logicalRowAt(lastTop, total, viewport), Math.floor((total * ROW_HEIGHT - viewport) / ROW_HEIGHT));
    }
  }
});

test("centered Go targets stay fully visible after physical pixel rounding", () => {
  for (const total of [5_199, 2_000_000]) {
    for (const viewport of [517, 803]) {
      for (const target of [0, 1, 249, 250, Math.floor(total / 2), total - 2, total - 1]) {
        const topRow = Math.max(0, target - Math.floor(viewport / ROW_HEIGHT / 2));
        const top = Math.min(maxScrollTop(total, viewport), Math.round(scrollTopForRow(topRow, total, viewport)));
        const window = assertCoverage(top, total, viewport);
        const y = window.innerTop + (target - window.start) * ROW_HEIGHT - top;
        assert.ok(target >= window.start && target < window.end);
        assert.ok(y >= -0.00001, `target ${target} clipped above: ${y}`);
        assert.ok(y + ROW_HEIGHT <= viewport + 0.00001, `target ${target} clipped below: ${y}`);
      }
    }
  }
});

test("a filter shrink clamps an old scroll position to the new final viewport", () => {
  const window = assertCoverage(7_999_000, 1_000, 517);
  assert.equal(window.end, 1_000);
  assert.equal(clampRow(1_500_000, 1_000), 999);
  assert.equal(clampRow(-1, 1_000), 0);
  assert.equal(clampRow(5, 0), 0);
});

test("empty and short results do not invent a scroll range", () => {
  for (const total of [0, 1, 3]) {
    assert.equal(maxScrollTop(total, 800), 0);
    assert.equal(scrollTopForRow(5, total, 800), 0);
    assert.equal(logicalRowAt(5, total, 800), 0);
    assert.deepEqual(virtualWindow(5, total, 800, BUFFER), {
      start: 0, end: total, innerTop: 0, height: total * ROW_HEIGHT,
    });
  }
  assert.equal(physicalHeight(2_000_000), MAX_PHYSICAL_HEIGHT);
  assert.equal(physicalHeight(Number.NaN), 0);
  assert.equal(logicalRowAt(Number.NaN, 2_000_000, 800), 0);
});
