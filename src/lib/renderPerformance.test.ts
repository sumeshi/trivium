import assert from "node:assert/strict";
import { test } from "node:test";
import { beginPageTiming } from "./renderPerformance.ts";
import type { PageTiming } from "./renderPerformance.ts";

test("page measurements are opt-in, bounded and distinguish commit from paint opportunity", async () => {
  assert.equal(beginPageTiming(0, 250), undefined);
  const originalWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  const originalFrame = Object.getOwnPropertyDescriptor(globalThis, "requestAnimationFrame");
  const sink = { pages: [] as PageTiming[] };
  const frames: FrameRequestCallback[] = [];
  Object.defineProperty(globalThis, "window", { configurable: true, value: { __TRIVIUM_PERF__: sink } });
  Object.defineProperty(globalThis, "requestAnimationFrame", { configurable: true, value: (callback: FrameRequestCallback) => frames.push(callback) });
  try {
    const page = beginPageTiming(500, 250)!;
    for (const stage of ["dispatched", "responded", "normalizeStarted", "normalized", "published"] as const) page.mark(stage);
    page.rows(250, 120);
    let current = true;
    page.present(() => current);
    await Promise.resolve(); await Promise.resolve();
    const sample = sink.pages[0];
    assert.equal(sample.rows, 250);
    assert.equal(sample.columns, 120);
    assert.ok(sample.flushed! >= sample.published!);
    assert.equal(sample.presented, undefined);
    frames.shift()!(performance.now());
    assert.equal(sample.presented, undefined);
    frames.shift()!(performance.now());
    assert.ok(sample.presented! >= sample.flushed!);
    const stale = beginPageTiming(750, 250)!;
    stale.present(() => current);
    await Promise.resolve(); await Promise.resolve();
    current = false;
    frames.shift()!(performance.now()); frames.shift()!(performance.now());
    assert.equal(sink.pages[1].presented, undefined);
    for (let offset = 0; offset < 500; offset += 1) beginPageTiming(offset * 250, 250);
    assert.ok(sink.pages.length <= 128);
    assert.equal(sink.pages.at(-1)?.offset, 499 * 250);
  } finally {
    if (originalWindow) Object.defineProperty(globalThis, "window", originalWindow);
    else Reflect.deleteProperty(globalThis, "window");
    if (originalFrame) Object.defineProperty(globalThis, "requestAnimationFrame", originalFrame);
    else Reflect.deleteProperty(globalThis, "requestAnimationFrame");
  }
});
