import assert from "node:assert/strict";
import { test } from "node:test";
import { recycleSlots } from "./renderSlots.ts";

test("scrolling reuses departing DOM slots and preserves every retained logical item", () => {
  const values = Array.from({ length: 1000 }, (_, id) => ({ id }));
  let window = recycleSlots(values.slice(0, 40), (value) => value.id);
  const allocated = new Set(window.map((entry) => entry.slot));
  for (let start = 1; start < 900; start += 1) {
    const previous = new Map(window.map((entry) => [entry.value.id, entry]));
    window = recycleSlots(values.slice(start, start + 40), (value) => value.id, window);
    assert.equal(new Set(window.map((entry) => entry.slot)).size, 40);
    for (const entry of window) {
      assert.ok(allocated.has(entry.slot), "Scrolling a fixed-size window must not allocate another DOM key");
      if (previous.has(entry.value.id)) assert.equal(entry, previous.get(entry.value.id));
    }
  }
});

test("recycling grows and shrinks with the viewport while retaining a focused item", () => {
  const values = Array.from({ length: 120 }, (_, id) => ({ id }));
  const initial = recycleSlots(values.slice(0, 7), (value) => value.id);
  const focused = initial[0];
  const moved = recycleSlots([values[0], ...values.slice(60, 69)], (value) => value.id, initial);
  assert.equal(moved[0], focused);
  assert.equal(new Set(moved.map((entry) => entry.slot)).size, 10);
  const smaller = recycleSlots([values[0], values[68]], (value) => value.id, moved);
  assert.equal(smaller[0], focused);
  assert.equal(smaller.length, 2);
  assert.deepEqual(smaller.map((entry) => entry.value.id), [0, 68]);
  assert.deepEqual(recycleSlots([], (value: { id: number }) => value.id, smaller), []);
});

test("annotation changes replace the row value without moving its DOM key", () => {
  const first = { id: 42, flag: "safe" };
  const initial = recycleSlots([first], (value) => value.id);
  assert.equal(recycleSlots([first], (value) => value.id, initial), initial);
  const updated = { ...first, flag: "critical" };
  const next = recycleSlots([updated], (value) => value.id, initial);
  assert.equal(next[0].slot, initial[0].slot);
  assert.equal(next[0].value, updated);
  assert.notEqual(next[0], initial[0]);
});
