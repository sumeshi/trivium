import assert from "node:assert/strict";
import { test } from "node:test";
import { tableColumnLayout, visibleTableColumns } from "./columnWindow.ts";
import { frozenColumnLayout, frozenColumnViewport } from "./frozenColumns.ts";

const annotations = 450;

test("120-column windows preserve every uncovered cell and bound offscreen DOM at both ends", () => {
  const columns = Array.from({ length: 120 }, (_, index) => `c${index}`);
  const widths = columns.map(() => 100);
  const frozen = frozenColumnLayout(columns, columns, widths, "c0", annotations);
  const layout = tableColumnLayout(columns, widths, frozen.offsets, annotations);
  for (const left of [0, 51, 99, 200, 5000, 11000, 11450]) {
    const rendered = visibleTableColumns(layout, left, 1000, frozen.width);
    assert.ok(rendered.length <= 11, `Only fixed, intersecting and nearby columns are mounted at ${left}`);
    assert.equal(rendered[0].id, "c0");
    const visible = layout.filter((column) => !column.frozen && column.left + column.width > left + frozen.width && column.left < left + 1000);
    for (const column of visible) assert.ok(rendered.includes(column), `Column ${column.id} cannot leave a viewport hole`);
    assert.deepEqual(rendered.map(({ index }) => index), [...rendered.map(({ index }) => index)].sort((a, b) => a - b));
    for (const column of rendered) assert.equal(column.left, annotations + column.index * 100);
  }
});

test("small movement reuses a row's column props until its mounted window changes", () => {
  const columns = Array.from({ length: 120 }, (_, index) => `c${index}`);
  const layout = tableColumnLayout(columns, columns.map(() => 100), new Map(), annotations);
  const first = visibleTableColumns(layout, 500, 1000, annotations);
  assert.equal(visibleTableColumns(layout, 510, 1000, annotations, first), first);
  const next = visibleTableColumns(layout, 601, 1000, annotations, first);
  assert.notEqual(next, first);
  assert.ok(next.every((column) => layout[column.index] === column));
});

test("arbitrary frozen prefixes, hidden widths and oversized panning share the same source indexes", () => {
  const columns = ["source", "hidden", "timestamp", "message", "last"];
  const visible = ["source", "timestamp", "message", "last"];
  const widths = [450, 450, 450, 170];
  const frozen = frozenColumnLayout(columns, visible, widths, "message", annotations);
  const layout = tableColumnLayout(visible, widths, frozen.offsets, annotations);
  const viewport = frozenColumnViewport(frozen, visible.length, annotations, 1000, 1000);
  const rendered = visibleTableColumns(layout, 1000, 1000, viewport.edge);
  assert.deepEqual(rendered.map(({ id }) => id), visible);
  assert.deepEqual(layout.map(({ left }) => left), [450, 900, 1350, 1800]);
  assert.deepEqual(rendered.filter(({ frozen }) => frozen).map(({ id }) => id), ["source", "timestamp", "message"]);
});

test("empty views and narrower-than-annotation viewports cannot fabricate or duplicate columns", () => {
  assert.deepEqual(visibleTableColumns([], 0, 0, annotations), []);
  const layout = tableColumnLayout(["a", "b", "c"], [80, 80, 80], new Map(), annotations);
  const rendered = visibleTableColumns(layout, 0, 300, annotations);
  assert.equal(new Set(rendered.map(({ id }) => id)).size, rendered.length);
  assert.ok(rendered.length <= 2);
});

test("one focused offscreen column retains its identity without widening the virtual window", () => {
  const columns = Array.from({ length: 120 }, (_, index) => `c${index}`);
  const layout = tableColumnLayout(columns, columns.map(() => 100), new Map(), annotations);
  const normal = visibleTableColumns(layout, 6000, 1000, annotations);
  const focused = visibleTableColumns(layout, 6000, 1000, annotations, normal, 2, "c2");
  assert.equal(focused.length, normal.length + 1);
  assert.ok(focused.includes(layout[2]));
  for (const column of normal) assert.ok(focused.includes(column));
  assert.equal(visibleTableColumns(layout, 6010, 1000, annotations, focused, 2, "c2"), focused);
  assert.equal(visibleTableColumns(layout, 6000, 1000, annotations, normal, 2, "hidden-column"), normal);
});
