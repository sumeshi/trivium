import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { applyPresetView, changedPresetLayout } from "./presetView.ts";
import { createWorkspaceController } from "./workspaceController.ts";

function setup() {
  const workspace = createWorkspaceController();
  workspace.search.set("keep current search"); workspace.flagFilter.set("critical");
  workspace.bookmarkedOnly.set(true); workspace.filterByValue("c1", "old rule");
  workspace.hiddenColumns.set(new Set(["old hidden"]));
  workspace.sortKey.set("old sort"); workspace.sortDirection.set("desc");
  return workspace;
}

test("a preset restores only documented rules/columns/sort and emits a per-case sort revision", () => {
  const a = setup(), b = setup();
  const condition = { column: "c2", op: "eq" as const, values: ["exact,value"], negated: false };
  const preset = { condition, hiddenColumns: ["c3"], sortKey: "c2", sortDirection: "asc" as const, unknown_columns: [] };
  applyPresetView(a, preset);
  assert.deepEqual(get(a.conditionPayload), condition);
  assert.deepEqual([...get(a.hiddenColumns)], ["c3"]);
  assert.equal(get(a.sortKey), "c2"); assert.equal(get(a.sortDirection), "asc");
  assert.equal(get(a.sortRevision), 1); assert.equal(get(b.sortRevision), 0);
  assert.equal(get(a.search), "keep current search"); assert.equal(get(a.flagFilter), "critical");
  assert.equal(get(a.bookmarkedOnly), true);
  applyPresetView(a, preset);
  assert.equal(get(a.sortRevision), 1, "an unchanged sort does not force a duplicate query");
  a.dispose(); b.dispose();
});

test("loading an empty-rule preset clears old rules and restores source-order sorting", () => {
  const workspace = setup();
  applyPresetView(workspace, { condition: null, hiddenColumns: [], sortKey: null, sortDirection: null, unknown_columns: [] });
  assert.equal(get(workspace.conditionPayload), null);
  assert.equal(get(workspace.conditionTree).children.length, 0);
  assert.deepEqual([...get(workspace.hiddenColumns)], []);
  assert.equal(get(workspace.sortKey), null); assert.equal(get(workspace.sortDirection), "asc");
  assert.equal(get(workspace.search), "keep current search");
  assert.equal(get(workspace.flagFilter), "critical");
  workspace.dispose();
});

test("preset saving includes only changed known column layout with owned snapshots", () => {
  const columns = ["c1", "c2", "c3"];
  const baseline = new Map([["c1", 100], ["c2", 200], ["c3", 300]]);
  assert.deepEqual(changedPresetLayout(columns, [], baseline, baseline), {});
  const order = ["c3", "c1", "c1", "removed"];
  const widths = new Map([["c1", 100], ["c2", 347], ["removed", 600], ["c3", NaN]]);
  const saved = changedPresetLayout(columns, order, widths, baseline);
  assert.deepEqual(saved.columnOrder, ["c3", "c1", "c2"]);
  assert.deepEqual(Object.entries(saved.columnWidths!), [["c2", 347]]);
  widths.set("c2", 600); order.reverse();
  assert.deepEqual(saved.columnOrder, ["c3", "c1", "c2"]);
  assert.equal(saved.columnWidths!.c2, 347);
  const unusual = changedPresetLayout(["__proto__"], [], new Map([["__proto__", 347]]), new Map([["__proto__", 100]]));
  assert.equal(Object.getOwnPropertyDescriptor(unusual.columnWidths, "__proto__")?.value, 347);
});

test("presets restore only present column order fields and can explicitly reset order", () => {
  const workspace = setup(), other = setup();
  workspace.columnOrder.set(["c3", "c1", "c2"]);
  other.columnOrder.set(["other-order"]);
  const preset = { condition: null, hiddenColumns: ["c3"], sortKey: null, sortDirection: null, unknown_columns: [] };
  applyPresetView(workspace, preset);
  assert.deepEqual(get(workspace.columnOrder), ["c3", "c1", "c2"], "older/omitted layout retains the current order");
  const order = ["c2", "c3", "c1"];
  applyPresetView(workspace, { ...preset, columnOrder: order });
  order.reverse();
  assert.deepEqual(get(workspace.columnOrder), ["c2", "c3", "c1"]);
  assert.deepEqual(get(other.columnOrder), ["other-order"]);
  applyPresetView(workspace, { ...preset, columnOrder: [] });
  assert.deepEqual(get(workspace.columnOrder), []);
  assert.equal(get(workspace.search), "keep current search");
  assert.equal(get(workspace.flagFilter), "critical");
  workspace.dispose(); other.dispose();
});
