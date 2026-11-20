import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { applyPresetAndSaveColumns, applyPresetView, changedPresetLayout } from "./presetView.ts";
import { createWorkspaceController } from "./workspaceController.ts";
import { queueHiddenColumnWrite } from "./hiddenColumnWrites.ts";

function setup() {
  const workspace = createWorkspaceController();
  workspace.search.set("keep current search"); workspace.flagFilter.set("critical");
  workspace.bookmarkedOnly.set(true); workspace.filterByValue("c1", "old rule");
  workspace.hiddenColumns.set(new Set(["old hidden"]));
  workspace.sortKey.set("old sort"); workspace.sortDirection.set("desc");
  return workspace;
}

test("a legacy preset keeps omitted filters and emits a per-case sort revision", () => {
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

test("a filter snapshot restores search scope, flags and bookmarks, including explicit resets", () => {
  const workspace = setup(), other = setup();
  const preset = { condition: null, hiddenColumns: [], sortKey: null, sortDirection: null,
    filters: { search: '"日本語, quoted" -noise', searchColumns: ["c2"],
      flagFilter: "safe|critical" as const, bookmarkedOnly: true }, unknown_columns: [] };
  applyPresetView(workspace, preset);
  preset.filters.searchColumns.push("later mutation");
  assert.equal(get(workspace.search), '"日本語, quoted" -noise');
  assert.deepEqual(get(workspace.searchColumns), ["c2"]);
  assert.equal(get(workspace.flagFilter), "safe|critical");
  assert.equal(get(workspace.bookmarkedOnly), true);
  assert.equal(get(other.search), "keep current search", "a preset only updates its own case");
  applyPresetView(workspace, { ...preset, filters: {
    search: "", searchColumns: [], flagFilter: "all", bookmarkedOnly: false } });
  assert.equal(get(workspace.search), "");
  assert.deepEqual(get(workspace.searchColumns), []);
  assert.equal(get(workspace.flagFilter), "all");
  assert.equal(get(workspace.bookmarkedOnly), false);
  workspace.dispose(); other.dispose();
});

test("a slow preset metadata save cannot overwrite subsequent investigation and column edits", async () => {
  const workspace = setup();
  const preset = { condition: { column: "c2", op: "in" as const, values: ["alice,bob", " carol "], negated: false },
    hiddenColumns: ["c1"], sortKey: "c2", sortDirection: "asc" as const,
    filters: { search: "beacon", searchColumns: ["c2"], flagFilter: "safe" as const, bookmarkedOnly: true },
    columnOrder: ["c2", "c1"], columnWidths: { c2: 347 }, unknown_columns: [] };
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  let widths: Record<string, number> = {}, persisted: string[] = [];
  const writes: string[][] = [];
  const pending = applyPresetAndSaveColumns(workspace, preset, {
    setColumnWidths: next => { widths = next; },
    saveHiddenColumns: columns => {
      assert.deepEqual(get(workspace.conditionPayload), preset.condition);
      assert.deepEqual([...get(workspace.hiddenColumns)], ["c1"]);
      assert.deepEqual(get(workspace.columnOrder), ["c2", "c1"]);
      assert.equal(get(workspace.search), "beacon");
      assert.deepEqual(get(workspace.searchColumns), ["c2"]);
      assert.equal(get(workspace.flagFilter), "safe");
      assert.equal(get(workspace.bookmarkedOnly), true);
      assert.deepEqual(widths, { c2: 347 }, "all view fields must publish before metadata is queued");
      return queueHiddenColumnWrite("preset-latest-edit", columns, async snapshot => {
        writes.push(snapshot); await held; persisted = snapshot;
      });
    },
  });
  workspace.filterByValue("c1", "later rule");
  workspace.hiddenColumns.set(new Set(["c2"]));
  workspace.sortKey.set("c1"); workspace.sortDirection.set("desc");
  workspace.columnOrder.set(["c1", "c2"]); widths = { c1: 400 };
  workspace.search.set("later search"); workspace.searchColumns.set(["c1"]);
  workspace.flagFilter.set("critical"); workspace.bookmarkedOnly.set(false);
  const latest = queueHiddenColumnWrite("preset-latest-edit", ["c2"], async snapshot => {
    writes.push(snapshot); persisted = snapshot;
  });
  release();
  await Promise.all([pending, latest]);
  assert.deepEqual(writes, [["c1"], ["c2"]]);
  assert.deepEqual([...get(workspace.hiddenColumns)], persisted);
  assert.deepEqual(persisted, ["c2"]);
  assert.deepEqual(get(workspace.conditionPayload), { kind: "and", children: [preset.condition,
    { column: "c1", op: "in", values: ["later rule"], negated: false }] });
  assert.equal(get(workspace.sortKey), "c1"); assert.equal(get(workspace.sortDirection), "desc");
  assert.deepEqual(get(workspace.columnOrder), ["c1", "c2"]);
  assert.deepEqual(widths, { c1: 400 });
  assert.equal(get(workspace.search), "later search");
  assert.deepEqual(get(workspace.searchColumns), ["c1"]);
  assert.equal(get(workspace.flagFilter), "critical");
  assert.equal(get(workspace.bookmarkedOnly), false);
  workspace.dispose();
});

test("preset application is complete before a panel can close and failure never rolls back newer edits", async () => {
  const workspace = setup();
  let reject!: (error: Error) => void;
  const held = new Promise<void>((_, fail) => { reject = fail; });
  const preset = { condition: null, hiddenColumns: ["c3"], sortKey: null,
    sortDirection: null, columnWidths: { c1: 160 }, unknown_columns: [] };
  let callbacks = 0;
  const pending = applyPresetAndSaveColumns(workspace, preset, {
    setColumnWidths: () => { callbacks += 1; },
    saveHiddenColumns: () => held,
  });
  assert.equal(get(workspace.conditionPayload), null);
  assert.deepEqual([...get(workspace.hiddenColumns)], ["c3"]);
  assert.equal(get(workspace.sortKey), null);
  assert.equal(callbacks, 1, "closing the panel after this call cannot leave only metadata applied");
  workspace.hiddenColumns.set(new Set(["later-column"]));
  workspace.filterByValue("c2", "later rule");
  const failed = assert.rejects(pending, /disk unavailable/);
  reject(new Error("disk unavailable"));
  await failed;
  assert.deepEqual([...get(workspace.hiddenColumns)], ["later-column"]);
  assert.equal(get(workspace.conditionTree).children[0].type, "condition");
  assert.equal(callbacks, 1, "neither success nor failure may publish the old layout again");
  workspace.dispose();
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
