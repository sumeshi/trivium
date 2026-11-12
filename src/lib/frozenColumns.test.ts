import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { frozenColumnLayout, frozenColumnViewport, horizontalScrollForColumn } from "./frozenColumns.ts";
import { savedViewFromUnknown, savedViewToViewState, viewStateToSavedView, viewStateWarnings } from "./viewStateMapping.ts";
import { createWorkspaceController } from "./workspaceController.ts";
import { createWorkspaceView } from "./workspaceView.ts";
import { copyWorkspaceSession } from "./workspaceSession.ts";
import { applyPresetView } from "./presetView.ts";
import type { Backend } from "./backend.ts";
import type { LoadProjectResponse, LoadViewStateResult, ViewState } from "./types.ts";

const columns = ["source", "host", "TimeCreated", "message", "extra"];
const widths = [90, 120, 210, 260, 140];
const annotations = 450;
const editor = () => ({ type: "group" as const, id: "root", kind: "and" as const, collapsed: false, children: [] });
const saved = () => ({ version: 1, search: "", searchColumns: [], flagFilter: "all" as const, bookmarkedOnly: false,
  condition: editor(), hiddenColumns: [], sortKey: null, sortDirection: "asc" as const,
  columnWidths: {}, logicalRow: 0 });

test("freezing TimeCreated freezes the cumulative visible source prefix with exact offsets", () => {
  const layout = frozenColumnLayout(columns, columns, widths, "TimeCreated", annotations);
  assert.deepEqual([...layout.offsets], [["source", 450], ["host", 540], ["TimeCreated", 660]]);
  assert.equal(layout.width, 870); assert.equal(layout.lastColumn, "TimeCreated");
  assert.equal(layout.offsets.has("message"), false);
  const wide = frozenColumnLayout(columns, columns, widths, "extra", annotations);
  assert.equal(wide.offsets.size, 5, "the user's boundary is not silently capped at the viewport");
});

test("hidden prefix columns and a hidden boundary keep source identity without wasting frozen width", () => {
  const firstHidden = frozenColumnLayout(columns, ["host", "TimeCreated", "message", "extra"], widths.slice(1), "TimeCreated", annotations);
  assert.deepEqual([...firstHidden.offsets], [["host", 450], ["TimeCreated", 570]]);
  const boundaryHidden = frozenColumnLayout(columns, ["source", "host", "message"], [90, 120, 260], "TimeCreated", annotations);
  assert.deepEqual([...boundaryHidden.offsets], [["source", 450], ["host", 540]]);
  assert.equal(boundaryHidden.boundary, "TimeCreated"); assert.equal(boundaryHidden.width, 660);
  const allPrefixHidden = frozenColumnLayout(columns, ["message", "extra"], [260, 140], "TimeCreated", annotations);
  assert.equal(allPrefixHidden.offsets.size, 0); assert.equal(allPrefixHidden.width, annotations);
});

test("missing legacy state freezes the first visible column while explicit unfreeze freezes no data", () => {
  const visible = ["host", "TimeCreated", "message"];
  const legacy = frozenColumnLayout(columns, visible, [120, 210, 260], undefined, annotations);
  assert.deepEqual([...legacy.offsets], [["host", annotations]]);
  const unfreeze = frozenColumnLayout(columns, visible, [120, 210, 260], "", annotations);
  assert.equal(unfreeze.width, annotations); assert.equal(unfreeze.offsets.size, 0);
  const empty = frozenColumnLayout(columns, [], [], undefined, annotations);
  assert.equal(empty.offsets.size, 0);
});

test("keyboard focus reveals nonfrozen columns beside the whole prefix and preserves frozen focus", () => {
  const layout = frozenColumnLayout(columns, columns, widths, "TimeCreated", annotations);
  assert.equal(horizontalScrollForColumn(1, columns, widths, layout, annotations, 200, 1280), 200);
  const scroll = horizontalScrollForColumn(4, columns, widths, layout, annotations, 0, 1280);
  const left = annotations + widths.slice(0, 4).reduce((sum, width) => sum + width, 0);
  assert.equal(scroll, 0, "the last column already fits this viewport");
  const constrained = horizontalScrollForColumn(4, columns, widths, layout, annotations, 0, 1100);
  assert.ok(left - constrained >= layout.width);
  assert.ok(left + widths[4] - constrained <= 1100);
  const back = horizontalScrollForColumn(3, columns, widths, layout, annotations, 250, 1280);
  assert.equal(back, 0, "a scrolling column hidden behind frozen cells returns to their right edge");
});

test("wire/view parsing preserves explicit unfreeze and hidden boundaries, defaults old files, and warns on unknown ids", () => {
  const old = viewStateToSavedView(saved(), columns)!;
  assert.equal(Object.hasOwn(old, "frozenThrough"), false);
  for (const boundary of ["", "TimeCreated"]) {
    const source = { ...saved(), hiddenColumns: ["TimeCreated"], frozenThrough: boundary };
    const restored = viewStateToSavedView(source, columns)!;
    assert.equal(restored.frozenThrough, boundary);
    assert.equal(savedViewToViewState(restored).frozenThrough, boundary);
  }
  const unknown = { ...saved(), frozenThrough: "removed-column" };
  assert.equal(viewStateToSavedView(unknown, columns)?.frozenThrough, undefined);
  assert.deepEqual(viewStateWarnings(unknown, columns), ["frozen column removed-column"]);
  assert.equal(savedViewFromUnknown({ ...saved(), frozenThrough: 100 }, columns)?.frozenThrough, undefined);
});

test("per-case pin state survives save/reload and tab copies without changing filters or presets", async () => {
  const writes: ViewState[] = [];
  const backend = { saveViewState: async ({ state }: { state: ViewState }) => { writes.push(structuredClone(state)); },
    loadViewState: async (): Promise<LoadViewStateResult> => ({ status: "ok", state: writes.at(-1)! }) } as unknown as Backend;
  const detail: LoadProjectResponse = {
    project: { meta: { id: "pin-case", name: "pin-case", created_at: "2026-11-12", total_records: 100,
      flagged_records: 0, ioc_applied_records: 0, hidden_columns: [] } },
    columns, columns_meta: [], hidden_columns: [], column_max_chars: {}, column_kinds: {},
    iocs: [], color_rules: [], initial_rows: [],
  };
  const workspace = createWorkspaceController();
  workspace.currentProjectId.set("pin-case"); workspace.projectDetail.set(detail);
  workspace.search.set("keep query"); workspace.searchColumns.set(["message"]);
  workspace.frozenThrough.set("TimeCreated"); workspace.hiddenColumns.set(new Set(["host"]));
  const createView = (owner: ReturnType<typeof createWorkspaceController>) => createWorkspaceView(owner,
    { backend, isReady: () => true, columnWidths: () => new Map(), logicalRow: () => 0,
      scrollLeft: () => 200, signature: () => "same-query", notify: () => {} });
  const view = createView(workspace);
  await view.flushPersist("pin-case");
  const snapshot = copyWorkspaceSession(view.snapshotSession()!);
  assert.equal(snapshot.view.frozenThrough, "TimeCreated");
  workspace.frozenThrough.set("");
  assert.equal(snapshot.view.frozenThrough, "TimeCreated", "inactive snapshots are immutable");
  await view.flushPersist("pin-case");
  const replacement = createWorkspaceController(); replacement.currentProjectId.set("pin-case");
  replacement.projectDetail.set(detail);
  const reopened = await createView(replacement).loadSavedView(detail, null, () => true);
  assert.equal(reopened?.frozenThrough, "");
  const restoredTab = await view.loadSavedView(detail, snapshot, () => true);
  assert.equal(restoredTab?.frozenThrough, "TimeCreated");
  workspace.frozenThrough.set("TimeCreated");
  applyPresetView(workspace, { condition: null, hiddenColumns: [], sortKey: null, sortDirection: null, unknown_columns: [] });
  assert.equal(get(workspace.frozenThrough), "TimeCreated", "old presets preserve this new case preference");
  assert.equal(get(workspace.search), "keep query"); assert.deepEqual(get(workspace.searchColumns), ["message"]);
  workspace.dispose(); replacement.dispose();
});


test("oversized prefixes pan beneath fixed annotations and stop once a scrolling reserve fits", () => {
  const visible = ["a", "b", "c", "timestamp", "next"];
  const wideWidths = [450, 450, 450, 450, 170];
  const layout = frozenColumnLayout(visible, visible, wideWidths, "timestamp", annotations);
  const start = frozenColumnViewport(layout, visible.length, annotations, 1280, 0);
  assert.deepEqual({ pan: start.pan, maxPan: start.maxPan, edge: start.edge, reserve: start.reserve, overflow: start.overflow },
    { pan: 0, maxPan: 1050, edge: 2250, reserve: 80, overflow: true });
  const midway = frozenColumnViewport(layout, visible.length, annotations, 1280, 400);
  assert.equal(midway.pan, 400);
  const settled = frozenColumnViewport(layout, visible.length, annotations, 1280, 1140);
  assert.equal(settled.pan, 1050); assert.equal(settled.edge, 1200);
  assert.equal(layout.offsets.get("timestamp")! - settled.pan, 750);
  assert.equal(layout.offsets.size, 4, "panning never changes the requested frozen prefix");
});

test("keyboard focus reaches every oversized frozen column and the first scrolling header in full", () => {
  const visible = ["a", "b", "c", "timestamp", "next"];
  const wideWidths = [450, 450, 450, 450, 170];
  const layout = frozenColumnLayout(visible, visible, wideWidths, "timestamp", annotations);
  let scroll = 1140;
  for (let index = 0; index < visible.length; index += 1) {
    scroll = horizontalScrollForColumn(index, visible, wideWidths, layout, annotations, scroll, 1280);
    const frozen = layout.offsets.has(visible[index]);
    const geometry = frozenColumnViewport(layout, visible.length, annotations, 1280, scroll,
      frozen ? undefined : wideWidths[index]);
    const sourceLeft = annotations + wideWidths.slice(0, index).reduce((sum, width) => sum + width, 0);
    const left = sourceLeft - (frozen ? geometry.pan : scroll);
    assert.ok(left >= annotations, `column ${index} must be beside the fixed annotations`);
    assert.ok(left + wideWidths[index] <= 1280, `column ${index} must be fully inside the viewport`);
    if (!frozen) {
      assert.ok(left >= geometry.edge);
      assert.equal(geometry.reserve, 170, "the focused scrolling column gets enough space");
    }
  }
  scroll = horizontalScrollForColumn(0, visible, wideWidths, layout, annotations, scroll, 1280);
  const restored = frozenColumnViewport(layout, visible.length, annotations, 1280, scroll);
  assert.equal(restored.reserve, 80); assert.equal(scroll, 0);
});

test("all-frozen prefixes still pan to their end and resizing restores ordinary fixed geometry", () => {
  const visible = ["a", "b", "c", "timestamp"];
  const wideWidths = [450, 450, 450, 450];
  const layout = frozenColumnLayout(visible, visible, wideWidths, "timestamp", annotations);
  const end = horizontalScrollForColumn(3, visible, wideWidths, layout, annotations, 0, 1280);
  const small = frozenColumnViewport(layout, visible.length, annotations, 1280, end);
  assert.equal(small.reserve, 0); assert.equal(small.pan, 970); assert.equal(small.edge, 1280);
  assert.equal(layout.offsets.get("timestamp")! - small.pan + wideWidths[3], 1280);
  const larger = frozenColumnViewport(layout, visible.length, annotations, 2600, end);
  assert.equal(larger.overflow, false); assert.equal(larger.pan, 0);
  assert.equal(layout.offsets.get("timestamp"), 1800);
  assert.equal(layout.boundary, "timestamp");
  const ordinary = frozenColumnLayout(columns, columns, widths, "TimeCreated", annotations);
  assert.equal(frozenColumnViewport(ordinary, columns.length, annotations, 1280, 200, 900).pan, 0,
    "focus reserves cannot move an ordinary fitting frozen prefix");
});
