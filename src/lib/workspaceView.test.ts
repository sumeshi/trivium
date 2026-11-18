import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import type { Backend } from "./backend.ts";
import type { LoadProjectResponse, LoadViewStateResult, ViewState } from "./types.ts";
import { createWorkspaceController } from "./workspaceController.ts";
import { createWorkspaceView } from "./workspaceView.ts";
import { normalizeRow } from "./components/project_view/state.ts";
import { flushAllSaves } from "./saveDrain.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => { resolve = yes; });
  return { promise, resolve };
}
function detail(id: string): LoadProjectResponse {
  return { project: { meta: { id, name: id, created_at: "2026", total_records: 5199, flagged_records: 0,
    ioc_applied_records: 0, hidden_columns: [] } }, columns: ["c1"], columns_meta: [], column_max_chars: {},
    column_kinds: {}, color_rules: [], iocs: [], hidden_columns: [], initial_rows: [] };
}
function saved(search = "restored"): ViewState {
  return { version: 1, search, searchColumns: ["c1"], flagFilter: "critical", bookmarkedOnly: true,
    condition: { type: "group", id: "root", kind: "and", collapsed: false, children: [] },
    sortKey: "c1", sortDirection: "desc", hiddenColumns: [], columnWidths: { c1: 155 }, logicalRow: 2999 };
}
function setup(api: Partial<Backend> = {}, id = "view-test") {
  const writes: ViewState[] = [], notices: string[] = [];
  const backend = { loadViewState: async () => ({ state: null, status: "missing" }),
    saveViewState: async ({ state }: { state: ViewState }) => { writes.push(state); }, ...api } as Backend;
  const workspace = createWorkspaceController();
  workspace.currentProjectId.set(id);
  workspace.projectDetail.set(detail(id));
  workspace.setHistoryProject(id);
  const view = createWorkspaceView(workspace, { backend, isReady: () => true,
    columnWidths: () => new Map([["c1", 155]]), logicalRow: () => 2999, scrollLeft: () => 200,
    signature: () => "applied scope", notify: (message) => notices.push(message) });
  return { workspace, view, writes, notices, backend };
}

test("workspace snapshots capture owned controls/history/IDs and flush a stable state before disposal", async () => {
  const { workspace, view, writes } = setup();
  workspace.search.set("query");
  workspace.searchColumns.set(["c1"]);
  workspace.flagFilter.set("critical");
  workspace.sortKey.set("c1");
  workspace.sortDirection.set("desc");
  workspace.filterByValue("c1", "filter");
  workspace.recordAppliedCondition(4, get(workspace.conditionTree), 2999);
  workspace.selectedRowIds.set(new Set([2, 4567]));
  workspace.columnOrder.set(['c1']);
  workspace.columnFormats.set({ c1: 'iso' });
  workspace.registerLoadedPage(0, 0, [normalizeRow({ row_index: 2,
    cells: { c1: { kind: "string", text: "never retain this row" } }, flag: "", memo: "", tags: [], bookmarked: false })]);
  const snapshot = view.snapshotSession()!;
  const saving = view.flushPersist("view-test");
  workspace.dispose();
  await saving;
  assert.equal(snapshot.view.search, "query");
  assert.equal(snapshot.view.logicalRow, 2999);
  assert.equal(snapshot.view.sortDirection, "desc");
  assert.deepEqual(snapshot.view.columnOrder, ['c1']);
  assert.deepEqual(snapshot.view.columnFormats, { c1: 'iso' });
  assert.deepEqual(snapshot.selectedRowIds, [2, 4567]);
  assert.equal(snapshot.history.index, 1);
  assert.equal(snapshot.scrollLeft, 200);
  assert.equal(JSON.stringify(snapshot).includes("never retain this row"), false);
  assert.equal(writes[0].search, "query");
  assert.equal((writes[0].condition!.children as unknown[]).length, 1);
  assert.equal(view.snapshotSession(), null);
});

test("session restores bypass disk while preserving read-only view protection", async () => {
  let loads = 0;
  const { workspace, view, writes } = setup({ loadViewState: async () => { loads += 1; throw new Error("must not load"); } });
  const snapshot = view.snapshotSession()!;
  snapshot.view = saved("tab state");
  snapshot.writable = false;
  const result = await view.loadSavedView(detail("view-test"), snapshot, () => true);
  assert.equal(result?.search, "tab state");
  assert.equal(result?.logicalRow, 2999);
  assert.equal(loads, 0);
  await view.flushPersist("view-test");
  assert.deepEqual(writes, []);
  assert.equal(view.snapshotSession()?.writable, false);
  workspace.dispose();
});

test("unreadable, invalid or newer saved views are never replaced by default settings", async () => {
  for (const result of [
    { status: "unreadable", state: null },
    { status: "newer", state: saved() },
    { status: "ok", state: { ...saved(), condition: { bad: true } } },
  ] satisfies LoadViewStateResult[]) {
    const { workspace, view, writes, notices } = setup({ loadViewState: async () => result });
    await view.loadSavedView(detail("view-test"), null, () => true);
    await view.flushPersist("view-test");
    assert.deepEqual(writes, []);
    assert.equal(view.snapshotSession()?.writable, false);
    assert.match(notices.join(" "), /kept/);
    workspace.dispose();
  }
});

test("unsupported saved settings survive idle saves, close flushes and tab restoration", async t => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const unsupported: Partial<ViewState>[] = [
    { searchColumns: ["removed"] },
    { flagFilter: "future-value" as never },
    { sortKey: "removed" },
    { sortDirection: "natural" as never },
    { frozenThrough: "removed" },
    { hiddenColumns: ["removed"] },
    { columnOrder: ["removed", "c1"] },
    { columnFormats: { c1: "future-format" } },
    { columnWidths: { removed: 200 } },
    { columnWidths: { c1: -10 } },
    { noteColumnWidth: -10 },
  ];
  for (const [index, fields] of unsupported.entries()) {
    const id = `unsupported-view-${index}`, stored = { ...saved(), ...fields };
    const original = JSON.stringify(stored);
    const state = setup({ loadViewState: async () => ({ state: stored, status: "ok" }) }, id);
    state.view.watch();
    const restored = await state.view.loadSavedView(detail(id), null, () => true);
    assert.ok(restored, "safe fields can still be used to open the investigation");
    state.workspace.search.set("a temporary session query");
    state.view.schedulePersist();
    t.mock.timers.tick(1000);
    await Promise.resolve(); await Promise.resolve();
    await state.view.flushPersist(id, false, true);
    assert.deepEqual(state.writes, [], "neither autosave nor the final close flush may replace unsupported data");
    assert.equal(JSON.stringify(stored), original);
    assert.match(state.notices.join(" "), /without overwriting the saved view/);
    const snapshot = state.view.snapshotSession()!;
    assert.equal(snapshot.writable, false);
    state.workspace.dispose();

    const next = setup({ loadViewState: async () => assert.fail("a retained tab uses its session") }, id);
    await next.view.loadSavedView(detail(id), snapshot, () => true);
    await next.view.flushPersist(id);
    assert.equal(next.view.snapshotSession()?.writable, false, "a sanitized tab snapshot must retain the original file's protection");
    assert.deepEqual(next.writes, []);
    next.workspace.dispose();
  }
});

test("protecting unsupported view settings still allows annotations to be saved", async () => {
  const annotations: string[] = [];
  const id = "unsupported-view-with-annotations";
  const state = setup({
    loadViewState: async () => ({ state: { ...saved(), flagFilter: "future-value" as never }, status: "ok" }),
    updateFlag: async args => {
      annotations.push(String(args.memo));
      return { entry: { flag: "", memo: args.memo, tags: [], bookmarked: false }, revision: 1, flaggedRecords: 0 };
    },
  }, id);
  state.workspace.backend.set(state.backend);
  await state.view.loadSavedView(detail(id), null, () => true);
  const row = normalizeRow({ row_index: 2, cells: { c1: { kind: "string", text: "evidence" } },
    flag: "", memo: "", tags: [], bookmarked: false });
  state.workspace.registerLoadedPage(0, 0, [row]);
  assert.equal(await state.workspace.saveAnnotation(id, row, { memo: "finding" }), true);
  await state.view.flushPersist(id);
  assert.deepEqual(annotations, ["finding"]);
  assert.deepEqual(state.writes, []);
  state.workspace.dispose();
});

test("a late view restore cannot revive a disposed case or publish warnings into its replacement", async () => {
  const gate = deferred<LoadViewStateResult>();
  const a = setup({ loadViewState: () => gate.promise }, "old-view");
  const restoring = a.view.loadSavedView(detail("old-view"), null, () => true);
  await Promise.resolve(); await Promise.resolve();
  a.workspace.dispose();
  const b = setup({}, "new-view");
  gate.resolve({ state: saved("old"), status: "newer" });
  assert.equal(await restoring, undefined);
  assert.deepEqual(a.notices, []);
  assert.deepEqual(b.notices, []);
  assert.equal(b.view.snapshotSession()?.projectId, "new-view");
  assert.equal(b.view.snapshotSession()?.view.search, "");
  b.workspace.dispose();
});

test("scroll and control changes save one latest view after idle and unsubscribe on disposal", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { workspace, view, writes } = setup();
  view.watch();
  view.watch();
  workspace.scrollTop.set(32);
  workspace.search.set("first");
  t.mock.timers.tick(200);
  assert.equal(writes.length, 0);
  workspace.scrollTop.set(160);
  workspace.search.set("latest");
  t.mock.timers.tick(249);
  assert.equal(writes.length, 0);
  t.mock.timers.tick(1);
  await Promise.resolve(); await Promise.resolve();
  assert.equal(writes.length, 1);
  assert.equal(writes[0].search, "latest");
  workspace.scrollLeft.set(400);
  workspace.dispose();
  t.mock.timers.tick(1000);
  await Promise.resolve(); await Promise.resolve();
  assert.equal(writes.length, 1);
});

test("tab teardown preserves the last live viewport ahead of its published stores", async () => {
  const workspace = createWorkspaceController(), writes: ViewState[] = [];
  workspace.currentProjectId.set("detach");
  workspace.projectDetail.set(detail("detach"));
  workspace.totalFilteredRows.set(5199);
  workspace.scrollTop.set(32);
  workspace.scrollLeft.set(10);
  const body = { isConnected: true, scrollTop: 320, scrollLeft: 200 };
  workspace.bodyScrollEl.set(body as HTMLDivElement);
  let signature = "first";
  const view = createWorkspaceView(workspace, {
    backend: { saveViewState: async ({ state }: { state: ViewState }) => { writes.push(state); } } as unknown as Backend,
    isReady: () => true, columnWidths: () => new Map(), signature: () => signature,
    logicalRow: () => Math.floor((body.isConnected ? body.scrollTop : get(workspace.scrollTop)) / 32),
    scrollLeft: () => body.isConnected ? body.scrollLeft : get(workspace.scrollLeft), notify: () => {},
  });
  view.watch();
  const live = view.snapshotSession()!;
  assert.equal(live.view.logicalRow, 10);
  assert.equal(live.scrollLeft, 200);
  body.isConnected = false;
  body.scrollTop = body.scrollLeft = 0;
  workspace.bodyScrollEl.set(null);
  const detached = view.snapshotSession()!;
  assert.equal(detached.view.logicalRow, 10);
  assert.equal(detached.scrollLeft, 200);
  assert.equal(detached.history.entries[detached.history.index].offset, 10);
  await view.flushPersist("detach");
  assert.equal(writes[0].logicalRow, 10, "the teardown save must agree with the retained tab");
  body.isConnected = true;
  body.scrollTop = 640;
  body.scrollLeft = 350;
  workspace.bodyScrollEl.set(body as HTMLDivElement);
  workspace.scrollTop.set(640);
  workspace.scrollLeft.set(350);
  body.isConnected = false;
  body.scrollTop = body.scrollLeft = 0;
  workspace.bodyScrollEl.set(null);
  const afterImport = view.snapshotSession()!;
  assert.equal(afterImport.view.logicalRow, 20, "scrolling after an earlier import capture must use its newer published position");
  assert.equal(afterImport.scrollLeft, 350);
  await view.flushPersist("detach");
  assert.equal(writes[1].logicalRow, 20);
  signature = "different query";
  workspace.scrollTop.set(0);
  assert.equal(view.snapshotSession()?.view.logicalRow, 0, "a new result scope must not reuse the previous viewport");
  workspace.dispose();
});

test("pausing closes the debounce window while a strict close flush saves the latest view", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { workspace, view, writes } = setup({}, "close-paused-view");
  view.watch();
  workspace.search.set("queued before close");
  view.suspendPersistence(true);
  workspace.search.set("latest accepted state");
  t.mock.timers.tick(1000);
  await Promise.resolve(); await Promise.resolve();
  assert.equal(writes.length, 0);
  await view.flushPersist("close-paused-view", false, true);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].search, "latest accepted state");
  workspace.search.set("after keep open");
  view.suspendPersistence(false);
  view.schedulePersist();
  t.mock.timers.tick(250);
  await Promise.resolve(); await Promise.resolve();
  assert.equal(writes.length, 2);
  assert.equal(writes[1].search, "after keep open");
  workspace.dispose();
});

test("a strict close flush exposes storage failure and retries a newly captured view", async () => {
  let fail = true;
  const writes: ViewState[] = [];
  const { workspace, view, notices } = setup({ saveViewState: async ({ state }) => {
    if (fail) throw new Error("view storage failed");
    writes.push(state);
  } }, "strict-close-view");
  workspace.search.set("unsaved");
  await assert.rejects(view.flushPersist("strict-close-view", false, true), /view storage failed/);
  await flushAllSaves().catch(() => {});
  assert.deepEqual(notices, [], "the close coordinator owns the persistent failure dialog");
  fail = false;
  workspace.search.set("reviewed latest");
  await view.flushPersist("strict-close-view", false, true);
  assert.deepEqual(writes.map(state => state.search), ["reviewed latest"]);
  workspace.dispose();
});
