import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import type { Backend, QueryProjectRowsArgs, QueryProjectRowsResponse, UpdateFlagResult } from "./backend.ts";
import type { LoadProjectResponse, ProjectRow } from "./types.ts";
import type { AppliedFilters } from "./components/project_view/state.ts";
import { normalizeRow, PAGE_SIZE } from "./components/project_view/state.ts";
import { createWorkspaceController } from "./workspaceController.ts";
import { createWorkspaceQuery } from "./workspaceQuery.ts";
import { createWorkspaceNavigation } from "./workspaceNavigation.ts";
import { createOwnedRequests } from "./workspaceRequests.ts";
import { createWorkspaceAnnotationObserver } from "./workspaceAnnotations.ts";
import { markBulkAnnotationsChanged, forgetAnnotationVersion } from "./annotationVersions.ts";
import { virtualWindow } from "./virtualScroll.ts";
import { createWorkspaceView } from "./workspaceView.ts";
import { effectiveSearchScopeKey } from "./searchScope.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const filters = (search = ""): AppliedFilters => ({ search, flag: "all", bookmarkedOnly: false, searchColumns: [], condition: null });
function detail(id: string, total = 5199): LoadProjectResponse {
  return {
    project: { meta: { id, name: id, created_at: "2026-11-11", total_records: total, flagged_records: 0,
      ioc_applied_records: 0, hidden_columns: [] } },
    columns: ["c1"], columns_meta: [], column_max_chars: {}, column_kinds: {},
    color_rules: [], iocs: [], hidden_columns: [], initial_rows: [],
  };
}
function row(id: number, text = String(id)): ProjectRow {
  return { row_index: id, cells: { c1: { kind: "string", text } }, flag: "", memo: "", tags: [], bookmarked: false };
}
function response(offset = 0, rows = [row(offset)], total = 5199): QueryProjectRowsResponse {
  return { offset, rows, total_rows: total, total_filtered_rows: total, annotated_total: 0, annotated_filtered: 0 };
}
async function settle() { for (let step = 0; step < 8; step += 1) await Promise.resolve(); }
function setup(api: Partial<Backend> = {}, id = "a", total = 5199) {
  const cancelled: string[] = [];
  const backend = {
    queryProjectRows: async (args: QueryProjectRowsArgs) => response(args.offset ?? 0, [row(args.offset ?? 0)], total),
    cancelRequest: async (id: string) => { cancelled.push(id); return 1; },
    ...api,
  } as Backend;
  const workspace = createWorkspaceController();
  workspace.backend.set(backend);
  workspace.projectDetail.set(detail(id, total));
  workspace.currentProjectId.set(id);
  workspace.setHistoryProject(id);
  workspace.viewportHeight.set(777);
  const errors: string[] = [];
  const queries = createWorkspaceQuery(workspace, {
    backend, isReady: () => true, restoreViewport: async () => {}, scopeChanged: () => {}, summary: () => {},
    notify: (message) => errors.push(message),
  });
  queries.initialize(filters());
  return { workspace, queries, backend, cancelled, errors };
}

test("investigation instances isolate conditions, sort events, history, selection and page payloads", () => {
  const a = setup({}, "a"), b = setup({}, "b");
  a.workspace.search.set("evidence A");
  a.workspace.filterByValue("c1", "first");
  a.workspace.recordAppliedCondition(10, get(a.workspace.conditionTree), 20);
  a.workspace.toggleSort("c1");
  a.workspace.toggleRowSelection(4000);
  a.workspace.registerLoadedPage(0, 0, [normalizeRow(row(4000, "secret A"))]);
  assert.equal(get(b.workspace.search), "");
  assert.equal(get(b.workspace.conditionTree).children.length, 0);
  assert.equal(get(b.workspace.canGoBack), false);
  assert.equal(get(b.workspace.sortRevision), 0);
  assert.equal(get(b.workspace.selectedCount), 0);
  assert.equal(get(b.workspace.rowsCache).size, 0);
  a.workspace.goBack();
  assert.equal(get(a.workspace.historyRestore)?.projectId, "a");
  assert.equal(get(b.workspace.historyRestore), null);
  a.workspace.dispose();
  assert.equal(get(a.workspace.rowsCache).size, 0);
  assert.equal(get(a.workspace.positionToRowIndex).size, 0);
  assert.equal(get(a.workspace.projectDetail), null);
  assert.equal(get(a.workspace.selectedCount), 0);
  assert.equal(get(a.workspace.backend), null);
  assert.equal(get(a.workspace.canGoBack), false);
  b.workspace.dispose();
});

test("a late annotation remains persisted by its backend but cannot repopulate a disposed investigation", async () => {
  const mutation = deferred<UpdateFlagResult>();
  const a = setup({ updateFlag: () => mutation.promise }, "a");
  const b = setup({}, "b");
  a.workspace.registerLoadedPage(0, 0, [normalizeRow(row(2))]);
  b.workspace.registerLoadedPage(0, 0, [normalizeRow(row(2, "B"))]);
  const updating = a.workspace.setFlag(get(a.workspace.rowsCache).get(2)!, "critical");
  assert.equal(get(a.workspace.rowsCache).get(2)?.flag, "critical");
  a.workspace.dispose();
  assert.deepEqual(a.cancelled, []); // Mutations are never owned by a read scope.
  mutation.resolve({ entry: null, revision: 1, flaggedRecords: 100 });
  await updating;
  assert.equal(get(a.workspace.rowsCache).size, 0);
  assert.equal(get(b.workspace.rowsCache).get(2)?.previewCache.c1, "B");
  assert.equal(get(b.workspace.flaggedCount), 0);
  b.workspace.dispose();
});

test("read scope replacement and disposal cancel exact requests, even after an older completion", async () => {
  const reads = createOwnedRequests();
  const cancelled: string[] = [], started: string[] = [];
  const api = { cancelRequest: async (id: string) => { cancelled.push(id); return 1; } };
  const first = deferred<void>(), second = deferred<void>(), other = deferred<void>();
  const old = reads.run("histogram", api, (id) => { started.push(id); return first.promise; });
  const current = reads.run("histogram", api, (id) => { started.push(id); return second.promise; });
  const independent = reads.run("stats", api, (id) => { started.push(id); return other.promise; });
  assert.deepEqual(cancelled, [started[0]]);
  first.resolve(); await old;
  reads.dispose(); reads.dispose();
  assert.deepEqual(cancelled, started);
  second.resolve(); other.resolve();
  await Promise.all([current, independent]);
  await assert.rejects(reads.run("new", api, async () => {}), /disposed/);
});

test("query generations drain stale queued pages and ignore out-of-order successes and failures", async () => {
  const calls: Array<{ args: QueryProjectRowsArgs; gate: ReturnType<typeof deferred<QueryProjectRowsResponse>> }> = [];
  const { workspace, queries, cancelled, errors } = setup({ queryProjectRows: (args) => {
    const gate = deferred<QueryProjectRowsResponse>(); calls.push({ args, gate }); return gate.promise;
  } });
  const oldFirst = queries.requestPage(0), oldSecond = queries.requestPage(1), oldQueued = queries.requestPage(2);
  await settle();
  assert.equal(calls.length, 2);
  const fresh = queries.applyFilters(filters("new result"), true, true);
  await oldQueued; // Does not wait for the two blocked native requests.
  assert.equal(calls.length, 2);
  assert.equal(cancelled.length, 2);
  calls[1].gate.reject(new Error("stale failure"));
  calls[0].gate.resolve(response(0, [row(0, "stale")], 2));
  await Promise.all([oldFirst, oldSecond]); await settle();
  assert.equal(calls.length, 3);
  assert.equal(calls[2].args.search, "new result");
  assert.equal(get(workspace.appliedResultLoading), true);
  calls[2].gate.resolve(response(0, [row(77, "fresh")], 1));
  await fresh;
  assert.deepEqual([...get(workspace.rowsCache).keys()], [77]);
  assert.deepEqual([...get(workspace.positionToRowIndex)], [[0, 77]]);
  assert.equal(get(workspace.totalFilteredRows), 1);
  assert.equal(get(workspace.appliedResultLoading), false);
  assert.deepEqual(errors, []);
  workspace.dispose();
});

test("disposing a workspace settles queued reads, cancels active jobs, and cannot retain late row payloads", async () => {
  const gates: Array<ReturnType<typeof deferred<QueryProjectRowsResponse>>> = [];
  const { workspace, queries, cancelled } = setup({ queryProjectRows: () => {
    const gate = deferred<QueryProjectRowsResponse>(); gates.push(gate); return gate.promise;
  } });
  const first = queries.requestPage(0), second = queries.requestPage(1), queued = queries.requestPage(2);
  await settle();
  workspace.dispose();
  await queued;
  assert.equal(cancelled.length, 2);
  gates.forEach((gate) => gate.resolve(response()));
  await Promise.all([first, second]);
  assert.equal(get(workspace.rowsCache).size, 0);
  assert.equal(get(workspace.pendingPages).size, 0);
});

test("a current query failure ends loading without losing the evidence row count", async () => {
  const { workspace, queries, errors } = setup({ queryProjectRows: async () => { throw "disk read failed"; } });
  await queries.applyFilters(filters("failure"), true, true);
  assert.equal(get(workspace.totalRows), 5199);
  assert.equal(get(workspace.totalFilteredRows), 0);
  assert.equal(get(workspace.appliedResultLoading), false);
  assert.equal(get(workspace.pendingPages).size, 0);
  assert.deepEqual(errors, ["disk read failed"]);
  workspace.dispose();
});

test("real navigation controller resolves original IDs after sorting and never removes a filter", async () => {
  for (const total of [5199, 2_000_000]) {
    const lookups: Array<{ rowIndex: number; flagFilter?: unknown }> = [];
    const { workspace, queries, backend } = setup({ locateProjectRow: async (args) => {
      lookups.push(args);
      return { position: args.rowIndex === 10 ? null : total - 100, total_rows: total, total_filtered_rows: total };
    } }, "navigate", total);
    workspace.totalFilteredRows.set(total);
    workspace.flagFilter.set("critical");
    workspace.sortKey.set("c1");
    workspace.sortDirection.set("desc");
    const notices: string[] = [];
    const navigation = createWorkspaceNavigation(workspace, queries, {
      backend, isReady: () => true, isFilterPending: () => false, beforeScroll: () => {}, notify: (message) => notices.push(message),
    });
    await navigation.jumpToEnteredRow(77);
    assert.equal(lookups[0].rowIndex, 76);
    assert.equal(lookups[0].flagFilter, "critical");
    const geometry = virtualWindow(get(workspace.scrollTop), total, 777, 8);
    assert.ok(geometry.start <= total - 100 && geometry.end > total - 100);
    await navigation.jumpToEnteredRow(11);
    assert.match(notices[0], /not in the current filtered result/);
    assert.equal(get(workspace.flagFilter), "critical");
    navigation.jumpToFirstRow();
    assert.equal(navigation.rowOffset(), 0);
    navigation.jumpToLastRow();
    assert.equal(virtualWindow(get(workspace.scrollTop), total, 777, 8).end, total);
    assert.ok([...get(workspace.pendingPages)].length <= 3);
    await settle();
    assert.ok([...get(workspace.loadedPages)].every((page) => Number.isInteger(page) && page * PAGE_SIZE < total));
    workspace.dispose();
  }
});

test("First/Last cancel their replaced Go request without cancelling independent page reads", async () => {
  for (const last of [false, true]) {
    const locating = deferred<{ position: number; total_rows: number; total_filtered_rows: number }>();
    const page = deferred<QueryProjectRowsResponse>();
    let lookupId = "", pageId = "";
    const { workspace, queries, backend, cancelled } = setup({
      locateProjectRow: (args) => { lookupId = args.requestId!; return locating.promise; },
      queryProjectRows: (args) => { pageId = args.requestId!; return page.promise; },
    });
    workspace.totalFilteredRows.set(5199);
    const navigation = createWorkspaceNavigation(workspace, queries, {
      backend, isReady: () => true, isFilterPending: () => false, beforeScroll: () => {}, notify: () => {},
    });
    const go = navigation.jumpToEnteredRow(4000);
    const loading = queries.requestPage(1);
    await settle();
    assert.ok(lookupId && pageId);
    if (last) navigation.jumpToLastRow();
    else navigation.jumpToFirstRow();
    assert.deepEqual(cancelled, [lookupId]);
    assert.equal(get(navigation.locatingRow), false);
    const boundary = get(workspace.scrollTop);
    locating.resolve({ position: 3999, total_rows: 5199, total_filtered_rows: 5199 });
    page.resolve(response(PAGE_SIZE));
    await Promise.all([go, loading]);
    assert.equal(get(workspace.scrollTop), boundary, "a cancelled Go must not move away from the boundary");
    assert.deepEqual(cancelled, [lookupId]);
    await settle();
    workspace.dispose();
  }
});

test("restored annotation baselines cannot supersede a slow first query and discard its saved position", async () => {
  const projectId = "slow-restored-query";
  markBulkAnnotationsChanged(projectId);
  const gate = deferred<QueryProjectRowsResponse>();
  let calls = 0;
  const { workspace, queries, cancelled } = setup({ queryProjectRows: () => { calls += 1; return gate.promise; } }, projectId);
  workspace.currentProjectId.set(null);
  const observer = createWorkspaceAnnotationObserver(workspace, {
    single: () => {}, bulk: () => { void queries.applyFilters(filters(), false, true); },
  });
  workspace.currentProjectId.set(projectId);
  workspace.scrollTop.set(6624);
  observer.initialize();
  workspace.appliedResultLoading.set(true);
  const initial = queries.requestPage(0);
  await settle();
  assert.equal(calls, 1);
  assert.deepEqual(cancelled, [], "an existing bulk revision must not cancel the slow initial query");
  gate.resolve({ ...response(0), total_filtered_rows: 330 });
  await initial;
  assert.equal(get(workspace.scrollTop), 6624);
  assert.equal(get(workspace.totalFilteredRows), 330);
  assert.equal(get(workspace.appliedResultLoading), false);
  markBulkAnnotationsChanged(projectId);
  await settle();
  assert.equal(calls, 2, "a later real bulk mutation must still refresh the active case");
  workspace.dispose();
  forgetAnnotationVersion(projectId);
});


test("empty or whitespace search scope changes preserve cached pages, selection, position and saved picker scope", async () => {
  const requests: QueryProjectRowsArgs[] = [];
  const { workspace, queries, backend, cancelled } = setup({ queryProjectRows: async (args) => {
    requests.push(args); return response(args.offset ?? 0);
  } });
  await queries.requestPage(0);
  workspace.scrollTop.set(640); workspace.scrollLeft.set(80); workspace.toggleRowSelection(23);
  const cached = get(workspace.rowsCache), applied = get(workspace.appliedFilters);
  const revision = queries.revision, signature = queries.signature;
  const scopeKey = effectiveSearchScopeKey("", []);
  for (const search of ["", "  ", "\t\n", "\u0085", "\u2003\u2028"]) {
    workspace.search.set(search);
    workspace.searchColumns.set(["c1"]);
    assert.equal(effectiveSearchScopeKey(search, get(workspace.searchColumns)), scopeKey);
    await queries.applyFilters({ ...filters(search), searchColumns: get(workspace.searchColumns) }, true, false);
    workspace.searchColumns.set([]);
    await queries.applyFilters(filters(search), true, false);
  }
  workspace.searchColumns.set(["c1"]);
  const view = createWorkspaceView(workspace, { backend, isReady: () => true, columnWidths: () => new Map(),
    logicalRow: () => 20, scrollLeft: () => 80, signature: () => queries.signature, notify: () => {} });
  assert.equal(requests.length, 1);
  assert.deepEqual(cancelled, []);
  assert.equal(queries.revision, revision); assert.equal(queries.signature, signature);
  assert.strictEqual(get(workspace.rowsCache), cached); assert.strictEqual(get(workspace.appliedFilters), applied);
  assert.equal(get(workspace.scrollTop), 640); assert.equal(get(workspace.scrollLeft), 80);
  assert.deepEqual([...get(workspace.selectedRowIds)], [23]);
  assert.deepEqual(view.buildViewState().searchColumns, ["c1"], "unused picker changes still persist");
  assert.deepEqual(view.snapshotSession()?.selectedRowIds, [23]);
  assert.equal(view.snapshotSession()?.selectionSignature, queries.signature);
  workspace.dispose();
});

test("the first real query uses the chosen scope and a scope change during text search reloads normally", async () => {
  const requests: QueryProjectRowsArgs[] = [];
  const { workspace, queries } = setup({ queryProjectRows: async (args) => {
    requests.push(args); return response(args.offset ?? 0);
  } });
  await queries.requestPage(0);
  workspace.searchColumns.set(["c1"]);
  await queries.applyFilters({ ...filters("  "), searchColumns: ["c1"] }, false, false);
  assert.equal(requests.length, 1);
  workspace.search.set("  \u0085needle\u0085  ");
  await queries.applyFilters({ ...filters(get(workspace.search)), searchColumns: get(workspace.searchColumns) }, false, false);
  assert.equal(requests.length, 2);
  assert.equal(requests[1].search, "needle"); assert.deepEqual(requests[1].searchColumns, ["c1"]);
  workspace.searchColumns.set([]);
  await queries.applyFilters(filters("needle"), false, false);
  assert.equal(requests.length, 3); assert.deepEqual(requests[2].searchColumns, []);
  assert.equal(get(workspace.appliedFilters).search, "needle");
  workspace.dispose();
});

test("an unused search-column change cannot supersede an in-flight original-row Go", async () => {
  const located = deferred<{ position: number; total_rows: number; total_filtered_rows: number }>();
  const lookups: QueryProjectRowsArgs[] = [];
  let queryCalls = 0;
  const { workspace, queries, backend, cancelled, errors } = setup({
    queryProjectRows: async (args) => { queryCalls += 1; return response(args.offset ?? 0); },
    locateProjectRow: (args) => { lookups.push(args); return located.promise; },
  });
  await queries.requestPage(0);
  const navigation = createWorkspaceNavigation(workspace, queries, {
    backend, isReady: () => true, isFilterPending: () => false, beforeScroll: () => {}, notify: (message) => errors.push(message),
  });
  const moving = navigation.jumpToEnteredRow(43);
  await settle();
  assert.equal(lookups.length, 1); assert.deepEqual(lookups[0].searchColumns, []);
  workspace.search.set(" "); workspace.searchColumns.set(["c1"]);
  await queries.applyFilters({ ...filters(" "), searchColumns: ["c1"] }, false, false);
  assert.equal(queryCalls, 1); assert.deepEqual(cancelled, []); assert.equal(get(navigation.locatingRow), true);
  located.resolve({ position: 42, total_rows: 5199, total_filtered_rows: 5199 });
  await moving;
  assert.ok(get(workspace.scrollTop) > 0, "the original Go still scrolls to its requested row");
  assert.equal(get(navigation.locatingRow), false); assert.deepEqual(errors, []);
  assert.equal(queryCalls, 1, "the target's already cached page stays available");
  workspace.dispose();
});
