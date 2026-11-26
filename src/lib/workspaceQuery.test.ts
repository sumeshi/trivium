import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import type { Backend, LocateProjectRowResponse, QueryProjectRowsArgs, QueryProjectRowsResponse } from "./backend.ts";
import type { LoadProjectResponse } from "./types.ts";
import { PAGE_SIZE } from "./components/project_view/state.ts";
import { createWorkspaceController } from "./workspaceController.ts";
import { createWorkspaceQuery } from "./workspaceQuery.ts";
import { createWorkspaceNavigation } from "./workspaceNavigation.ts";
import { markAnnotationsChanged, forgetAnnotationVersion } from "./annotationVersions.ts";

const TOTAL = PAGE_SIZE * 100;
async function settle() { for (let step = 0; step < 16; step += 1) await Promise.resolve(); }

function setup() {
  const calls: Array<{ page: number; resolve: () => void }> = [];
  const notices: string[] = [], cancelled: string[] = [];
  let active = 0, maximumActive = 0;
  const backend = {
    queryProjectRows: (args: QueryProjectRowsArgs) => {
      const offset = args.offset ?? 0;
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      return new Promise<QueryProjectRowsResponse>((resolve) => {
        let resolved = false;
        calls.push({ page: offset / PAGE_SIZE, resolve: () => {
          if (resolved) return;
          resolved = true;
          active -= 1;
          resolve({ offset, rows: [{ color: null,  row_index: offset, cells: { c1: { truncated: false, length: Array.from(String(offset)).length,  kind: "string", text: String(offset) } },
            flag: "", memo: "", tags: [], bookmarked: false }], total_rows: TOTAL,
            total_filtered_rows: TOTAL, annotated_total: 0, annotated_filtered: 0, annotation_revision: 0 });
        } });
      });
    },
    cancelRequest: async (id: string) => { cancelled.push(id); return 1; },
  } as Backend;
  const workspace = createWorkspaceController();
  workspace.currentProjectId.set("queue-test");
  workspace.projectDetail.set({ project: { meta: { id: "queue-test", total_records: TOTAL } },
    columns: ["c1"], columns_meta: [], hidden_columns: [],  } as unknown as LoadProjectResponse);
  const queries = createWorkspaceQuery(workspace, { backend, isReady: () => true,
    restoreViewport: async () => {}, scopeChanged: () => {}, summary: () => {},
    notify: (message) => notices.push(message) });
  queries.initialize({ search: "", flag: "all", bookmarkedOnly: false, searchColumns: [], condition: null });
  workspace.totalFilteredRows.set(TOTAL);
  const show = (page: number) => queries.ensureRangeLoaded(page * PAGE_SIZE, page * PAGE_SIZE + 40);
  const pages = () => calls.map((call) => call.page);
  const finish = async () => {
    workspace.dispose();
    for (const call of calls) call.resolve();
    await settle();
    assert.equal(active, 0);
    assert.equal(get(workspace.pendingPages).size, 0);
    assert.ok(maximumActive <= 2, `Native page concurrency exceeded two: ${maximumActive}`);
    assert.deepEqual(notices, []);
  };
  return { workspace, queries, calls, cancelled, show, pages, finish };
}

test("visible rows load before prefetch, with the next page following scroll direction", async () => {
  const { show, calls, pages, finish } = setup();
  show(5);
  await settle();
  assert.deepEqual(pages(), [5, 6], "The old preceding page must not occupy the first native slot");
  show(4);
  calls[0].resolve();
  await settle();
  assert.deepEqual(pages(), [5, 6, 4]);
  calls[1].resolve();
  await settle();
  assert.deepEqual(pages(), [5, 6, 4, 3], "Upward scrolling prefetches the preceding page first");
  await finish();
});

test("rapid jumps discard queued offscreen pages without resetting cached rows or selection", async () => {
  const { workspace, queries, show, calls, pages, finish } = setup();
  const revision = queries.revision, signature = queries.signature;
  workspace.toggleRowSelection(7);
  show(5);
  await settle();
  show(20);
  show(30);
  await settle();
  assert.equal(get(workspace.pendingPages).size, 5, "Only two running reads and three current viewport pages remain");
  calls[0].resolve();
  await settle();
  assert.deepEqual(pages(), [5, 6, 30]);
  assert.ok(get(workspace.rowsCache).has(5 * PAGE_SIZE), "A completed old viewport read is still valid cache data");
  calls[1].resolve();
  await settle();
  assert.deepEqual(pages(), [5, 6, 30, 31]);
  calls[2].resolve();
  await settle();
  assert.deepEqual(pages(), [5, 6, 30, 31, 29]);
  assert.equal(queries.revision, revision);
  assert.equal(queries.signature, signature);
  assert.deepEqual([...get(workspace.selectedRowIds)], [7]);
  await finish();
});

test("an explicit jump promotes an existing queued prefetch without duplicating its request", async () => {
  const { queries, show, calls, pages, finish } = setup();
  void queries.requestPage(0);
  void queries.requestPage(1);
  await settle();
  show(5);
  const jump = queries.requestPage(4);
  const duplicate = queries.requestPage(4);
  show(15);
  calls[0].resolve();
  await settle();
  assert.deepEqual(pages(), [0, 1, 4], "The explicit jump outranks newer viewport prefetch");
  calls[1].resolve();
  await settle();
  assert.deepEqual(pages(), [0, 1, 4, 15]);
  calls[2].resolve();
  await Promise.all([jump, duplicate]);
  assert.equal(pages().filter((page) => page === 4).length, 1);
  await finish();
});

test("original-row lookups take the next slot ahead of queued viewport pages", async () => {
  const { queries, show, calls, pages, finish } = setup();
  void queries.requestPage(0);
  void queries.requestPage(1);
  await settle();
  show(5);
  let located = false;
  const lookup = queries.runLookup(() => true, async () => { located = true; return 42; });
  calls[0].resolve();
  await settle();
  assert.equal(located, true);
  assert.equal(await lookup, 42);
  assert.deepEqual(pages(), [0, 1, 5]);
  await finish();
});

test("cancelling a queued lookup settles it while both native slots are still blocked", async () => {
  const { queries, calls, finish } = setup();
  void queries.requestPage(0);
  void queries.requestPage(1);
  await settle();
  let current = true, started = false;
  const lookup = queries.runLookup(() => current, async () => { started = true; return 42; });
  current = false;
  queries.cancelLookup();
  assert.equal(await lookup, undefined);
  assert.equal(started, false);
  assert.equal(calls.length, 2);
  await finish();
});

test("returning immediately to a dropped page cannot let its old completion clear the new pending key", async () => {
  const { workspace, queries, show, calls, pages, finish } = setup();
  void queries.requestPage(0);
  void queries.requestPage(1);
  await settle();
  show(5);
  show(20);
  show(5);
  await settle();
  const key = workspace.pendingPageKey(get(workspace.viewGeneration), queries.revision, 5);
  assert.ok(get(workspace.pendingPages).has(key));
  calls[0].resolve();
  await settle();
  assert.deepEqual(pages(), [0, 1, 5]);
  assert.ok(get(workspace.pendingPages).has(key), "The replacement request stays pending until its own response");
  await finish();
});

test("a stale annotation prefetch yields its slot to the current visible page instead of retrying offscreen", async () => {
  const { show, calls, pages, finish } = setup();
  show(5);
  await settle();
  assert.deepEqual(pages(), [5, 6]);
  show(20);
  markAnnotationsChanged("queue-test");
  calls[1].resolve();
  await settle();
  assert.deepEqual(pages(), [5, 6, 20]);
  assert.equal(pages().filter((page) => page === 6).length, 1);
  await finish();
  forgetAnnotationVersion("queue-test");
});

function queryStateSetup(columns = ["c1"], hiddenColumns: string[] = []) {
  const calls: Array<{ args: QueryProjectRowsArgs; resolve: (count?: number) => void; reject: (message: string) => void }> = [];
  const notices: string[] = [], cancelled: string[] = [];
  const workspace = createWorkspaceController();
  workspace.currentProjectId.set("query-state");
  workspace.projectDetail.set({ project: { meta: { id: "query-state", total_records: TOTAL } },
    columns, columns_meta: [], hidden_columns: hiddenColumns,  } as unknown as LoadProjectResponse);
  workspace.hiddenColumns.set(new Set(hiddenColumns));
  const backend = { queryProjectRows: (args: QueryProjectRowsArgs) => new Promise<QueryProjectRowsResponse>((resolve, reject) => {
    calls.push({ args, reject: message => reject(new Error(message)), resolve: (count = TOTAL) => {
      const offset = args.offset ?? 0;
      resolve({ offset, rows: count > offset ? [{ color: null,  row_index: offset, cells: Object.fromEntries((args.pageColumns ?? columns)
        .map(column => [column, { truncated: false, length: Array.from(column === "c1" ? args.search || "source" : column + " source").length,  kind: "string", text: column === "c1" ? args.search || "source" : column + " source" }])),
        flag: "", memo: "", tags: [], bookmarked: false }] : [], total_rows: TOTAL,
        total_filtered_rows: count, annotated_total: 0, annotated_filtered: 0, annotation_revision: 0 });
    } });
  }), cancelRequest: async (requestId: string) => { cancelled.push(requestId); return 1; } } as unknown as Backend;
  const queries = createWorkspaceQuery(workspace, { backend, isReady: () => true,
    restoreViewport: async () => {}, scopeChanged: () => {}, summary: () => {}, notify: message => notices.push(message) });
  const filters = (search = "") => ({ search, flag: "all" as const, bookmarkedOnly: false, searchColumns: [], condition: null });
  queries.initialize(filters());
  return { workspace, queries, backend, calls, notices, cancelled, filters };
}

function queryPayload(args: QueryProjectRowsArgs) {
  const { requestId: _, ...payload } = args;
  return payload;
}

test("an initial query failure has an unknown result count and a persistent reason until explicit Retry", async () => {
  const { workspace, queries, calls, notices } = queryStateSetup();
  const initial = queries.requestPage(4);
  await settle();
  assert.equal(get(workspace.queryCountKnown), false);
  assert.equal(get(workspace.appliedResultLoading), true);
  assert.equal(get(workspace.queryError), null);
  calls[0].reject("Could not read project data.");
  await initial;
  assert.equal(get(workspace.queryCountKnown), false, "a failed query is not a successful empty result");
  assert.equal(get(workspace.totalRows), TOTAL, "the source count remains available");
  assert.equal(get(workspace.appliedResultLoading), false);
  assert.deepEqual(get(workspace.queryError), { message: "Could not read project data.", page: 4 });
  for (let attempt = 0; attempt < 3; attempt++) queries.ensureRangeLoaded(1000, 1040);
  await settle();
  assert.equal(calls.length, 1, "a failed first query must not restart on every viewport update");
  const revision = queries.revision;
  const retry = queries.retryFailedPage();
  await settle();
  assert.equal(get(workspace.queryError), null);
  assert.equal(get(workspace.appliedResultLoading), true);
  assert.deepEqual(queryPayload(calls[1].args), queryPayload(calls[0].args));
  calls[1].resolve();
  await retry;
  assert.equal(queries.revision, revision, "retry keeps the query generation and requested page");
  assert.equal(get(workspace.queryCountKnown), true);
  assert.equal(get(workspace.totalFilteredRows), TOTAL);
  assert.equal(get(workspace.appliedResultLoading), false);
  assert.deepEqual(notices, ["Could not read project data."]);
  workspace.dispose();
});

test("a failed changed query clears old rows, then a successful empty retry is distinguishable from failure", async () => {
  const { workspace, queries, calls, filters } = queryStateSetup();
  const original = queries.requestPage(0);
  await settle(); calls[0].resolve(21); await original;
  assert.equal(get(workspace.queryCountKnown), true);
  assert.equal(get(workspace.rowsCache).size, 1);
  const changed = queries.applyFilters(filters("missing | term"), true, false);
  await settle();
  assert.equal(get(workspace.queryCountKnown), false);
  assert.equal(get(workspace.totalFilteredRows), 21, "internal scroll geometry is retained until the new count is known");
  assert.equal(get(workspace.rowsCache).size, 0, "previous-query evidence cannot appear under the new filter");
  calls[1].reject("Invalid search expression.");
  await changed;
  assert.equal(get(workspace.queryCountKnown), false);
  assert.deepEqual(get(workspace.queryError), { message: "Invalid search expression.", page: 0 });
  assert.equal(get(workspace.appliedFilters).search, "missing | term");
  const retry = queries.retryFailedPage();
  await settle();
  assert.equal(calls[2].args.search, "missing | term");
  calls[2].resolve(0); await retry;
  assert.equal(get(workspace.queryCountKnown), true);
  assert.equal(get(workspace.totalFilteredRows), 0);
  assert.equal(get(workspace.rowsCache).size, 0);
  assert.equal(get(workspace.queryError), null, "a valid empty result has no failure reason");
  workspace.dispose();
});

test("a failed page retains known counts and good cached rows; unrelated page successes do not dismiss its error", async () => {
  const { workspace, queries, calls } = queryStateSetup();
  const first = queries.requestPage(0);
  await settle(); calls[0].resolve(); await first;
  const originalRow = get(workspace.rowsCache).get(0);
  const page = queries.requestPage(1);
  await settle(); calls[1].reject("Page unavailable."); await page;
  assert.equal(get(workspace.queryCountKnown), true);
  assert.equal(get(workspace.totalFilteredRows), TOTAL);
  assert.equal(get(workspace.rowsCache).get(0), originalRow);
  assert.deepEqual(get(workspace.queryError), { message: "Page unavailable.", page: 1 });
  const other = queries.requestPage(2);
  await settle(); calls[2].resolve(); await other;
  assert.deepEqual(get(workspace.queryError), { message: "Page unavailable.", page: 1 });
  for (let attempt = 0; attempt < 3; attempt++) queries.ensureRangeLoaded(PAGE_SIZE, PAGE_SIZE + 40);
  await settle();
  assert.equal(calls.filter(call => call.args.offset === PAGE_SIZE).length, 1, "automatic prefetch cannot loop a failing page");
  const revision = queries.revision;
  const retry = queries.retryFailedPage();
  await settle();
  assert.deepEqual(queryPayload(calls[3].args), queryPayload(calls[1].args));
  calls[3].resolve(); await retry;
  assert.equal(get(workspace.queryError), null);
  assert.equal(get(workspace.rowsCache).get(0), originalRow, "Retry does not discard a valid cache");
  assert.equal(queries.revision, revision);
  assert.ok(get(workspace.loadedPages).has(0) && get(workspace.loadedPages).has(1));
  workspace.dispose();
});

test("failure while refreshing a known page preserves the previous result and retries the same page", async () => {
  const { workspace, queries, calls } = queryStateSetup();
  const first = queries.requestPage(0);
  await settle(); calls[0].resolve(17); await first;
  const oldRow = get(workspace.rowsCache).get(0);
  const refresh = queries.requestPage(0, true);
  await settle(); calls[1].reject("Refresh failed."); await refresh;
  assert.equal(get(workspace.queryCountKnown), true);
  assert.equal(get(workspace.totalFilteredRows), 17);
  assert.equal(get(workspace.rowsCache).get(0), oldRow);
  const retry = queries.retryFailedPage();
  await settle();
  assert.equal(calls.length, 3, "a failed cached page must not suppress explicit Retry");
  calls[2].resolve(19); await retry;
  assert.equal(get(workspace.totalFilteredRows), 19);
  assert.equal(get(workspace.queryError), null);
  workspace.dispose();
});

for (const lateSuccess of [false, true]) {
  test(`a late old-query ${lateSuccess ? "success" : "failure"} cannot hide the current error or invent a count`, async () => {
    const { workspace, queries, calls, filters, notices } = queryStateSetup();
    const old = queries.requestPage(0);
    await settle();
    const current = queries.applyFilters(filters("new query"), true, false);
    await settle();
    calls[1].reject("Current query failed."); await current;
    if (lateSuccess) calls[0].resolve(7); else calls[0].reject("Old query failed.");
    await old;
    assert.equal(get(workspace.queryCountKnown), false);
    assert.equal(get(workspace.rowsCache).size, 0);
    assert.deepEqual(get(workspace.queryError), { message: "Current query failed.", page: 0 });
    assert.deepEqual(notices, ["Current query failed."]);
    workspace.dispose();
  });
}

test("changing a failed query clears its error and establishes a fresh pending result", async () => {
  const { workspace, queries, calls, filters } = queryStateSetup();
  const failed = queries.applyFilters(filters("invalid"), true, false);
  await settle(); calls[0].reject("Invalid filter."); await failed;
  const corrected = queries.applyFilters(filters("valid"), true, false);
  await settle();
  assert.equal(get(workspace.queryError), null);
  assert.equal(get(workspace.queryCountKnown), false);
  assert.equal(get(workspace.appliedResultLoading), true);
  calls[1].resolve(3); await corrected;
  assert.equal(get(workspace.queryCountKnown), true);
  assert.equal(get(workspace.totalFilteredRows), 3);
  assert.equal(get(workspace.queryError), null);
  workspace.dispose();
});

test("visibility refreshes every cached page and retires old responses while preserving the applied investigation", async () => {
  const { workspace, queries, calls, notices, cancelled, filters } = queryStateSetup(["c1", "c2"], ["c2"]);
  queries.initialize({ ...filters("needle"), searchColumns: ["c2"] });
  const first = queries.requestPage(0);
  await settle(); calls[0].resolve(); await first;
  const offscreen = queries.requestPage(2);
  await settle(); calls[1].resolve(); await offscreen;
  const old = queries.requestPage(1);
  await settle();
  workspace.viewportHeight.set(160);
  workspace.scrollTop.set(PAGE_SIZE * get(workspace.rowHeight));
  workspace.scrollLeft.set(170);
  workspace.toggleRowSelection(0);
  const signature = queries.signature, revision = queries.revision;
  const applied = get(workspace.appliedFilters), top = get(workspace.scrollTop);
  workspace.hiddenColumns.set(new Set());
  await settle();
  assert.equal(queries.revision, revision, "presentation changes keep the membership revision used by navigation");
  assert.ok(cancelled.length > 0);
  assert.equal(queries.signature, signature);
  assert.equal(get(workspace.appliedFilters), applied);
  assert.equal(get(workspace.queryCountKnown), true);
  assert.equal(get(workspace.totalFilteredRows), TOTAL);
  assert.deepEqual([...get(workspace.selectedRowIds)], [0]);
  assert.equal(get(workspace.scrollTop), top);
  assert.equal(get(workspace.scrollLeft), 170);
  assert.equal(get(workspace.rowsCache).size, 0, "offscreen pages with omitted cells must also be dropped");
  assert.equal(get(workspace.loadedPages).size, 0);
  assert.deepEqual(calls[3].args.pageColumns, ["c1", "c2"]);
  assert.equal(calls[3].args.offset, PAGE_SIZE, "the viewport page reloads instead of returning to the first page");
  assert.equal(calls[3].args.search, "needle");
  assert.deepEqual(calls[3].args.searchColumns, ["c2"], "search scope is independent of page projection");
  calls[3].resolve(); await settle();
  calls[2].resolve(); await old;
  assert.equal(get(workspace.rowsCache).get(PAGE_SIZE)?.displayCache.c2, "c2 source", "the old omitted-column response cannot overwrite new cells");
  assert.deepEqual([...get(workspace.loadedPages)], [1]);
  const revisited = queries.requestPage(2);
  await settle();
  assert.deepEqual(calls[4].args.pageColumns, ["c1", "c2"]);
  calls[4].resolve(); await revisited;
  assert.equal(get(workspace.rowsCache).get(PAGE_SIZE * 2)?.displayCache.c2, "c2 source");
  workspace.columnOrder.set(["c2", "c1"]);
  await settle();
  assert.equal(calls.length, 5, "reordering the same visible set does not reread evidence");
  workspace.hiddenColumns.set(new Set(["c2"])); await settle();
  workspace.hiddenColumns.set(new Set()); await settle();
  assert.equal(calls.length, 5, "hide and reveal reuse cells already held by every cached page");
  workspace.hiddenColumns.set(new Set(["c2"]));
  const omitted = queries.requestPage(3); await settle();
  assert.deepEqual(calls[5].args.pageColumns, ["c1"]);
  workspace.hiddenColumns.set(new Set()); await settle();
  assert.deepEqual(calls[6].args.pageColumns, ["c2", "c1"]);
  calls[6].resolve(); await settle();
  calls[5].reject("Retired hidden projection failed"); await omitted; await settle();
  assert.equal(get(workspace.rowsCache).get(PAGE_SIZE)?.displayCache.c2, "c2 source");
  assert.equal(get(workspace.queryError), null);
  assert.deepEqual(notices, []);
  workspace.dispose();
});

test("an empty visible projection stays explicit and disposed tabs ignore projection responses", async () => {
  const { workspace, queries, calls, notices } = queryStateSetup(["c1", "c2"]);
  const original = queries.requestPage(0);
  await settle(); calls[0].resolve(); await original;
  workspace.hiddenColumns.set(new Set(["c1", "c2"]));
  await settle();
  assert.equal(calls.length, 1, "hiding all columns leaves the query and existing cells untouched");
  const hidden = queries.requestPage(1); await settle();
  assert.deepEqual(calls[1].args.pageColumns, [], "all-hidden is distinct from the default saved visibility");
  workspace.hiddenColumns.set(new Set());
  await settle();
  assert.deepEqual(calls[2].args.pageColumns, ["c1", "c2"]);
  workspace.dispose();
  calls[2].resolve(); calls[1].reject("Old tab projection failed");
  await hidden; await settle();
  assert.equal(get(workspace.rowsCache).size, 0);
  assert.equal(get(workspace.pendingPages).size, 0);
  assert.deepEqual(notices, []);
});

for (const failed of [false, true]) {
  test(`visibility preserves an ${failed ? "errored" : "empty"} result until the next requested page`, async () => {
    const { workspace, queries, calls } = queryStateSetup(["c1", "c2"], ["c2"]);
    const initial = queries.requestPage(0);
    await settle();
    if (failed) calls[0].reject("Invalid search"); else calls[0].resolve(0);
    await initial;
    const error = get(workspace.queryError);
    workspace.hiddenColumns.set(new Set()); await settle();
    assert.equal(calls.length, 1, "a visibility edit cannot retry a failed query or rerun an empty result");
    assert.equal(get(workspace.queryError), error);
    assert.equal(get(workspace.queryCountKnown), !failed);
    const next = failed ? queries.retryFailedPage() : queries.requestPage(0, true);
    await settle();
    assert.deepEqual(calls[1].args.pageColumns, ["c1", "c2"]);
    calls[1].resolve(); await next;
    assert.equal(get(workspace.rowsCache).get(0)?.displayCache.c2, "c2 source");
    workspace.dispose();
  });
}

for (const queued of [false, true]) {
  test(`a visibility change preserves an ${queued ? "awaiting-slot" : "in-flight"} original-row Go`, async () => {
    const { workspace, queries, backend, calls, notices, cancelled } = queryStateSetup(["c1", "c2"], ["c2"]);
    const initial = queries.requestPage(0);
    await settle(); calls[0].resolve(); await initial;
    workspace.viewportHeight.set(160);
    let lookupId = "";
    let resolveLocate!: (result: LocateProjectRowResponse) => void;
    const locating = new Promise<LocateProjectRowResponse>(resolve => { resolveLocate = resolve; });
    backend.locateProjectRow = args => { lookupId = args.requestId!; return locating; };
    const navigation = createWorkspaceNavigation(workspace, queries, {
      backend, isReady: () => true, isFilterPending: () => false, beforeScroll: () => {},
      notify: message => notices.push(message),
    });
    const blocked: Promise<void>[] = [];
    if (queued) {
      blocked.push(queries.requestPage(1), queries.requestPage(2));
      await settle();
    }
    const position = PAGE_SIZE * 4;
    const moving = navigation.jumpToEnteredRow(position + 1);
    await settle();
    if (!queued) {
      assert.ok(lookupId);
      blocked.push(queries.requestPage(1));
      await settle();
    } else assert.equal(lookupId, "", "both native slots are occupied by page reads");
    const revision = queries.revision;
    workspace.hiddenColumns.set(new Set());
    await settle();
    assert.equal(queries.revision, revision);
    assert.equal(get(navigation.locatingRow), true, "presentation changes do not discard the requested navigation");
    if (queued) {
      calls[1].resolve(); await blocked[0]; await settle();
      assert.ok(lookupId, "the preserved Go waiter takes the next released slot");
    }
    assert.ok(!cancelled.includes(lookupId), "only obsolete page read IDs are cancelled");
    resolveLocate({ position, total_rows: TOTAL, total_filtered_rows: TOTAL });
    await moving;
    assert.ok(navigation.rowOffset() > 0 && navigation.rowOffset() <= position, "the original Go scrolls to the located evidence");
    assert.equal(get(navigation.locatingRow), false);
    assert.ok(!cancelled.includes(lookupId));
    assert.deepEqual(notices, []);
    workspace.dispose();
    for (const call of calls) call.resolve();
    await Promise.all(blocked); await settle();
  });
}
