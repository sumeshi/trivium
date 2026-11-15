import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import type { Backend, QueryProjectRowsArgs, QueryProjectRowsResponse } from "./backend.ts";
import type { LoadProjectResponse } from "./types.ts";
import { PAGE_SIZE } from "./components/project_view/state.ts";
import { createWorkspaceController } from "./workspaceController.ts";
import { createWorkspaceQuery } from "./workspaceQuery.ts";
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
          resolve({ offset, rows: [{ row_index: offset, cells: { c1: { kind: "string", text: String(offset) } },
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
    columns: ["c1"], columns_meta: [], hidden_columns: [], initial_rows: [] } as unknown as LoadProjectResponse);
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

function queryStateSetup() {
  const calls: Array<{ args: QueryProjectRowsArgs; resolve: (count?: number) => void; reject: (message: string) => void }> = [];
  const notices: string[] = [];
  const workspace = createWorkspaceController();
  workspace.currentProjectId.set("query-state");
  workspace.projectDetail.set({ project: { meta: { id: "query-state", total_records: TOTAL } },
    columns: ["c1"], columns_meta: [], hidden_columns: [], initial_rows: [] } as unknown as LoadProjectResponse);
  const backend = { queryProjectRows: (args: QueryProjectRowsArgs) => new Promise<QueryProjectRowsResponse>((resolve, reject) => {
    calls.push({ args, reject: message => reject(new Error(message)), resolve: (count = TOTAL) => {
      const offset = args.offset ?? 0;
      resolve({ offset, rows: count > offset ? [{ row_index: offset, cells: { c1: { kind: "string", text: args.search || "source" } },
        flag: "", memo: "", tags: [], bookmarked: false }] : [], total_rows: TOTAL,
        total_filtered_rows: count, annotated_total: 0, annotated_filtered: 0, annotation_revision: 0 });
    } });
  }), cancelRequest: async () => 1 } as unknown as Backend;
  const queries = createWorkspaceQuery(workspace, { backend, isReady: () => true,
    restoreViewport: async () => {}, scopeChanged: () => {}, summary: () => {}, notify: message => notices.push(message) });
  const filters = (search = "") => ({ search, flag: "all" as const, bookmarkedOnly: false, searchColumns: [], condition: null });
  queries.initialize(filters());
  return { workspace, queries, calls, notices, filters };
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
