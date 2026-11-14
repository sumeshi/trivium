import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import type { Backend, QueryProjectRowsArgs, QueryProjectRowsResponse } from "./backend.ts";
import type { LoadProjectResponse } from "./types.ts";
import { PAGE_SIZE } from "./components/project_view/state.ts";
import { createWorkspaceController } from "./workspaceController.ts";
import { createWorkspaceQuery } from "./workspaceQuery.ts";

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
            total_filtered_rows: TOTAL, annotated_total: 0, annotated_filtered: 0 });
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
