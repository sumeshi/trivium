import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { createProjectController } from "./stores/projects.ts";
import { copyWorkspaceSession, saveWorkspaceView, waitForWorkspaceSave } from "./workspaceSession.ts";
import type { WorkspaceSession } from "./workspaceSession.ts";
import type { Backend } from "./backend.ts";
import type { LoadProjectResponse, ProjectSummary, ViewState } from "./types.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

function summary(id: string): ProjectSummary {
  return { meta: { id, name: id, source_file_name: `${id}.csv`, created_at: "2026-11-11",
    total_records: 100, flagged_records: 0, ioc_applied_records: 0, hidden_columns: [] } };
}

function detail(id: string): LoadProjectResponse {
  return { project: summary(id), columns: ["c1"], columns_meta: [], column_max_chars: {}, column_kinds: {}, color_rules: [], iocs: [],
    hidden_columns: [], initial_rows: [{ row_index: 0, cells: { c1: { kind: "string", text: "seed" } },
      flag: "", memo: "", tags: [], bookmarked: false }] };
}

function fixture(overrides: Partial<Backend> = {}) {
  let deleted = 0;
  const projects = [summary("a"), summary("b"), summary("c")];
  const backend = {
    listProjects: async () => projects,
    loadProject: async (id: string) => detail(id),
    deleteProject: async () => { deleted += 1; },
    ...overrides,
  } as Backend;
  return { controller: createProjectController(backend), backend, projects, deleted: () => deleted };
}

test("opening and closing compact tabs never deletes saved projects or caches inactive rows", async () => {
  const { controller, deleted } = fixture();
  await controller.loadProjects();
  await controller.selectProject("a");
  await controller.selectProject("b");
  await controller.selectProject("c");
  assert.deepEqual(get(controller.state).openProjectIds, ["a", "b", "c"]);
  assert.deepEqual(get(controller.state).projectDetail?.initial_rows, []);
  await controller.closeProject("b");
  assert.equal(get(controller.state).selectedProjectId, "c");
  await controller.closeProject("c");
  assert.equal(get(controller.state).selectedProjectId, "a");
  await controller.closeProject("a");
  assert.equal(get(controller.state).selectedProjectId, null);
  assert.equal(get(controller.state).projectDetail, null);
  assert.equal(get(controller.state).projects.length, 3);
  assert.equal(deleted(), 0);
  await controller.selectProject("b");
  assert.equal(get(controller.state).projectDetail?.project.meta.id, "b");
});

test("late project details cannot replace the active tab", async () => {
  const a = deferred<LoadProjectResponse>();
  const b = deferred<LoadProjectResponse>();
  const { controller } = fixture({ loadProject: (id) => id === "a" ? a.promise : b.promise });
  const first = controller.selectProject("a");
  const second = controller.selectProject("b");
  b.resolve(detail("b"));
  await second;
  a.resolve(detail("a"));
  await first;
  assert.equal(get(controller.state).selectedProjectId, "b");
  assert.equal(get(controller.state).projectDetail?.project.meta.id, "b");
  assert.deepEqual(get(controller.state).openProjectIds, ["a", "b"]);
});

test("closing a loading tab invalidates both its late success and late failure", async () => {
  for (const reject of [false, true]) {
    const response = deferred<LoadProjectResponse>();
    const { controller } = fixture({ loadProject: () => response.promise });
    const loading = controller.selectProject("a");
    await controller.closeProject("a");
    if (reject) response.reject(new Error("late failure"));
    else response.resolve(detail("a"));
    await loading;
    assert.equal(get(controller.state).selectedProjectId, null);
    assert.equal(get(controller.state).projectDetail, null);
    assert.equal(get(controller.state).isLoadingDetail, false);
    assert.deepEqual(get(controller.state).openProjectIds, []);
  }
});

test("an import finishing after a user switches tabs does not steal the active tab", async () => {
  const imported = deferred<ProjectSummary>();
  const { controller } = fixture({ createProject: () => imported.promise });
  await controller.loadProjects();
  controller.setPendingFile("/tmp/c.csv");
  const creating = controller.createProject();
  await controller.selectProject("b");
  imported.resolve(summary("c"));
  await creating;
  assert.equal(get(controller.state).selectedProjectId, "b");
  assert.equal(get(controller.state).projectDetail?.project.meta.id, "b");
  assert.deepEqual(get(controller.state).openProjectIds, ["b", "c"]);
});

test("a stale summary cannot rewrite the new tab metadata", async () => {
  const { controller } = fixture();
  await controller.selectProject("b");
  controller.updateSummary({ projectId: "a", flagged: 99, iocApplied: 0, hiddenColumns: ["c1"] });
  assert.equal(get(controller.state).projectDetail?.project.meta.flagged_records, 0);
  assert.deepEqual(get(controller.state).projectDetail?.hidden_columns, []);
});

function view(search = "keep"): ViewState {
  return { version: 1, search, searchColumns: ["c1"], flagFilter: "safe", bookmarkedOnly: true,
    condition: { type: "group", id: "root", kind: "and", collapsed: false, children: [] },
    sortKey: "c1", sortDirection: "desc", hiddenColumns: ["c2"], columnWidths: { c1: 200 }, logicalRow: 299 };
}

test("tab snapshots isolate controls, history and original-ID selections without retaining row payloads", () => {
  const snapshot: WorkspaceSession = {
    projectId: "a", view: view(), selectedRowIds: [5, 89], selectionSignature: "query-a", scrollLeft: 180,
    history: { index: 0, entries: [{ condition: { type: "group", id: "root", kind: "and", collapsed: false, children: [] }, offset: 299 }] },
    writable: false,
  };
  const copied = copyWorkspaceSession({ ...snapshot, rowsCache: new Map([[5, { huge: "unused" }]]) } as WorkspaceSession);
  snapshot.view.search = "edited";
  snapshot.view.columnWidths.c1 = 900;
  snapshot.selectedRowIds.push(999);
  snapshot.history.entries[0].offset = 0;
  assert.equal(copied.view.search, "keep");
  assert.deepEqual(copied.view.columnWidths, { c1: 200 });
  assert.deepEqual(copied.selectedRowIds, [5, 89]);
  assert.equal(copied.history.entries[0].offset, 299);
  assert.equal(copied.scrollLeft, 180);
  assert.equal(copied.writable, false);
  assert.ok(!("rowsCache" in copied));
  assert.ok(!("initial_rows" in copied));
});

test("view saves stay ordered across tab instances and reopening waits for the final write", async () => {
  const oldWrite = deferred<void>();
  const calls: string[] = [];
  const backend = { saveViewState: async ({ state }: { state: ViewState }) => {
    calls.push(state.search);
    if (state.search === "old") await oldWrite.promise;
  } };
  const first = saveWorkspaceView(backend, "project-save-order", view("old"));
  const secondState = view("new");
  const second = saveWorkspaceView(backend, "project-save-order", secondState);
  secondState.search = "mutated after snapshot";
  let reopened = false;
  const reopening = waitForWorkspaceSave("project-save-order").then(() => { reopened = true; });
  await Promise.resolve();
  await Promise.resolve();
  assert.deepEqual(calls, ["old"]);
  assert.equal(reopened, false);
  oldWrite.resolve();
  await Promise.all([first, second, reopening]);
  assert.deepEqual(calls, ["old", "new"]);
  assert.equal(reopened, true);
});

test("switching or closing a loading tab cancels only its owned native load", async () => {
  const a = deferred<LoadProjectResponse>(), b = deferred<LoadProjectResponse>();
  const cancelled: string[] = [], requests: string[] = [];
  const { controller } = fixture({
    loadProject: (id, requestId) => { requests.push(requestId!); return id === "a" ? a.promise : b.promise; },
    cancelRequest: async (requestId) => { cancelled.push(requestId); return 1; },
  });
  const first = controller.selectProject("a");
  const second = controller.selectProject("b");
  assert.deepEqual(cancelled, [requests[0]]);
  controller.dispose();
  assert.deepEqual(cancelled, requests);
  a.resolve(detail("a")); b.resolve(detail("b"));
  await Promise.all([first, second]);
  assert.equal(get(controller.state).projectDetail, null);
  assert.equal(get(controller.state).isLoadingDetail, false);
  assert.deepEqual(get(controller.state).openProjectIds, []);
});
