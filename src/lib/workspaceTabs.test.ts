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

test("saved-case discovery failures remain visible until a successful retry without losing open tabs", async () => {
  let failed = true;
  const { controller, projects } = fixture({ listProjects: async () => {
    if (failed) throw new Error("Cannot read saved case; its files were preserved.");
    return [summary("a"), summary("b"), summary("c")];
  } });
  await assert.rejects(controller.loadProjects(), /Cannot read saved case/);
  assert.match(get(controller.state).projectsError, /files were preserved/);
  assert.equal(get(controller.state).projectsLoaded, false);
  assert.equal(get(controller.state).isLoadingProjects, false);
  failed = false;
  await controller.loadProjects();
  assert.equal(get(controller.state).projectsError, "");
  assert.deepEqual(get(controller.state).projects, projects);
  await controller.selectProject("a");
  failed = true;
  await assert.rejects(controller.loadProjects(true), /Cannot read saved case/);
  assert.deepEqual(get(controller.state).projects, projects);
  assert.deepEqual(get(controller.state).openProjectIds, ["a"]);
  assert.equal(get(controller.state).projectDetail?.project.meta.id, "a");
  assert.match(get(controller.state).projectsError, /files were preserved/);
  controller.dispose();
});

test("a late discovery failure cannot replace a newer successful project list", async () => {
  const old = deferred<ProjectSummary[]>();
  let calls = 0;
  const { controller, projects } = fixture({ listProjects: () => ++calls === 1 ? old.promise : Promise.resolve([summary("a"), summary("b"), summary("c")]) });
  const first = controller.loadProjects();
  await controller.loadProjects(true);
  old.reject(new Error("stale discovery failure"));
  await first;
  assert.equal(get(controller.state).projectsError, "");
  assert.equal(get(controller.state).projectsLoaded, true);
  assert.deepEqual(get(controller.state).projects, projects);
  controller.dispose();
});

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
  controller.setPendingFiles(["/tmp/c.csv"]);
  const creating = controller.createProjects();
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

test("deleting an active case closes its tab and settles teardown before native deletion", async () => {
  const teardown = deferred<void>();
  let deleted = false, enteredHook = false;
  const f = fixture({ deleteProject: async id => {
    assert.equal(id, "a");
    assert.equal(enteredHook, true);
    assert.equal(get(f.controller.state).selectedProjectId, "b");
    assert.ok(!get(f.controller.state).openProjectIds.includes("a"));
    deleted = true;
    f.projects.splice(f.projects.findIndex(project => project.meta.id === id), 1);
  } });
  await f.controller.loadProjects();
  await f.controller.selectProject("a");
  await f.controller.selectProject("b");
  await f.controller.selectProject("a");
  const deleting = f.controller.deleteProject("a", async () => {
    enteredHook = true;
    assert.equal(get(f.controller.state).selectedProjectId, "b");
    await teardown.promise;
  });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(deleted, false);
  assert.equal(enteredHook, true);
  await assert.rejects(f.controller.selectProject("a"), /being deleted/);
  await assert.rejects(f.controller.deleteProject("a"), /already being deleted/);
  teardown.resolve(); await deleting;
  assert.equal(deleted, true);
  assert.deepEqual(get(f.controller.state).openProjectIds, ["b"]);
  assert.equal(get(f.controller.state).selectedProjectId, "b");
  assert.ok(!get(f.controller.state).projects.some(project => project.meta.id === "a"));
  f.controller.dispose();
});

test("a failed deletion restores its open tab, view and selection without deleting saved metadata", async () => {
  const f = fixture({ deleteProject: async () => { throw new Error("could not move saved case"); } });
  await f.controller.loadProjects();
  await f.controller.selectProject("a");
  await f.controller.selectProject("b");
  await f.controller.selectProject("a");
  await assert.rejects(f.controller.deleteProject("a", async () => {
    assert.deepEqual(get(f.controller.state).openProjectIds, ["b"]);
  }), /could not move saved case/);
  assert.deepEqual(get(f.controller.state).openProjectIds, ["a", "b"]);
  assert.equal(get(f.controller.state).selectedProjectId, "a");
  assert.equal(get(f.controller.state).projectDetail?.project.meta.id, "a");
  assert.equal(get(f.controller.state).projects.length, 3);
  await f.controller.selectProject("a"); // Deletion admission was released after failure.
  f.controller.dispose();
});

for (const active of [false, true]) {
  test(`a failed ${active ? "active" : "inactive"} case deletion does not steal a later tab selection`, async () => {
    const deletion = deferred<void>();
    const f = fixture({ deleteProject: () => deletion.promise });
    await f.controller.loadProjects();
    await f.controller.selectProject("a");
    await f.controller.selectProject("b");
    if (active) await f.controller.selectProject("a");
    const deleting = f.controller.deleteProject("a");
    const checked = assert.rejects(deleting, /delete failed/);
    await new Promise(resolve => setImmediate(resolve));
    await f.controller.selectProject("c");
    deletion.reject(new Error("delete failed")); await checked;
    assert.deepEqual(get(f.controller.state).openProjectIds, ["a", "b", "c"]);
    assert.equal(get(f.controller.state).selectedProjectId, "c");
    assert.equal(get(f.controller.state).projectDetail?.project.meta.id, "c");
    f.controller.dispose();
  });
}

test("failed teardown aborts deletion and restores the saved case's tab", async () => {
  const f = fixture();
  await f.controller.loadProjects();
  await f.controller.selectProject("a");
  await assert.rejects(f.controller.deleteProject("a", async () => { throw new Error("teardown failed"); }), /teardown failed/);
  assert.equal(f.deleted(), 0);
  assert.deepEqual(get(f.controller.state).openProjectIds, ["a"]);
  assert.equal(get(f.controller.state).selectedProjectId, "a");
  assert.equal(get(f.controller.state).projectDetail?.project.meta.id, "a");
  f.controller.dispose();
});

test("an unreadable adjacent case does not prevent deleting the closed target and returns a warning", async () => {
  let failAdjacent = false, nativeDeletes = 0, teardownSettled = false;
  const f = fixture({
    loadProject: async id => {
      if (id === "b" && failAdjacent) throw new Error("adjacent project unreadable");
      return detail(id);
    },
    deleteProject: async id => {
      assert.equal(id, "a");
      assert.equal(teardownSettled, true);
      nativeDeletes += 1;
      f.projects.splice(f.projects.findIndex(project => project.meta.id === id), 1);
    },
  });
  await f.controller.loadProjects();
  await f.controller.selectProject("a");
  await f.controller.selectProject("b");
  await f.controller.selectProject("a");
  failAdjacent = true;
  const result = await f.controller.deleteProject("a", async () => {
    assert.deepEqual(get(f.controller.state).openProjectIds, ["b"]);
    teardownSettled = true;
  });
  assert.deepEqual(result, { adjacentLoadFailed: true });
  assert.equal(nativeDeletes, 1);
  assert.ok(!get(f.controller.state).projects.some(project => project.meta.id === "a"));
  assert.equal(get(f.controller.state).selectedProjectId, "b");
  assert.equal(get(f.controller.state).projectDetail, null);
  assert.equal(get(f.controller.state).isLoadingDetail, false);
  failAdjacent = false;
  await f.controller.selectProject("b");
  assert.equal(get(f.controller.state).projectDetail?.project.meta.id, "b", "the adjacent saved case remains reopenable");
  f.controller.dispose();
});
