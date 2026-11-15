import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import { get } from "svelte/store";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import { createBackend } from "./backend.ts";
import type { Backend, CreateProjectArgs } from "./backend.ts";
import { acceptSave, flushAllSaves, reopenSaveAdmission } from "./saveDrain.ts";
import { createProjectController } from "./stores/projects.ts";
import type { LoadProjectResponse, ProjectSummary } from "./types.ts";

const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => { resolve = yes; });
  return { promise, resolve };
}
function summary(id: string): ProjectSummary {
  return { meta: { id, name: id, source_file_name: id, created_at: "2026-11-15",
    total_records: 2, flagged_records: 0, ioc_applied_records: 0, hidden_columns: [] } };
}
function detail(project: ProjectSummary): LoadProjectResponse {
  return { project, columns: [], columns_meta: [], column_max_chars: {}, column_kinds: {},
    color_rules: [], iocs: [], hidden_columns: [], initial_rows: [{ row_index: 0, cells: {},
      flag: "", memo: "", tags: [], bookmarked: false }] };
}
function fixture(create: (args: CreateProjectArgs) => Promise<void> = async () => {}) {
  const projects: ProjectSummary[] = [];
  const calls: CreateProjectArgs[] = [], loaded: string[] = [];
  let lists = 0;
  const backend = {
    createProject: async (args: CreateProjectArgs) => {
      assert.ok(args.path);
      calls.push(args); await create(args);
      const project = summary(args.path); projects.push(project); return project;
    },
    listProjects: async () => { lists += 1; return [...projects]; },
    loadProject: async (id: string) => { loaded.push(id); return detail(projects.find((p) => p.meta.id === id) ?? summary(id)); },
  } as Backend;
  return { controller: createProjectController(backend), backend, projects, calls, loaded, lists: () => lists };
}

afterEach(async () => {
  reopenSaveAdmission();
  await flushAllSaves().catch(() => {});
});

test("batch imports run in selection order, isolate a middle failure, and retain only failed files for retry", async () => {
  const first = deferred(), last = deferred();
  let rejectMiddle = true;
  const f = fixture(async ({ path }) => {
    if (path === "a.csv") await first.promise;
    if (path === "bad.json" && rejectMiddle) throw "Duplicate JSON key: user";
    if (path === "c.ndjson") await last.promise;
  });
  f.controller.setPendingFiles(["a.csv", "bad.json", "c.ndjson"]);
  const importing = f.controller.createProjects();
  assert.deepEqual(f.calls.map((a) => a.path), ["a.csv"]);
  await assert.rejects(f.controller.createProjects(), /already being imported/);
  first.resolve(); await turn();
  assert.deepEqual(f.calls.map((a) => a.path), ["a.csv", "bad.json", "c.ndjson"]);
  assert.deepEqual(get(f.controller.state).openProjectIds, ["a.csv"]);
  assert.deepEqual(get(f.controller.state).importProgress, { index: 3, total: 3, fileName: "c.ndjson" });
  assert.equal(get(f.controller.state).importFailures[0].message, "Duplicate JSON key: user");
  assert.deepEqual(f.loaded, [], "intermediate imports retain summaries, not full project details");
  last.resolve();
  const result = await importing;
  assert.deepEqual(result.imported.map((p) => p.meta.id), ["a.csv", "c.ndjson"]);
  assert.deepEqual(result.failures.map((p) => p.path), ["bad.json"]);
  assert.deepEqual(get(f.controller.state).openProjectIds, ["a.csv", "c.ndjson"]);
  assert.equal(get(f.controller.state).selectedProjectId, "c.ndjson");
  assert.deepEqual(get(f.controller.state).projectDetail?.initial_rows, []);
  assert.deepEqual(f.loaded, ["c.ndjson"]);
  assert.equal(f.lists(), 1, "refresh the recent list once per batch");
  assert.deepEqual(get(f.controller.state).pendingFilePaths, ["bad.json"]);
  assert.equal(get(f.controller.state).creating, false);
  rejectMiddle = false;
  await f.controller.createProjects();
  assert.deepEqual(f.calls.map((a) => a.path), ["a.csv", "bad.json", "c.ndjson", "bad.json"]);
  assert.deepEqual(get(f.controller.state).pendingFilePaths, []);
  assert.deepEqual(get(f.controller.state).importFailures, []);
  f.controller.dispose();
});

test("one option snapshot applies CSV delimiter and annotation restore only to CSV files", async () => {
  const held = deferred();
  const f = fixture(async ({ path }) => { if (path?.endsWith("json")) await held.promise; });
  f.controller.setPendingFiles(["input.json", "C:\\export.CSV", "input.jsonl", "input.ndjson", "C:\\export.CSV"]);
  f.controller.setPendingDescription("Shared description");
  f.controller.setPendingDelimiter("\t");
  f.controller.setPendingRestoreAnnotations(true);
  const importing = f.controller.createProjects();
  f.controller.setPendingDelimiter(";");
  f.controller.setPendingDescription("Changed while busy");
  held.resolve(); await importing;
  assert.deepEqual(f.calls.map((a) => [a.path, a.delimiter, a.restoreAnnotations, a.description]), [
    ["input.json", "", false, "Shared description"],
    ["C:\\export.CSV", "\t", true, "Shared description"],
    ["input.jsonl", "", false, "Shared description"],
    ["input.ndjson", "", false, "Shared description"],
  ]);
  assert.equal(get(f.controller.state).pendingRestoreAnnotations, false);
  f.controller.dispose();
});

test("a user tab selection during a batch wins over every later imported file", async () => {
  const held = deferred();
  const f = fixture(async ({ path }) => { if (path === "a.csv") await held.promise; });
  f.projects.push(summary("existing"));
  await f.controller.loadProjects();
  f.controller.setPendingFiles(["a.csv", "b.jsonl"]);
  const importing = f.controller.createProjects();
  await f.controller.selectProject("existing");
  held.resolve(); await importing;
  assert.equal(get(f.controller.state).selectedProjectId, "existing");
  assert.deepEqual(get(f.controller.state).openProjectIds, ["existing", "a.csv", "b.jsonl"]);
  assert.deepEqual(f.loaded, ["existing"]);
  f.controller.dispose();
});

test("a project-list response started before import cannot erase the newly opened tabs", async () => {
  const stale = deferred<ProjectSummary[]>();
  const f = fixture();
  const originalList = f.backend.listProjects;
  let first = true;
  f.backend.listProjects = () => { if (first) { first = false; return stale.promise; } return originalList(); };
  const old = f.controller.loadProjects();
  f.controller.setPendingFiles(["a.csv", "b.json"]);
  await f.controller.createProjects();
  stale.resolve([]); await old;
  assert.deepEqual(get(f.controller.state).openProjectIds, ["a.csv", "b.json"]);
  assert.equal(get(f.controller.state).selectedProjectId, "b.json");
  assert.equal(get(f.controller.state).isLoadingProjects, false);
  f.controller.dispose();
});

test("a project-open failure never makes a successful source import eligible for duplicate retry", async () => {
  const f = fixture();
  f.backend.loadProject = async () => { throw new Error("Unable to load project metadata"); };
  f.controller.setPendingFiles(["a.csv", "b.json"]);
  const result = await f.controller.createProjects();
  assert.equal(result.imported.length, 2);
  assert.deepEqual(result.failures, []);
  assert.equal(result.openError, "Unable to load project metadata");
  assert.deepEqual(get(f.controller.state).pendingFilePaths, []);
  assert.deepEqual(get(f.controller.state).openProjectIds, ["a.csv", "b.json"]);
  f.controller.dispose();
});

test("disposal prevents a completed native import from reviving state or starting the next file", async () => {
  const held = deferred();
  const f = fixture(async () => held.promise);
  f.controller.setPendingFiles(["a.csv", "b.json"]);
  const importing = f.controller.createProjects();
  f.controller.dispose(); held.resolve();
  await importing;
  assert.deepEqual(f.calls.map((a) => a.path), ["a.csv"]);
  assert.deepEqual(get(f.controller.state).openProjectIds, []);
  assert.equal(get(f.controller.state).creating, false);
});

test("normal close drains the whole accepted batch through the actual native backend IPC wrapper", async () => {
  const first = deferred(), second = deferred();
  const previous = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { value: {}, configurable: true, writable: true });
  const calls: string[] = [], projects: ProjectSummary[] = [];
  mockIPC(async (command, args) => {
    if (command === "create_project") {
      const path = (args as { payload: { path: string } }).payload.path;
      calls.push(path); await (path === "a.csv" ? first.promise : second.promise);
      const imported = summary(path); projects.push(imported); return imported;
    }
    if (command === "load_project") return detail(projects.at(-1)!);
    if (command === "list_projects") return [...projects];
    throw new Error(`Unexpected IPC command: ${command}`);
  });
  try {
    const controller = createProjectController(createBackend());
    controller.setPendingFiles(["a.csv", "b.jsonl"]);
    const importing = acceptSave(() => controller.createProjects());
    let closed = false;
    const closing = flushAllSaves().then(() => { closed = true; });
    await turn(); assert.deepEqual(calls, ["a.csv"]); assert.equal(closed, false);
    first.resolve(); await turn();
    assert.deepEqual(calls, ["a.csv", "b.jsonl"]);
    assert.equal(closed, false, "the second import accepted after close began still belongs to this batch");
    second.resolve(); await Promise.all([importing, closing]);
    assert.equal(closed, true);
    assert.deepEqual(get(controller.state).openProjectIds, ["a.csv", "b.jsonl"]);
    controller.dispose();
  } finally {
    clearMocks();
    if (previous) Object.defineProperty(globalThis, "window", previous);
    else Reflect.deleteProperty(globalThis, "window");
  }
});
