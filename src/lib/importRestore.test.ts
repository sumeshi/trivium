import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import { createBackend } from "./backend.ts";
import type { Backend, CreateProjectArgs } from "./backend.ts";
import type { LoadProjectResponse, ProjectSummary } from "./types.ts";
import { createProjectController } from "./stores/projects.ts";

function fixture() {
  const args: CreateProjectArgs[] = [];
  const summary = { meta: { id: "imported", name: "imported", source_file_name: "export.csv",
    created_at: "2026-11-15", total_records: 2, flagged_records: 1, ioc_applied_records: 0, hidden_columns: [] } } satisfies ProjectSummary;
  const detail = { project: summary, columns: [], columns_meta: [], column_max_chars: {}, column_kinds: {},
    color_rules: [], iocs: [], hidden_columns: [], initial_rows: [] } satisfies LoadProjectResponse;
  let fail = false;
  const backend: Partial<Backend> = {
    createProject: async (payload: CreateProjectArgs) => {
      args.push(payload);
      if (fail) throw new Error("import failed");
      return summary;
    },
    listProjects: async () => [summary], loadProject: async () => detail,
  };
  return { controller: createProjectController(backend as Backend), args, fail: (value: boolean) => { fail = value; } };
}

test("annotation restoration is opt-in for CSV and never carried to another selected file", async () => {
  const { controller, args } = fixture();
  assert.equal(get(controller.state).pendingRestoreAnnotations, false);
  controller.setPendingRestoreAnnotations(true);
  assert.equal(get(controller.state).pendingRestoreAnnotations, false);
  controller.setPendingFile("/source/ordinary.csv");
  await controller.createProject();
  assert.equal(args.at(-1)?.restoreAnnotations, false);
  controller.setPendingFile("C:\\export.CSV");
  controller.setPendingRestoreAnnotations(true);
  assert.equal(get(controller.state).pendingRestoreAnnotations, true);
  await controller.createProject();
  assert.equal(args.at(-1)?.restoreAnnotations, true);
  assert.equal(get(controller.state).pendingRestoreAnnotations, false);
  for (const next of ["/source/other.csv", "/source/data.json", "/source/data.jsonl", "/source/data.ndjson"]) {
    controller.setPendingFile("/source/export.csv");
    controller.setPendingRestoreAnnotations(true);
    controller.setPendingFile(next);
    assert.equal(get(controller.state).pendingRestoreAnnotations, false);
    if (!next.endsWith(".csv")) controller.setPendingRestoreAnnotations(true);
    await controller.createProject();
    assert.equal(args.at(-1)?.restoreAnnotations, false);
  }
  controller.dispose();
});

test("failed imports retain the explicit restore choice for retry; reset and dispose clear it", async () => {
  const { controller, args, fail } = fixture();
  controller.setPendingFile("/source/export.csv");
  controller.setPendingRestoreAnnotations(true);
  fail(true);
  await assert.rejects(controller.createProject(), /import failed/);
  assert.equal(get(controller.state).pendingFilePath, "/source/export.csv");
  assert.equal(get(controller.state).pendingRestoreAnnotations, true);
  fail(false);
  await controller.createProject();
  assert.equal(args.at(-1)?.restoreAnnotations, true);
  controller.setPendingFile("/source/export.csv"); controller.setPendingRestoreAnnotations(true);
  controller.resetPending();
  assert.equal(get(controller.state).pendingRestoreAnnotations, false);
  controller.setPendingFile("/source/export.csv"); controller.setPendingRestoreAnnotations(true);
  controller.dispose();
  assert.equal(get(controller.state).pendingRestoreAnnotations, false);
});

test("native project creation forwards explicit annotation restore intent and defaults it off", async () => {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { value: {}, configurable: true, writable: true });
  const writes: Array<Record<string, unknown>> = [];
  mockIPC((command, args) => {
    assert.equal(command, "create_project");
    writes.push((args as { payload: Record<string, unknown> }).payload);
    return {};
  });
  try {
    const backend = createBackend();
    for (const restoreAnnotations of [undefined, true, false]) {
      await backend.createProject({ path: "/source/export.csv", delimiter: "\t", restoreAnnotations });
      assert.equal(writes.at(-1)?.restore_annotations, restoreAnnotations === true);
      assert.equal(writes.at(-1)?.delimiter, "\t");
      assert.equal(writes.at(-1)?.path, "/source/export.csv");
    }
  } finally {
    clearMocks();
    if (previous) Object.defineProperty(globalThis, "window", previous);
    else Reflect.deleteProperty(globalThis, "window");
  }
});
