import assert from "node:assert/strict";
import { test } from "node:test";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import { createBackend } from "./backend.ts";
import type { Backend } from "./backend";
import type { ViewState } from "./types";
import { saveWorkspaceView } from "./workspaceSession.ts";
import { flushAllSaves, reopenSaveAdmission, sealSaveAdmission } from "./saveDrain.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(yes => { resolve = yes; });
  return { promise, resolve };
}
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
async function withBackend(handler: Parameters<typeof mockIPC>[0], run: (backend: Backend) => Promise<void>) {
  reopenSaveAdmission();
  await flushAllSaves().catch(() => {});
  const previous = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { value: {}, configurable: true });
  mockIPC(handler);
  try { await run(createBackend()); }
  finally {
    reopenSaveAdmission();
    await flushAllSaves().catch(() => {});
    clearMocks();
    if (previous) Object.defineProperty(globalThis, "window", previous);
    else Reflect.deleteProperty(globalThis, "window");
  }
}

const directMutations: Array<{ command: string; start: (backend: Backend) => Promise<unknown> }> = [
  { command: "create_project", start: backend => backend.createProject({ path: "/synthetic/input.csv" }) },
  { command: "delete_project", start: backend => backend.deleteProject("drain-case") },
  { command: "save_iocs", start: backend => backend.saveIocs({ projectId: "drain-case", entries: [] }) },
  { command: "import_iocs", start: backend => backend.importIocs({ projectId: "drain-case", path: "/synthetic/rules.csv" }) },
  { command: "export_iocs", start: backend => backend.exportIocs({ projectId: "drain-case", destination: "/synthetic/rules.csv" }) },
  { command: "save_preset", start: backend => backend.savePreset({ projectId: "drain-case", hiddenColumns: [], destination: "/synthetic/view.json" }) },
  { command: "save_color_rules", start: backend => backend.saveColorRules({ projectId: "drain-case", rules: [] }) },
];

for (const mutation of directMutations) {
  test(`normal close awaits accepted ${mutation.command} and rejects a new request`, async () => {
    const gate = deferred();
    let calls = 0, closed = false;
    await withBackend(async command => {
      assert.equal(command, mutation.command);
      calls += 1;
      await gate.promise;
      return {};
    }, async backend => {
      const accepted = mutation.start(backend);
      sealSaveAdmission();
      const close = flushAllSaves().then(() => { closed = true; });
      try {
        await turn();
        assert.equal(calls, 1);
        assert.equal(closed, false);
        await assert.rejects(mutation.start(backend), /app is closing/);
        assert.equal(calls, 1, "new requests cannot reach native storage");
      } finally {
        gate.resolve();
        await Promise.all([accepted, close]);
      }
      assert.equal(closed, true);
    });
  });
}

test("a caught direct storage failure still prevents a normal close", async () => {
  await withBackend(async command => {
    assert.equal(command, "save_iocs");
    throw new Error("synthetic database failure");
  }, async backend => {
    await assert.rejects(backend.saveIocs({ projectId: "failed-case", entries: [] }), /database failure/);
    await assert.rejects(flushAllSaves(), /save or export failed/);
    await flushAllSaves();
  });
});

test("an accepted view queue dispatches its deferred native save after admission is sealed", async () => {
  const gate = deferred();
  let calls = 0;
  const state: ViewState = { version: 1, search: "final state", searchColumns: [],
    hiddenColumns: [], condition: null, flagFilter: "all", bookmarkedOnly: false,
    sortKey: null, sortDirection: "asc", columnWidths: {}, logicalRow: 0 };
  await withBackend(async command => { assert.equal(command, "save_view_state"); calls += 1; }, async backend => {
    const accepted = saveWorkspaceView({ saveViewState: async args => {
      await gate.promise;
      return backend.saveViewState(args);
    } }, "view-drain-case", state);
    sealSaveAdmission();
    const close = flushAllSaves();
    try {
      await turn();
      assert.equal(calls, 0);
    } finally {
      gate.resolve();
      await Promise.all([accepted, close]);
    }
    assert.equal(calls, 1);
  });
});

test("sealing save admission leaves lifecycle reads available", async () => {
  await withBackend(async command => {
    assert.ok(command === "list_projects" || command === "active_jobs");
    return [];
  }, async backend => {
    sealSaveAdmission();
    assert.deepEqual(await backend.listProjects(), []);
    assert.deepEqual(await backend.activeJobs(), []);
    await flushAllSaves();
  });
});
