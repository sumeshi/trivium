import assert from "node:assert/strict";
import test from "node:test";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import { createBackend } from "./backend.ts";

test("output IPC requires explicit overwrite approval for CSV, presets and IOC rules", async () => {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { value: {}, configurable: true, writable: true });
  const writes: Array<{ command: string; payload: Record<string, unknown> }> = [];
  mockIPC((command, args) => {
    writes.push({ command, payload: (args as { payload: Record<string, unknown> }).payload });
    return command === "export_project" ? { rows: 0, columns: [], warnings: [] } : undefined;
  });
  try {
    const backend = createBackend();
    for (const allowOverwrite of [undefined, true, false]) {
      const common = { projectId: "captured-case", destination: "/chosen/output.csv", allowOverwrite };
      await backend.exportProject(common);
      await backend.savePreset({ ...common, hiddenColumns: [] });
      await backend.exportIocs(common);
      for (const write of writes.slice(-3)) {
        assert.equal(write.payload.projectId, common.projectId);
        assert.equal(write.payload.destination, common.destination);
        assert.equal(write.payload.allowOverwrite, allowOverwrite === true);
      }
    }
    assert.deepEqual(writes.map(write => write.command), Array(3).fill([
      "export_project", "save_preset", "export_iocs",
    ]).flat());
  } finally {
    clearMocks();
    if (previous) Object.defineProperty(globalThis, "window", previous);
    else Reflect.deleteProperty(globalThis, "window");
  }
});
