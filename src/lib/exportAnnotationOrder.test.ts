import assert from "node:assert/strict";
import { test } from "node:test";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import { createBackend } from "./backend.ts";
import type { ExportProjectArgs } from "./backend.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((yes) => { resolve = yes; });
  return { promise, resolve };
}
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));

async function withBackend(
  handler: Parameters<typeof mockIPC>[0],
  run: (backend: ReturnType<typeof createBackend>) => Promise<void>,
) {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { value: {}, configurable: true, writable: true });
  mockIPC(handler);
  try { await run(createBackend()); }
  finally {
    clearMocks();
    if (previous) Object.defineProperty(globalThis, "window", previous);
    else Reflect.deleteProperty(globalThis, "window");
  }
}

for (const firstSucceeds of [true, false]) {
  for (const lastSucceeds of [true, false]) {
    test(`native CSV waits for queued annotation outcomes: first=${firstSucceeds}, last=${lastSucceeds}`, async () => {
      const gates = [deferred(), deferred()];
      let flag = "", calls = 0, exports = 0;
      const projectId = `export-order-${firstSucceeds}-${lastSucceeds}`;
      await withBackend(async (command, args) => {
        const payload = (args as { payload: Record<string, unknown> }).payload;
        if (command === "update_flag") {
          const index = calls++;
          await gates[index].promise;
          if (!(index === 0 ? firstSucceeds : lastSucceeds)) throw new Error("save failed");
          flag = String(payload.flag);
          return { entry: { flag, memo: "", tags: [], bookmarked: false },
            revision: index + 1, flaggedRecords: flag ? 1 : 0 };
        }
        assert.equal(command, "export_project");
        exports++;
        assert.equal(flag, "critical", "row annotations must reflect the final committed write");
        assert.equal(payload.flagFilter, "critical", "target resolution receives the captured filter after the saves");
        return { rows: flag === payload.flagFilter ? 1 : 0, columns: ["trivium-flag"], warnings: [] };
      }, async (backend) => {
        const first = backend.updateFlag({ projectId, rowIndex: 0, flag: "safe" });
        const last = backend.updateFlag({ projectId, rowIndex: 0, flag: "critical" });
        const writes = Promise.allSettled([first, last]);
        const output = backend.exportProject({ projectId, destination: "/chosen/result.csv", flagFilter: "critical" });
        const checked = firstSucceeds && lastSucceeds
          ? output.then(result => assert.equal(result.rows, 1))
          : assert.rejects(output, /annotation could not be saved/);
        await turn();
        assert.equal(calls, 1);
        assert.equal(exports, 0);
        gates[0].resolve(); await turn();
        assert.equal(calls, 2);
        assert.equal(exports, 0);
        gates[1].resolve();
        await Promise.all([writes, checked]);
        assert.equal(exports, Number(firstSucceeds && lastSucceeds));
      });
    });
  }
}

test("native export captures its query and destination before waiting; later writes cannot overtake it", async () => {
  const held = deferred(), exporting = deferred();
  const order: string[] = [];
  let flag = "";
  await withBackend(async (command, args) => {
    const payload = (args as { payload: Record<string, unknown> }).payload;
    if (command === "update_flag") {
      if (payload.flag === "safe") await held.promise;
      flag = String(payload.flag); order.push(flag);
      return { entry: { flag }, revision: order.length, flaggedRecords: 1 };
    }
    assert.equal(command, "export_project");
    assert.equal(flag, "safe");
    assert.equal(payload.destination, "/chosen/original.csv");
    assert.deepEqual(payload.searchColumns, ["c000001"]);
    assert.deepEqual(payload.condition, { column: "c000001", op: "eq", values: ["keep"], negated: false });
    order.push("export"); await exporting.promise;
    return { rows: 1, columns: [], warnings: [] };
  }, async (backend) => {
    const projectId = "export-captured-scope";
    const first = backend.updateFlag({ projectId, rowIndex: 0, flag: "safe" });
    const args: ExportProjectArgs = { projectId, destination: "/chosen/original.csv", searchColumns: ["c000001"],
      condition: { column: "c000001", op: "eq", values: ["keep"], negated: false } };
    const output = backend.exportProject(args);
    const last = backend.updateFlag({ projectId, rowIndex: 0, flag: "critical" });
    args.destination = "/changed.csv"; args.searchColumns![0] = "c000002";
    (args.condition as { values: string[] }).values[0] = "changed";
    held.resolve(); await first; await turn();
    assert.deepEqual(order, ["safe", "export"]);
    exporting.resolve(); await Promise.all([output, last]);
    assert.deepEqual(order, ["safe", "export", "critical"]);
  });
});

test("native export rejects an already settled annotation failure and permits a later explicit retry", async () => {
  let saves = 0, exports = 0;
  await withBackend((command) => {
    if (command === "update_flag") {
      if (saves++ === 0) throw new Error("save failed");
      return { entry: { flag: "critical" }, revision: 1, flaggedRecords: 1 };
    }
    assert.equal(command, "export_project"); exports++;
    return { rows: 1, columns: [], warnings: [] };
  }, async (backend) => {
    const projectId = "export-settled-failure";
    await assert.rejects(backend.updateFlag({ projectId, rowIndex: 0, flag: "safe" }));
    await backend.updateFlag({ projectId, rowIndex: 0, flag: "critical" });
    const args = { projectId, destination: "/chosen/retry.csv" };
    await assert.rejects(backend.exportProject(args), /Check the annotations before retrying the export/);
    assert.equal(exports, 0);
    assert.equal((await backend.exportProject(args)).rows, 1);
    assert.equal(exports, 1);
  });
});

test("native case deletion clears remembered failures only after a successful delete", async () => {
  let deleteSucceeds = false, exports = 0;
  await withBackend((command) => {
    if (command === "update_flag") throw new Error("save failed");
    if (command === "delete_project") {
      if (!deleteSucceeds) throw new Error("delete failed");
      return;
    }
    assert.equal(command, "export_project"); exports++;
    return { rows: 0, columns: [], warnings: [] };
  }, async (backend) => {
    const projectId = "export-deleted-failure";
    const update = () => backend.updateFlag({ projectId, rowIndex: 0, flag: "safe" });
    const args = { projectId, destination: "/chosen/deleted.csv" };
    await assert.rejects(update());
    await assert.rejects(backend.deleteProject(projectId), /delete failed/);
    await assert.rejects(backend.exportProject(args), /annotation could not be saved/);
    assert.equal(exports, 0);
    await assert.rejects(update());
    deleteSucceeds = true;
    await backend.deleteProject(projectId);
    await backend.exportProject(args);
    assert.equal(exports, 1);
  });
});
