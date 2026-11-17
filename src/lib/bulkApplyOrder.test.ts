import assert from "node:assert/strict";
import { test } from "node:test";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import { createBackend } from "./backend.ts";
import type { BulkAnnotateArgs, BulkUndoArgs, UpdateFlagArgs } from "./backend.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
}
const turn = () => new Promise<void>(done => setImmediate(done));

async function withBackend(handler: Parameters<typeof mockIPC>[0], run: (api: ReturnType<typeof createBackend>) => Promise<void>) {
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

test("the whole Apply follows earlier edits and precedes export and later edits, even while its preview waits", async () => {
  const first = deferred(), preview = deferred(), output = deferred();
  const order: string[] = [];
  let flag = "", revision = 0;
  await withBackend(async (command, args) => {
    const p = (args as { payload: Record<string, unknown> }).payload;
    if (p.projectId === "independent") { order.push("other case"); return { entry: { flag: "safe" }, revision: 1, flaggedRecords: 1 }; }
    assert.equal(p.projectId, "whole-apply-order");
    if (command === "update_flag") {
      if (p.flag === "safe") await first.promise;
      flag = String(p.flag); order.push(flag);
      return { entry: { flag }, revision: ++revision, flaggedRecords: 1 };
    }
    if (command === "bulk_preview") {
      assert.equal(flag, "safe"); order.push("preview"); await preview.promise;
      return { count: 1, previewToken: "after-first-save" };
    }
    if (command === "bulk_update") {
      assert.equal(p.previewToken, "after-first-save");
      flag = String(p.flag); order.push("apply"); return { updated: 1 };
    }
    assert.equal(command, "export_project");
    assert.equal(flag, "critical", "Export must include the earlier accepted Apply");
    order.push("export"); await output.promise;
    return { rows: 1, columns: [], warnings: [] };
  }, async api => {
    const before = api.updateFlag({ projectId: "whole-apply-order", rowIndex: 0, flag: "safe" });
    const apply = api.bulkApply({ projectId: "whole-apply-order", target: "all", flag: "critical" }, () => true);
    const exported = api.exportProject({ projectId: "whole-apply-order", destination: "/synthetic/result.csv" });
    const after = api.updateFlag({ projectId: "whole-apply-order", rowIndex: 0, flag: "suspicious" });
    const pending = [before, apply, exported, after];
    try {
      await api.updateFlag({ projectId: "independent", rowIndex: 0, flag: "safe" });
      assert.deepEqual(order, ["other case"]);
      first.resolve(); await before; await turn();
      assert.deepEqual(order, ["other case", "safe", "preview"]);
      preview.resolve(); assert.deepEqual(await apply, { updated: 1 }); await turn();
      assert.deepEqual(order, ["other case", "safe", "preview", "apply", "export"]);
      output.resolve(); await Promise.all(pending);
      assert.deepEqual(order, ["other case", "safe", "preview", "apply", "export", "suspicious"]);
    } finally { first.resolve(); preview.resolve(); output.resolve(); await Promise.allSettled(pending); }
  });
});

test("an Apply freezes case, selection and patch before it waits for another save", async () => {
  const held = deferred();
  const payloads: Record<string, unknown>[] = [];
  await withBackend(async (command, args) => {
    const p = (args as { payload: Record<string, unknown> }).payload;
    if (command === "update_flag") { await held.promise; return { entry: { flag: "safe" }, revision: 1, flaggedRecords: 1 }; }
    payloads.push(p);
    return command === "bulk_preview" ? { count: 1, previewToken: "captured" } : { updated: 1 };
  }, async api => {
    const first = api.updateFlag({ projectId: "apply-captured", rowIndex: 0, flag: "safe" });
    const args: BulkAnnotateArgs = { projectId: "apply-captured", target: "selection", rowIndices: [2], add_tags: ["original"],
      condition: { column: "c1", op: "eq", values: ["source"], negated: false } };
    const applying = api.bulkApply(args, () => true);
    args.projectId = "other"; args.rowIndices!.push(99); args.add_tags!.push("changed");
    (args.condition as { values: string[] }).values[0] = "changed";
    held.resolve(); await Promise.all([first, applying]);
    assert.equal(payloads.length, 2);
    for (const p of payloads) {
      assert.equal(p.projectId, "apply-captured"); assert.deepEqual(p.rowIndices, [2]);
      assert.deepEqual(p.add_tags, ["original"]);
      assert.deepEqual(p.condition, { column: "c1", op: "eq", values: ["source"], negated: false });
    }
  });
});

test("a stale queued Apply skips both native stages and does not block a subsequent export", async () => {
  const held = deferred();
  const calls: string[] = [];
  let current = true;
  await withBackend(async command => {
    calls.push(command);
    if (command === "update_flag") { await held.promise; return { entry: { flag: "safe" }, revision: 1, flaggedRecords: 1 }; }
    assert.equal(command, "export_project"); return { rows: 1, columns: [], warnings: [] };
  }, async api => {
    const before = api.updateFlag({ projectId: "stale-whole-apply", rowIndex: 0, flag: "safe" });
    const applying = api.bulkApply({ projectId: "stale-whole-apply", target: "all", flag: "critical" }, () => current);
    const output = api.exportProject({ projectId: "stale-whole-apply", destination: "/synthetic/result.csv" });
    current = false; held.resolve();
    await before; assert.equal(await applying, null); await output;
    assert.deepEqual(calls, ["update_flag", "export_project"]);
  });
});

test("a zero-row Apply retains the existing annotations and Undo before the next snapshot", async () => {
  const calls: string[] = [];
  await withBackend(command => {
    calls.push(command);
    if (command === "bulk_preview") return { count: 0, previewToken: "empty" };
    assert.equal(command, "export_project"); return { rows: 0, columns: [], warnings: [] };
  }, async api => {
    assert.deepEqual(await api.bulkApply({ projectId: "empty-whole-apply", target: "filtered", flag: "critical" }, () => true), { updated: 0 });
    await api.exportProject({ projectId: "empty-whole-apply", destination: "/synthetic/empty.csv" });
    assert.deepEqual(calls, ["bulk_preview", "export_project"]);
  });
});

test("a failed accepted Apply stops the already queued export before it can write stale annotations", async () => {
  let exports = 0;
  await withBackend(command => {
    if (command === "bulk_preview") throw new Error("Could not evaluate the selected rows.");
    if (command === "export_project") { exports++; return { rows: 1, columns: [], warnings: [] }; }
    throw new Error(`Unexpected ${command}`);
  }, async api => {
    const applying = api.bulkApply({ projectId: "failed-whole-apply", target: "all", flag: "critical" }, () => true);
    const output = api.exportProject({ projectId: "failed-whole-apply", destination: "/synthetic/result.csv" });
    await Promise.all([assert.rejects(applying, /Could not evaluate/), assert.rejects(output, /annotation could not be saved/)]);
    assert.equal(exports, 0);
    await api.exportProject({ projectId: "failed-whole-apply", destination: "/synthetic/retried.csv" });
    assert.equal(exports, 1);
  });
});

test("queued low-level annotation commands cannot be retargeted by later argument mutations", async () => {
  const held = deferred();
  const seen: Array<[string, Record<string, unknown>]> = [];
  await withBackend(async (command, args) => {
    const p = (args as { payload: Record<string, unknown> }).payload;
    if (command === "update_flag" && p.row_index === 0) { await held.promise; return { entry: { flag: "safe" }, revision: 1, flaggedRecords: 1 }; }
    seen.push([command, p]);
    return command === "update_flag" ? { entry: { memo: p.memo, tags: p.tags }, revision: 2, flaggedRecords: 1 } : { updated: 1 };
  }, async api => {
    const first = api.updateFlag({ projectId: "raw-captured", rowIndex: 0, flag: "safe" });
    const note: UpdateFlagArgs = { projectId: "raw-captured", rowIndex: 2, memo: "original", tags: ["review"] };
    const bulk: BulkAnnotateArgs = { projectId: "raw-captured", target: "selection", rowIndices: [2], add_tags: ["bulk"], previewToken: "checked" };
    const undo: BulkUndoArgs = { projectId: "raw-captured" };
    const pending = [first, api.updateFlag(note), api.bulkUpdate(bulk), api.bulkUndo(undo)];
    note.projectId = bulk.projectId = undo.projectId = "other";
    note.rowIndex = 99; note.memo = "changed"; note.tags!.push("changed"); bulk.rowIndices!.push(99); bulk.add_tags!.push("changed");
    held.resolve(); await Promise.all(pending);
    assert.deepEqual(seen.map(([command]) => command), ["update_flag", "bulk_update", "bulk_undo"]);
    assert.ok(seen.every(([, p]) => p.projectId === "raw-captured"));
    assert.equal(seen[0][1].row_index, 2); assert.equal(seen[0][1].memo, "original"); assert.deepEqual(seen[0][1].tags, ["review"]);
    assert.deepEqual(seen[1][1].rowIndices, [2]); assert.deepEqual(seen[1][1].add_tags, ["bulk"]);
  });
});
