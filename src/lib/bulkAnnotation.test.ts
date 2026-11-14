import assert from "node:assert/strict";
import { test } from "node:test";
import type { BulkAnnotateArgs } from "./backend";
import type { BulkCount } from "./types";
import { applyBulkAnnotation, buildBulkAnnotateArgs, buildBulkPatch } from "./bulkAnnotation.ts";

const filters = {
  search: "alpha", flag: "priority" as const, bookmarkedOnly: true,
  searchColumns: ["c000002"], condition: null,
};
const blank = { flag: "keep" as const, memo: "", clearMemo: false, tag: "", tagOperation: "add" as const };
const args = (): BulkAnnotateArgs => buildBulkAnnotateArgs({
  projectId: "p1", target: "filtered", selectedRowIds: [], filters,
  sortKey: "c000001", sortDirection: "desc",
  patch: buildBulkPatch({ ...blank, tag: "review" }),
});
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};

test("unselected flags and blank fields do not change any annotation", () => {
  assert.deepEqual(buildBulkPatch(blank), {});
  assert.deepEqual(buildBulkPatch({ ...blank, memo: "  ", tag: "  " }), {});
});

test("one bulk operation combines flag, note and one exact tag without replacing other fields", () => {
  const patch = buildBulkPatch({ ...blank, flag: "critical", memo: "reviewed", tag: " network, IOC " });
  assert.deepEqual(patch, { flag: "critical", memo: "reviewed", add_tags: ["network, IOC"] });
  assert.equal("tags" in patch, false);
  assert.equal("bookmarked" in patch, false);
});

test("tag-only updates keep existing flags and notes, and remove is one-directional", () => {
  assert.deepEqual(buildBulkPatch({ ...blank, tag: "label" }), { add_tags: ["label"] });
  assert.deepEqual(buildBulkPatch({ ...blank, tag: "label", tagOperation: "remove" }), { remove_tags: ["label"] });
});

test("clearing a flag or note is explicit and can be combined with tag removal", () => {
  assert.deepEqual(buildBulkPatch({ ...blank, flag: "", memo: "ignored", clearMemo: true, tag: "old", tagOperation: "remove" }),
    { flag: "", memo: null, remove_tags: ["old"] });
});

test("the bulk payload shares the applied query and freezes selected row IDs", () => {
  const selected = [4, 2];
  const input = { projectId: "p1", target: "selection" as const, selectedRowIds: selected, filters,
    sortKey: null, sortDirection: "asc" as const, patch: { add_tags: ["tag"] } };
  const result = buildBulkAnnotateArgs(input);
  selected.push(99);
  assert.deepEqual(result.rowIndices, [4, 2]);
  assert.deepEqual(result.searchColumns, ["c000002"]);
  assert.equal(result.bookmarkedOnly, true);
  assert.equal("previewToken" in result, false);
  assert.equal("flag" in result, false);
  assert.equal("memo" in result, false);
});

test("direct Apply internally previews and commits exactly once with its token", async () => {
  const calls: string[] = [];
  const result = await applyBulkAnnotation({
    bulkPreview: async payload => { calls.push("preview"); assert.equal(payload.projectId, "p1"); return { count: 3, previewToken: "checked" }; },
    bulkUpdate: async payload => { calls.push("apply"); assert.equal(payload.previewToken, "checked"); assert.deepEqual(payload.add_tags, ["review"]); return { updated: 3 }; },
  }, args(), { isCurrent: () => true });
  assert.deepEqual(calls, ["preview", "apply"]);
  assert.deepEqual(result, { updated: 3 });
});

test("a changed draft or target during the internal preview prevents the write", async () => {
  const waiting = deferred<BulkCount>();
  let current = true, writes = 0;
  const result = applyBulkAnnotation({
    bulkPreview: () => waiting.promise,
    bulkUpdate: async () => { writes += 1; return { updated: 3 }; },
  }, args(), { isCurrent: () => current });
  current = false;
  waiting.resolve({ count: 3, previewToken: "old-target" });
  assert.equal(await result, null);
  assert.equal(writes, 0);
});

test("an Apply snapshot cannot be retargeted by mutable inputs while preview waits", async () => {
  const waiting = deferred<BulkCount>();
  const input = args();
  input.target = "selection"; input.rowIndices = [2];
  input.condition = { column: "c000002", op: "eq", values: ["original"], negated: false };
  let written: BulkAnnotateArgs | null = null;
  const operation = applyBulkAnnotation({
    bulkPreview: () => waiting.promise,
    bulkUpdate: async payload => { written = payload; return { updated: 1 }; },
  }, input, { isCurrent: () => true });
  input.projectId = "different-case";
  input.rowIndices.push(99);
  input.searchColumns?.push("different-column");
  input.condition.values[0] = "different value";
  input.add_tags?.push("different tag");
  waiting.resolve({ count: 1, previewToken: "captured-target" });
  assert.deepEqual(await operation, { updated: 1 });
  assert.ok(written);
  assert.equal((written as BulkAnnotateArgs).projectId, "p1");
  assert.deepEqual((written as BulkAnnotateArgs).rowIndices, [2]);
  assert.deepEqual((written as BulkAnnotateArgs).searchColumns, ["c000002"]);
  assert.deepEqual((written as BulkAnnotateArgs).add_tags, ["review"]);
  assert.deepEqual((written as BulkAnnotateArgs).condition, { column: "c000002", op: "eq", values: ["original"], negated: false });
});

test("closing the form can retain its captured mutation without consulting the new case", async () => {
  const waiting = deferred<BulkCount>();
  let mounted = true, activeCase = "p1";
  const owner = "p1";
  let writtenCase = "";
  const result = applyBulkAnnotation({
    bulkPreview: () => waiting.promise,
    bulkUpdate: async payload => { writtenCase = payload.projectId; return { updated: 1 }; },
  }, args(), { isCurrent: () => !mounted || activeCase === owner });
  mounted = false; activeCase = "different-case";
  waiting.resolve({ count: 1, previewToken: "owned" });
  assert.deepEqual(await result, { updated: 1 });
  assert.equal(writtenCase, owner);
});

test("a zero-row Apply does not replace the last useful undo operation", async () => {
  let writes = 0;
  const result = await applyBulkAnnotation({
    bulkPreview: async () => ({ count: 0, previewToken: "empty" }),
    bulkUpdate: async () => { writes += 1; return { updated: 0 }; },
  }, args(), { isCurrent: () => true });
  assert.deepEqual(result, { updated: 0 });
  assert.equal(writes, 0);
});

test("an expired operation does not start either preview or apply", async () => {
  let calls = 0;
  const result = await applyBulkAnnotation({
    bulkPreview: async () => { calls += 1; return { count: 1, previewToken: "unused" }; },
    bulkUpdate: async () => { calls += 1; return { updated: 1 }; },
  }, args(), { isCurrent: () => false });
  assert.equal(result, null); assert.equal(calls, 0);
});

test("a race during Apply asks the user to apply again, with no visible preview step", async () => {
  for (const rejection of ["Target changed; preview again.", new Error("Annotations changed; preview again.")]) {
    await assert.rejects(() => applyBulkAnnotation({
      bulkPreview: async () => ({ count: 1, previewToken: "checked" }),
      bulkUpdate: async () => { throw rejection; },
    }, args(), { isCurrent: () => true }), { message: "Target changed. Apply again." });
  }
  const failure = new Error("Could not save annotations.");
  await assert.rejects(() => applyBulkAnnotation({
    bulkPreview: async () => ({ count: 1, previewToken: "checked" }),
    bulkUpdate: async () => { throw failure; },
  }, args(), { isCurrent: () => true }), error => error === failure);
});
