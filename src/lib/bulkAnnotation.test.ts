import assert from "node:assert/strict";
import { test } from "node:test";
import type { BulkAnnotateArgs } from "./backend";
import type { BulkCount } from "./types";
import { applyBulkAnnotation, buildBulkAnnotateArgs, buildBulkPatch } from "./bulkAnnotation.ts";
import { runSvelteTest } from "../../tests/ui/headlessSvelte.ts";

const filters = {
  search: "alpha", flag: "suspicious|critical" as const, bookmarkedOnly: true,
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

test("clearing tags uses the explicit replacement patch without changing notes or flags", () => {
  assert.deepEqual(buildBulkPatch({ ...blank, tagOperation: "clear" }), { tags: [] });
  assert.deepEqual(buildBulkPatch({ ...blank, tag: "previous add input", tagOperation: "clear" }), { tags: [] });
  assert.deepEqual(buildBulkPatch({ ...blank, tag: "previous add input", tagOperation: "add" }), { add_tags: ["previous add input"] });
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

test("bulk operations report same-case failures after query changes without reviving stale owners", () => runSvelteTest(String.raw`
import { createWorkspaceController } from './src/lib/workspaceController.ts';
const Panel=await compileHeadless('BulkControls');
for(const operation of ['undo','apply']){
 for(const mode of ['same','query-changed','query-loading','other-case','disposed']){
  let reject;const pending=new Promise((resolve,no)=>{reject=no;});
  const workspace=createWorkspaceController();globalThis.__workspace=workspace;
  workspace.currentProjectId.set('case');workspace.projectDetail.set({project:{meta:{id:'case',total_records:1}},columns:[]});
  workspace.queryCountKnown.set(true);workspace.totalFilteredRows.set(1);
  workspace.backend.set({bulkUndo:()=>pending,bulkApply:()=>pending});
  const panel=await mountHeadless(Panel);let refreshes=0;panel.$on('refresh',()=>refreshes++);
  if(operation==='apply'){panel.$capture_state().chooseFlag('critical');await tick();}
  const running=panel.$capture_state().run(operation);
  assert.equal(panel.$capture_state().busy,true,'the accepted operation is pending');
  if(mode==='query-changed')workspace.search.set('different query');
  if(mode==='query-loading'){workspace.queryCountKnown.set(false);workspace.appliedResultLoading.set(true);}
  if(mode==='other-case')workspace.currentProjectId.set('other');
  if(mode==='disposed')workspace.dispose();
  const message='Annotation changed since the bulk operation; undo would overwrite it.';
  reject(Error(message));await running;await tick();
  const state=panel.$capture_state();
  const visible=['same','query-changed','query-loading'].includes(mode);
  assert.equal(state.error,visible?message:'',operation+'/'+mode);
  assert.equal(refreshes,0,'a failed mutation must never publish successful annotation changes');
  if(mode!=='disposed')assert.equal(state.busy,false);
  panel.$destroy();workspace.dispose();
 }
}
`));

test("an accepted bulk write failure remains visible after its target's search changes", () => runSvelteTest(String.raw`
import { createWorkspaceController } from './src/lib/workspaceController.ts';
import { createBackend } from './src/lib/backend.ts';
import { clearMocks, mockIPC } from '@tauri-apps/api/mocks';
globalThis.window={};
let rejectWrite;const pending=new Promise((resolve,reject)=>{rejectWrite=reject;});
const calls=[];
mockIPC(async command=>{
 calls.push(command);
 if(command==='bulk_preview')return {count:1,previewToken:'checked-target'};
 assert.equal(command,'bulk_update');return pending;
});
const workspace=createWorkspaceController();globalThis.__workspace=workspace;
workspace.currentProjectId.set('case');workspace.projectDetail.set({project:{meta:{id:'case',total_records:1}},columns:[]});
workspace.queryCountKnown.set(true);workspace.totalFilteredRows.set(1);workspace.backend.set(createBackend());
const Panel=await compileHeadless('BulkControls');const panel=await mountHeadless(Panel);
panel.$capture_state().chooseFlag('critical');await tick();
const applying=panel.$capture_state().run('apply');
for(let turn=0;turn<24&&!calls.includes('bulk_update');turn++)await Promise.resolve();
assert.deepEqual(calls,['bulk_preview','bulk_update'],'the backend dispatched the accepted update after its preview');
workspace.search.set('a later search');
rejectWrite(Error('synthetic database write failed'));await applying;await tick();
assert.equal(panel.$capture_state().error,'synthetic database write failed');
assert.equal(panel.$capture_state().busy,false);
assert.deepEqual(calls,['bulk_preview','bulk_update'],'failure cannot retry or retarget the write');
panel.$destroy();workspace.dispose();clearMocks();
`));
