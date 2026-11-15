import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import { queueAfterAnnotationWrites, queueAnnotationWrite } from "./annotationWrites.ts";
import { queueHiddenColumnWrite } from "./hiddenColumnWrites.ts";
import { saveWorkspaceView, waitForWorkspaceSave } from "./workspaceSession.ts";
import { applyBulkAnnotation } from "./bulkAnnotation.ts";
import { acceptSave, flushAllSaves, reopenSaveAdmission, sealSaveAdmission, trackAcceptedSave } from "./saveDrain.ts";
import type { ViewState } from "./types.ts";

function deferred() {
  let resolve!: () => void, reject!: (error: Error) => void;
  const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
function view(search: string): ViewState {
  return { version: 1, search, searchColumns: [], flagFilter: "all", bookmarkedOnly: false,
    condition: { type: "group", id: "root", kind: "and", collapsed: false, children: [] },
    sortKey: null, sortDirection: "asc", hiddenColumns: [], columnWidths: {}, logicalRow: 0 };
}

afterEach(async () => {
  reopenSaveAdmission();
  await flushAllSaves().catch(() => {});
});

test("global close waits annotation, columns, view, and export work from every case", async () => {
  const annotation = deferred(), columns = deferred(), workspace = deferred(), exporting = deferred();
  const saves = [
    queueAnnotationWrite("close-tab-a", () => annotation.promise),
    queueHiddenColumnWrite("close-tab-a", ["c1"], () => columns.promise),
    saveWorkspaceView({ saveViewState: () => workspace.promise }, "closed-tab-b", view("new")),
    queueAfterAnnotationWrites("closed-tab-c", () => exporting.promise),
  ];
  let ready = false;
  const closing = flushAllSaves().then(() => { ready = true; });
  annotation.resolve(); columns.resolve(); workspace.resolve();
  await turn();
  assert.equal(ready, false, "an export owned by an inactive/closed case still matters");
  exporting.resolve();
  await Promise.all([...saves, closing]);
  assert.equal(ready, true);
});

test("sealing rejects new producers while already queued saves finish in order", async () => {
  const held = deferred();
  const calls: string[] = [];
  const backend = { saveViewState: async ({ state }: { state: ViewState }) => { calls.push(state.search); } };
  const first = queueAnnotationWrite("close-accepted-a", () => held.promise);
  const second = queueAnnotationWrite("close-accepted-a", async () => { calls.push("annotation"); });
  const hidden = queueHiddenColumnWrite("close-accepted-a", [], async () => { calls.push("columns"); });
  const workspace = saveWorkspaceView(backend, "close-accepted-a", view("view"));
  sealSaveAdmission();
  const rejected = [
    queueAnnotationWrite("close-blocked", async () => assert.fail("new flag must not start")),
    queueAfterAnnotationWrites("close-blocked", async () => assert.fail("new export must not start")),
    queueHiddenColumnWrite("close-blocked", [], async () => assert.fail("new column save must not start")),
    saveWorkspaceView(backend, "close-blocked", view("blocked")),
  ].map(promise => assert.rejects(promise, /app is closing/));
  held.resolve();
  await Promise.all([first, second, hidden, workspace, ...rejected, flushAllSaves()]);
  assert.deepEqual(calls.sort(), ["annotation", "columns", "view"]);
});

test("settled failures survive caller catches and queue cleanup until a close warns", async () => {
  await assert.rejects(queueAnnotationWrite("close-old-failure", async () => { throw new Error("flag failed"); }));
  await assert.rejects(queueHiddenColumnWrite("close-old-failure", [], async () => { throw new Error("columns failed"); }));
  await assert.rejects(saveWorkspaceView({ saveViewState: async () => { throw new Error("view failed"); } },
    "close-old-failure", view("unsaved")));
  await assert.rejects(queueAfterAnnotationWrites("close-output-failure", async () => { throw new Error("disk full"); }));
  // A later successful save cannot turn this same close attempt into success.
  await queueAnnotationWrite("close-old-failure", async () => {});
  await turn();
  const first = flushAllSaves(), duplicate = flushAllSaves();
  assert.equal(first, duplicate, "duplicate close events share one attempt");
  await Promise.all([first, duplicate].map(promise => assert.rejects(promise, /save or export failed/)));
  await flushAllSaves(); // Explicit later close after reviewing the confirmed state.
});

test("a failed draining save is never silently skipped by a later successful one", async () => {
  const held = deferred();
  const failed = queueHiddenColumnWrite("close-inflight-failure", [], () => held.promise);
  const rejected = assert.rejects(failed, /storage failed/);
  const later = queueHiddenColumnWrite("close-inflight-failure", ["c2"], async () => {});
  const closing = assert.rejects(flushAllSaves(), /Check your changes before closing again/);
  held.reject(new Error("storage failed"));
  await Promise.all([rejected, later, closing]);
  reopenSaveAdmission();
  await saveWorkspaceView({ saveViewState: async () => {} }, "close-inflight-failure", view("retried"));
  await flushAllSaves();
});

test("the whole accepted bulk flow survives a close while its preview is pending", async () => {
  const preview = deferred(), update = deferred();
  const calls: string[] = [];
  const operation = applyBulkAnnotation({
    bulkPreview: async () => { calls.push("preview"); await preview.promise; return { count: 1, previewToken: "checked" }; },
    bulkUpdate: () => queueAnnotationWrite("close-bulk-parent", async () => {
      calls.push("update"); await update.promise; return { updated: 1 };
    }),
  }, { projectId: "close-bulk-parent", target: "all", flag: "critical" }, { isCurrent: () => true });
  let ready = false;
  const closing = flushAllSaves().then(() => { ready = true; });
  await turn(); assert.equal(ready, false);
  preview.resolve(); await turn();
  assert.deepEqual(calls, ["preview", "update"]);
  assert.equal(ready, false, "the nested write created after close started must be drained too");
  update.resolve();
  assert.deepEqual(await operation, { updated: 1 });
  await closing; sealSaveAdmission();
});

test("accepted direct callbacks can finish after sealing and their failures remain visible", async () => {
  const held = deferred();
  const operation = acceptSave(() => held.promise);
  const rejected = assert.rejects(operation, /direct save failed/);
  sealSaveAdmission();
  const nested = deferred();
  const acceptedCallback = trackAcceptedSave(nested.promise);
  const closing = assert.rejects(flushAllSaves(), /save or export failed/);
  held.reject(new Error("direct save failed")); nested.resolve();
  await Promise.all([rejected, acceptedCallback, closing]);
});

test("workspace revisit waits for a later accepted view save queued while an earlier save is pending", async () => {
  const old = deferred(), next = deferred();
  const first = saveWorkspaceView({ saveViewState: () => old.promise }, "close-view-revisit", view("old"));
  let ready = false;
  const revisiting = waitForWorkspaceSave("close-view-revisit").then(() => { ready = true; });
  const second = saveWorkspaceView({ saveViewState: () => next.promise }, "close-view-revisit", view("new"));
  old.resolve(); await turn();
  assert.equal(ready, false);
  next.resolve(); await Promise.all([first, second, revisiting]);
});
