import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import { get } from "svelte/store";
import { createAppClose, installAppCloseListener } from "./appClose.ts";
import type { AppCloseOperations } from "./appClose.ts";
import { acceptSave, flushAllSaves, reopenSaveAdmission, sealSaveAdmission } from "./saveDrain.ts";
import { saveWorkspaceView } from "./workspaceSession.ts";
import type { ViewState } from "./types.ts";

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void, reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
function fixture(overrides: Partial<AppCloseOperations> = {}) {
  const calls: string[] = [];
  const close = createAppClose({
    unsavedDraft: () => null, notifyDraft: message => { calls.push(message); },
    freeze: () => { calls.push("freeze"); }, drain: async () => { calls.push("drain"); },
    flushActive: async () => { calls.push("flush"); }, seal: () => { calls.push("seal"); },
    finish: async () => { calls.push("finish"); }, cancelNative: async ack => { calls.push(`cancel:${ack}`); },
    resume: () => { calls.push("resume"); }, ...overrides,
  });
  return { close, calls };
}
afterEach(async () => { reopenSaveAdmission(); await flushAllSaves().catch(() => {}); });

test("close freezes input, drains accepted flows, saves the final view, then seals and finishes", async () => {
  const { close, calls } = fixture();
  await close.request();
  assert.deepEqual(calls, ["freeze", "drain", "flush", "drain", "seal", "finish"]);
  assert.equal(get(close.state).phase, "waiting", "UI stays frozen until the native exit occurs");
});

test("repeated X events share one attempt and a pending drain cannot be canceled unseen", async () => {
  const held = deferred(); let drained = 0;
  const { close, calls } = fixture({ drain: async () => { if (++drained === 1) await held.promise; } });
  const first = close.request();
  assert.equal(close.request(), first);
  assert.equal(close.keepOpen(), first, "no cancellation while a strict drain owns unseen failures");
  assert.deepEqual(calls, ["freeze"]);
  held.resolve(); await first;
  assert.deepEqual(calls, ["freeze", "flush", "seal", "finish"]);
  assert.equal(drained, 2);
});

test("an unsubmitted note blocks closing without writing or sealing", async () => {
  const { close, calls } = fixture({ unsavedDraft: () => "Save or cancel your note before closing." });
  await close.request();
  assert.deepEqual(calls, ["Save or cancel your note before closing."]);
  assert.equal(get(close.state).phase, "idle");
});

test("failed drains show their error before acknowledging it and require an explicit choice", async () => {
  let fail = true, finishes = 0;
  const fixtureResult = fixture({
    drain: async () => { if (fail) throw new Error("save failed"); },
    cancelNative: async ack => {
      assert.equal(get(fixtureResult.close.state).phase, ack ? "recovering" : "canceling");
      assert.equal(get(fixtureResult.close.state).message.includes("save failed") || !ack, true);
    }, finish: async () => { finishes++; },
  });
  await fixtureResult.close.request();
  assert.deepEqual(get(fixtureResult.close.state), { phase: "failed", message: "save failed" });
  await fixtureResult.close.request();
  assert.equal(finishes, 0, "another OS close does not silently consume a failed attempt");
  await fixtureResult.close.keepOpen();
  assert.equal(get(fixtureResult.close.state).phase, "idle");
  fail = false; await fixtureResult.close.request();
  assert.equal(finishes, 1);
});

test("failure recovery finishes before Retry or Keep open can start a competing attempt", async () => {
  const reset = deferred(); let cancels = 0;
  const { close } = fixture({ drain: async () => { throw new Error("failed"); },
    cancelNative: async () => { cancels++; await reset.promise; } });
  const closing = close.request(); await turn();
  assert.equal(get(close.state).phase, "recovering");
  assert.equal(close.retry(), closing);
  assert.equal(close.keepOpen(), closing);
  assert.equal(cancels, 1);
  reset.resolve(); await closing;
  assert.equal(get(close.state).phase, "failed");
});

test("strict active-view errors are drained once and repaired saves close on the next explicit attempt", async () => {
  let fail = true, finishes = 0;
  const view: ViewState = { version: 1, search: "latest", searchColumns: [], condition: {}, flagFilter: "all",
    bookmarkedOnly: false, sortKey: null, sortDirection: "asc", hiddenColumns: [], columnWidths: {}, logicalRow: 0 };
  const { close } = fixture({ drain: flushAllSaves, seal: sealSaveAdmission, resume: reopenSaveAdmission,
    flushActive: () => saveWorkspaceView({ saveViewState: async () => { if (fail) throw new Error("view failed"); } }, "closing-view", view),
    finish: async () => { finishes++; } });
  await close.request();
  assert.equal(get(close.state).phase, "failed"); assert.equal(finishes, 0);
  fail = false;
  await close.retry();
  assert.equal(finishes, 1, "the old view failure must not poison an otherwise successful retry");
});

test("a close leaves admission open for nested saves from already accepted parent flows", async () => {
  const preview = deferred(), written = deferred(); let finishes = 0;
  const parent = acceptSave(async () => { await preview.promise; await acceptSave(() => written.promise); });
  const { close } = fixture({ drain: flushAllSaves, seal: sealSaveAdmission, resume: reopenSaveAdmission,
    finish: async () => { finishes++; } });
  const closing = close.request(); preview.resolve(); await turn();
  assert.equal(finishes, 0);
  written.resolve(); await Promise.all([parent, closing]);
  assert.equal(finishes, 1);
});

test("a disposed close owner cannot exit after its old drain completes", async () => {
  const held = deferred();
  const { close, calls } = fixture({ drain: () => held.promise });
  const closing = close.request(); close.dispose(); held.resolve(); await closing;
  assert.deepEqual(calls, ["freeze"]);
});

test("startup stays inert until listener and pending-close query finish, then replays an early X", async () => {
  const pending = deferred<boolean>(); const calls: string[] = []; let event!: () => void;
  const ready = installAppCloseListener({
    listen: async handler => { event = handler; calls.push("listen"); return () => { calls.push("stop"); }; },
    pending: () => pending.promise, current: () => true,
    enable: () => { calls.push("enable"); }, request: () => { calls.push("request"); },
  });
  await turn(); event(); event(); assert.deepEqual(calls, ["listen"]);
  pending.resolve(false); const stop = await ready;
  assert.deepEqual(calls, ["listen", "enable", "request"]);
  event(); assert.equal(calls.at(-1), "request"); stop();
});

test("a native startup close pending before JS is replayed and failed readiness never enables edits", async () => {
  for (const fail of [false, true]) {
    const calls: string[] = [];
    const ready = installAppCloseListener({
      listen: async () => () => { calls.push("stop"); },
      pending: async () => { if (fail) throw new Error("native unavailable"); return true; },
      current: () => true, enable: () => { calls.push("enable"); }, request: () => { calls.push("request"); },
    });
    if (fail) { await assert.rejects(ready, /native unavailable/); assert.deepEqual(calls, ["stop"]); }
    else { const stop = await ready; assert.deepEqual(calls, ["enable", "request"]); stop(); }
  }
});

test("an unmounted startup owner removes its listener without opening admission", async () => {
  let current = true; const pending = deferred<boolean>(), calls: string[] = [];
  const ready = installAppCloseListener({
    listen: async () => () => { calls.push("stop"); }, pending: () => pending.promise, current: () => current,
    enable: () => { calls.push("enable"); }, request: () => { calls.push("request"); },
  });
  await turn(); current = false; pending.resolve(true); await ready;
  assert.deepEqual(calls, ["stop"]);
});
