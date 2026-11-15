import assert from "node:assert/strict";
import { test } from "node:test";
import { forgetAnnotationWrites, queueAfterAnnotationWrites, queueAnnotationWrite, waitForAnnotationWrites } from "./annotationWrites.ts";

function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));

test("closing and reopening waits for the old annotation commit while another case remains usable", async () => {
  const held = deferred();
  let flag = "", count = 0, reopened = false;
  const oldTabWrite = queueAnnotationWrite("annotation-reopen-a", async () => {
    await held.promise;
    flag = "safe";
    count = 1;
    return { flag, count };
  });
  const readAfterReopen = async (projectId: string) => {
    await waitForAnnotationWrites(projectId);
    reopened = true;
    return { flag, count };
  };
  const reopening = readAfterReopen("annotation-reopen-a");
  let independent = false;
  await waitForAnnotationWrites("annotation-reopen-b").then(() => { independent = true; });
  await turn();
  assert.equal(independent, true);
  assert.equal(reopened, false);
  assert.deepEqual({ flag, count }, { flag: "", count: 0 });
  held.resolve();
  assert.deepEqual(await reopening, { flag: "safe", count: 1 });
  assert.deepEqual(await oldTabWrite, { flag: "safe", count: 1 });
});

test("same-case mutations keep order across tab instances and a reopen includes newly queued writes", async () => {
  const first = deferred(), second = deferred();
  const changes: string[] = [];
  const old = queueAnnotationWrite("annotation-order-a", async () => { await first.promise; changes.push("flag"); });
  let ready = false;
  const reopen = waitForAnnotationWrites("annotation-order-a").then(() => { ready = true; });
  const bulk = queueAnnotationWrite("annotation-order-a", async () => { await second.promise; changes.push("bulk"); });
  await queueAnnotationWrite("annotation-order-b", async () => { changes.push("other case"); });
  first.resolve(); await turn();
  assert.deepEqual(changes, ["other case", "flag"]);
  assert.equal(ready, false);
  second.resolve();
  await Promise.all([old, bulk, reopen]);
  assert.deepEqual(changes, ["other case", "flag", "bulk"]);
  assert.equal(ready, true);
});

test("a failed edit remains rejected to its caller and cannot block the next edit or a reopened case", async () => {
  const held = deferred();
  const failed = queueAnnotationWrite("annotation-failure", () => held.promise);
  const rejected = assert.rejects(failed, /storage failed/);
  let saved = false;
  const next = queueAnnotationWrite("annotation-failure", async () => { saved = true; return "saved"; });
  const reopened = waitForAnnotationWrites("annotation-failure");
  held.reject(new Error("storage failed"));
  await Promise.all([rejected, next, reopened]);
  assert.equal(saved, true);
  await waitForAnnotationWrites("annotation-failure");
});

test("an export snapshot follows earlier writes and prevents later writes from overtaking it", async () => {
  const writeHeld = deferred(), exportHeld = deferred();
  const order: string[] = [];
  const write = queueAnnotationWrite("annotation-export-order", async () => {
    await writeHeld.promise; order.push("saved");
  });
  const snapshot = queueAfterAnnotationWrites("annotation-export-order", async () => {
    order.push("export started"); await exportHeld.promise; order.push("export finished");
  });
  const later = queueAnnotationWrite("annotation-export-order", async () => { order.push("later saved"); });
  await turn();
  assert.deepEqual(order, []);
  writeHeld.resolve(); await write; await turn();
  assert.deepEqual(order, ["saved", "export started"]);
  exportHeld.resolve();
  await Promise.all([snapshot, later]);
  assert.deepEqual(order, ["saved", "export started", "export finished", "later saved"]);
});

test("all exports requested before a save failure is acknowledged stop; a fresh retry uses confirmed state", async () => {
  const held = deferred();
  const write = queueAnnotationWrite("annotation-export-failure", () => held.promise);
  const writeRejected = assert.rejects(write, /storage failed/);
  let exports = 0;
  const exportSnapshot = () => queueAfterAnnotationWrites("annotation-export-failure", async () => ++exports);
  const first = assert.rejects(exportSnapshot(), /annotation could not be saved/);
  const second = assert.rejects(exportSnapshot(), /annotation could not be saved/);
  held.reject(new Error("storage failed"));
  await Promise.all([writeRejected, first, second]);
  assert.equal(exports, 0);
  assert.equal(await exportSnapshot(), 1);
});

test("a settled failed save survives lenient reopen waiting and an intervening successful write", async () => {
  const projectId = "annotation-export-settled-failure";
  await assert.rejects(queueAnnotationWrite(projectId, async () => { throw new Error("failed"); }));
  await waitForAnnotationWrites(projectId);
  await queueAnnotationWrite(projectId, async () => "saved");
  await assert.rejects(queueAfterAnnotationWrites(projectId, async () => assert.fail("must not export")),
    /Check the annotations before retrying the export/);
  assert.equal(await queueAfterAnnotationWrites(projectId, async () => "confirmed"), "confirmed");
});

test("failed exports do not count as annotation failures or block another case", async () => {
  await assert.rejects(queueAfterAnnotationWrites("annotation-export-bad-path", async () => {
    throw new Error("destination exists");
  }), /destination exists/);
  assert.equal(await queueAfterAnnotationWrites("annotation-export-bad-path", async () => "retried"), "retried");
  const held = deferred();
  const write = queueAnnotationWrite("annotation-export-other-pending", () => held.promise);
  assert.equal(await queueAfterAnnotationWrites("annotation-export-independent", async () => "independent"), "independent");
  held.resolve(); await write;
});

test("successful case deletion releases remembered failures", async () => {
  const projectId = "annotation-export-deleted";
  await assert.rejects(queueAnnotationWrite(projectId, async () => { throw new Error("failed"); }));
  forgetAnnotationWrites(projectId);
  assert.equal(await queueAfterAnnotationWrites(projectId, async () => "new snapshot"), "new snapshot");
});
