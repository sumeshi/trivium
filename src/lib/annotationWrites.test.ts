import assert from "node:assert/strict";
import { test } from "node:test";
import { queueAnnotationWrite, waitForAnnotationWrites } from "./annotationWrites.ts";

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
