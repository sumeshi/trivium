import assert from "node:assert/strict";
import { test } from "node:test";
import { queueHiddenColumnWrite, waitForHiddenColumnWrites } from "./hiddenColumnWrites.ts";

function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
}

const turn = () => new Promise<void>((resolve) => setImmediate(resolve));

test("A → B → A hidden-column writes retain user order across component instances", async () => {
  const first = deferred();
  const last = deferred();
  const started: string[][] = [];
  let saved: string[] = [];
  const oldComponent = (columns: string[]) => queueHiddenColumnWrite("switch-a", columns, async (snapshot) => {
    started.push(snapshot);
    if (started.length === 1) await first.promise;
    saved = snapshot;
  });
  const initial = ["c1"];
  const write1 = oldComponent(initial);
  const write2 = oldComponent(["c1", "c2"]);
  initial.push("mutated after scheduling");
  const write3 = queueHiddenColumnWrite("switch-a", ["c2"], async (snapshot) => {
    started.push(snapshot);
    await last.promise;
    saved = snapshot;
  });
  let bSaved = false;
  await queueHiddenColumnWrite("switch-b", [], async () => { bSaved = true; });
  assert.ok(bSaved, "another project must not wait for A");
  assert.deepEqual(started, [["c1"]], "later A saves must wait for the old instance");
  let revisitReady = false;
  const revisit = waitForHiddenColumnWrites("switch-a").then(() => { revisitReady = true; });
  first.resolve();
  await turn();
  assert.deepEqual(started, [["c1"], ["c1", "c2"], ["c2"]]);
  assert.equal(revisitReady, false, "metadata must not load before the newest save");
  last.resolve();
  await Promise.all([write1, write2, write3, revisit]);
  assert.deepEqual(saved, ["c2"], "the newest component's choice must remain on disk");
});

test("a failed old save is reported without blocking a newer choice or revisit", async () => {
  const held = deferred();
  const failed = queueHiddenColumnWrite("failure-a", ["c1"], () => held.promise);
  const rejected = assert.rejects(failed, /write failed/);
  let saved: string[] = [];
  const next = queueHiddenColumnWrite("failure-a", ["c3"], async (snapshot) => { saved = snapshot; });
  const revisit = waitForHiddenColumnWrites("failure-a");
  held.reject(new Error("write failed"));
  await Promise.all([rejected, next, revisit]);
  assert.deepEqual(saved, ["c3"]);
  await waitForHiddenColumnWrites("no-pending-project");
});

test("a revisit also waits for another change registered while its earlier save is pending", async () => {
  const first = deferred();
  const second = deferred();
  const write1 = queueHiddenColumnWrite("revisit-a", ["c1"], () => first.promise);
  let ready = false;
  const revisit = waitForHiddenColumnWrites("revisit-a").then(() => { ready = true; });
  const write2 = queueHiddenColumnWrite("revisit-a", ["c2"], () => second.promise);
  first.resolve();
  await turn();
  assert.equal(ready, false);
  second.resolve();
  await Promise.all([write1, write2, revisit]);
  assert.equal(ready, true);
});
