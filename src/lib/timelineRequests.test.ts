import assert from "node:assert/strict";
import { test } from "node:test";
import { requestTimelineHistogram } from "./timelineRequests.ts";

test("native timeline requests stay single flight across drawers and stale queued work never starts", async () => {
  let release!: () => void;
  let running = 0, maximum = 0, calls = 0;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const first = requestTimelineHistogram(() => true, async () => {
    calls += 1; running += 1; maximum = Math.max(maximum, running);
    await gate;
    running -= 1;
    return "old drawer";
  });
  let obsolete = false;
  const stale = requestTimelineHistogram(() => !obsolete, async () => { calls += 1; return "stale"; });
  const next = requestTimelineHistogram(() => true, async () => {
    calls += 1; running += 1; maximum = Math.max(maximum, running);
    running -= 1;
    return "new drawer";
  });
  obsolete = true;
  await Promise.resolve();
  assert.equal(calls, 1);
  release();
  assert.equal(await first, "old drawer");
  assert.equal(await stale, undefined);
  assert.equal(await next, "new drawer");
  assert.equal(maximum, 1);
  assert.equal(calls, 2);
});

test("a failed native timeline call releases the slot for the next valid request", async () => {
  await assert.rejects(requestTimelineHistogram(() => true, async () => { throw new Error("native failure"); }), /native failure/);
  assert.equal(await requestTimelineHistogram(() => true, async () => 42), 42);
});
