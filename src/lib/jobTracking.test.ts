import { test } from "node:test";
import assert from "node:assert/strict";

import {
  isCurrentQueryJob,
  isJobLimitError,
  jobErrorMessage,
} from "./jobTracking.ts";

test("the engine concurrency-limit message is retried", () => {
  assert.equal(
    isJobLimitError("Too many long-running operations. Wait for one to finish."),
    true
  );
  assert.equal(
    isJobLimitError(new Error("Too many long-running operations. Wait.")),
    true
  );
  assert.equal(isJobLimitError("Project not found."), false);
});

test("only query jobs for the current project are tracked", () => {
  assert.equal(isCurrentQueryJob({ kind: "query", projectId: "p1" }, "p1"), true);
  assert.equal(isCurrentQueryJob({ kind: "query", projectId: "p2" }, "p1"), false);
  assert.equal(isCurrentQueryJob({ kind: "query", projectId: null }, "p1"), true);
  assert.equal(isCurrentQueryJob({ kind: "export", projectId: "p1" }, "p1"), false);
  assert.equal(jobErrorMessage(undefined), "undefined");
});
