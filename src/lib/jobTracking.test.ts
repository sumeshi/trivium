import { test } from "node:test";
import assert from "node:assert/strict";

import {
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
  assert.equal(jobErrorMessage(undefined), "undefined");
});
