import assert from "node:assert/strict";
import { test } from "node:test";

import { shouldRequestPanel } from "./panelRequest.ts";

test("closed panel never requests data", () => {
  assert.equal(
    shouldRequestPanel({ open: false, projectId: "p1", column: "c1" }),
    false
  );
});

test("open panel requests only when project and column are ready", () => {
  assert.equal(
    shouldRequestPanel({ open: true, projectId: "p1", column: "c1" }),
    true
  );
  assert.equal(
    shouldRequestPanel({ open: true, projectId: "", column: "c1" }),
    false
  );
  assert.equal(
    shouldRequestPanel({ open: true, projectId: "p1", column: "" }),
    false
  );
});
