import assert from "node:assert/strict";
import { test } from "node:test";

import { errorMessage } from "./errors.ts";

test("Tauri string errors are kept instead of becoming [object Object]", () => {
  assert.equal(errorMessage("Project not found."), "Project not found.");
  assert.equal(errorMessage(new Error("Disk full")), "Disk full");
  assert.equal(errorMessage(undefined, "Fallback"), "Fallback");
  assert.equal(errorMessage("", "Fallback"), "Fallback");
});
