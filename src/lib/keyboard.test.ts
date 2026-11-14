import assert from "node:assert/strict";
import { test } from "node:test";
import { isTabKey } from "./keyboard.ts";

test("logical Tab navigation handles WebKit Shift+Tab without changing other keys", () => {
  assert.equal(isTabKey({ key: "Tab", code: "Tab" }), true);
  assert.equal(isTabKey({ key: "Unidentified", code: "Tab" }), true);
  assert.equal(isTabKey({ key: "Unidentified", code: "KeyA" }), false);
  assert.equal(isTabKey({ key: "Escape", code: "Escape" }), false);
  assert.equal(isTabKey({ key: "Escape", code: "Tab" }), false);
});
