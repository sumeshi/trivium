import assert from "node:assert/strict";
import { test } from "node:test";
import { createPointerActivationGuard } from "./recycledActivation.ts";

const origin = { projectId: "case-a", rowIndex: 43, columnId: "message" };

test("a held pointer cannot activate a recycled row, column or project", () => {
  const guard = createPointerActivationGuard();
  for (const changed of [{ ...origin, rowIndex: 115 }, { ...origin, columnId: "host" }, { ...origin, projectId: "case-b" }]) {
    guard.press(origin);
    assert.equal(guard.permits(changed, true, false), false, "context menus keep the original press identity");
    assert.equal(guard.permits(changed, true), false, "click default and child handlers must be cancelled");
  }
});

test("ordinary, keyboard and fresh pointer activations remain usable after a cancelled gesture", () => {
  const guard = createPointerActivationGuard();
  guard.press(origin);
  assert.equal(guard.permits({ ...origin }, true), true, "annotation objects may change while their identity stays the same");
  guard.press(origin);
  const next = { ...origin, rowIndex: 115 };
  assert.equal(guard.permits(next, false), true, "a stale pointer origin must not block keyboard or assistive activation");
  assert.equal(guard.permits(next, true), false);
  guard.press(next);
  assert.equal(guard.permits(next, true), true);
});

test("the press captures values rather than a mutable caller object", () => {
  const guard = createPointerActivationGuard();
  const mutable = { ...origin };
  guard.press(mutable);
  mutable.rowIndex = 115;
  assert.equal(guard.permits(mutable, true), false);
});
