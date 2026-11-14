import assert from "node:assert/strict";
import { test } from "node:test";
import { createCompositionGuard, isTabKey } from "./keyboard.ts";

test("logical Tab navigation handles WebKit Shift+Tab without changing other keys", () => {
  assert.equal(isTabKey({ key: "Tab", code: "Tab" }), true);
  assert.equal(isTabKey({ key: "Unidentified", code: "Tab" }), true);
  assert.equal(isTabKey({ key: "Unidentified", code: "KeyA" }), false);
  assert.equal(isTabKey({ key: "Escape", code: "Escape" }), false);
  assert.equal(isTabKey({ key: "Escape", code: "Tab" }), false);
});

test("IME confirmation cannot become an editor shortcut on Chromium or WebKit", () => {
  const guard = createCompositionGuard();
  const key = { isComposing: false, keyCode: 13, timeStamp: 100 };
  assert.equal(guard.blocksShortcut(key), false, "ordinary Enter remains available");
  assert.equal(guard.blocksShortcut({ ...key, isComposing: true }), true);
  assert.equal(guard.blocksShortcut({ ...key, keyCode: 229 }), true);
  guard.start();
  assert.equal(guard.blocksShortcut(key), true, "composition session also covers missing browser flags");
  guard.end({ timeStamp: 100 });
  assert.equal(guard.blocksShortcut({ ...key, timeStamp: 101 }), true,
    "WebKit may end composition before it reports the confirmation Enter");
  assert.equal(guard.blocksShortcut({ ...key, timeStamp: 151 }), false,
    "a subsequent intentional shortcut works without an extra Enter");
});

test("memo and tag composition state is independent and resets with the editor", () => {
  const memo = createCompositionGuard(), tags = createCompositionGuard();
  const key = { isComposing: false, keyCode: 13, timeStamp: 100 };
  memo.start();
  assert.equal(memo.blocksShortcut(key), true);
  assert.equal(tags.blocksShortcut(key), false);
  memo.end({ timeStamp: 100 });
  memo.reset();
  assert.equal(memo.blocksShortcut(key), false);
  tags.start();
  tags.reset();
  assert.equal(tags.blocksShortcut(key), false);
});
