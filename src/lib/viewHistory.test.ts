import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { createViewHistory } from "./stores/viewHistory.ts";
import type { EditorGroup } from "./stores/condition.ts";

const empty = (): EditorGroup => ({ type: "group", id: "root", kind: "and", collapsed: false, children: [] });
const filtered = (): EditorGroup => ({ ...empty(), children: [
  { type: "condition", id: "rule", column: "c1", op: "eq", value: "evidence", negated: false },
] });

test("history captures the last scroll before snapshots, value edits and Back/Forward", () => {
  const history = createViewHistory();
  history.setHistoryProject("case");
  history.resetHistory(empty(), 0);
  history.recordAppliedCondition(17, filtered(), 0);
  for (const row of [20, 40, 73]) history.rememberOffset(row);
  assert.equal(history.snapshotHistory().entries[1].offset, 73);
  history.rememberOffset(90);
  history.recordAppliedCondition(90, filtered(), 0);
  assert.equal(history.snapshotHistory().entries.length, 2);
  assert.equal(history.snapshotHistory().entries[1].offset, 90);
  history.rememberOffset(123);
  history.goBack();
  assert.equal(get(history.historyRestore)?.offset, 17);
  history.goForward();
  assert.equal(get(history.historyRestore)?.offset, 123);
});

test("history reset and tab restore discard offsets from the previous history", () => {
  const history = createViewHistory();
  history.rememberOffset(1000);
  history.resetHistory(empty(), 7);
  assert.equal(history.snapshotHistory().entries[0].offset, 7);
  const saved = history.snapshotHistory();
  history.rememberOffset(2000);
  history.restoreHistory(saved);
  assert.deepEqual(history.snapshotHistory(), saved);
});
