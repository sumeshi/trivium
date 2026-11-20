import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { createViewHistory } from "./stores/viewHistory.ts";
import type { EditorGroup } from "./stores/condition.ts";
import { createConditionState, presetCondition } from "./stores/condition.ts";

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

test("global Back and Forward restore verified group provenance so Ungroup preserves manual Rules", () => {
  const conditions = createConditionState();
  conditions.conditionTree.set(presetCondition({ kind: "or", children: [
    { column: "gate", op: "eq", values: ["keep"], negated: false },
    { column: "gate", op: "eq", values: ["other"], negated: false },
  ] })!);
  conditions.addFilterCondition("event", "eq", "1");
  const manual = structuredClone(get(conditions.conditionPayload));
  const history = createViewHistory();
  history.setHistoryProject("case");
  history.resetHistory(get(conditions.conditionTree), 0);
  const applyRestore = () => {
    const restored = get(history.historyRestore)!;
    conditions.conditionTree.set(structuredClone(restored.tree));
    conditions.restoreGroupFilter(restored.groupFilter);
  };
  conditions.filterByGroupPath(["event", "host"], [
    { label: "1", value: "1", count: 1, children: [] },
    { label: "alpha", value: "alpha", count: 1, children: [] },
  ]);
  const selected = structuredClone(get(conditions.conditionPayload));
  const provenance = conditions.snapshotGroupFilter()!;
  history.recordAppliedCondition(0, get(conditions.conditionTree), 0, provenance);
  provenance.selection.path[0].label = "changed after recording";
  history.goBack(); applyRestore();
  assert.deepEqual(get(conditions.conditionPayload), manual);
  assert.equal(get(conditions.groupSelection), null);
  history.goForward(); applyRestore();
  assert.deepEqual(get(conditions.conditionPayload), selected);
  assert.equal(get(conditions.groupSelection)?.path[0].label, "1");
  conditions.clearGroupFilter();
  assert.deepEqual(get(conditions.conditionPayload), manual, "Ungroup only removes the managed host predicate");

  history.goBack(); applyRestore();
  history.goForward(); applyRestore();
  const owned = conditions.snapshotGroupFilter()!;
  conditions.patchCondition(owned.ids[0], { value: "manually edited" });
  const edited = structuredClone(get(conditions.conditionPayload));
  history.recordAppliedCondition(0, get(conditions.conditionTree), 0, conditions.snapshotGroupFilter());
  assert.equal(history.snapshotHistory().entries.at(-1)?.groupFilter, undefined, "coalesced manual edits retire old provenance");
  history.goBack(); applyRestore();
  history.goForward(); applyRestore();
  assert.equal(get(conditions.groupSelection), null);
  conditions.clearGroupFilter();
  assert.deepEqual(get(conditions.conditionPayload), edited);
});
