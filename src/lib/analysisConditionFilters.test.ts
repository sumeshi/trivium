import assert from "node:assert/strict";
import test from "node:test";
import { get } from "svelte/store";
import { appendEmptyColumnRule } from "./appliedFilterSummary.ts";
import { appendGroupPath } from "./grouping.ts";
import { createConditionState, presetCondition } from "./stores/condition.ts";
import type { EditorCondition, EditorGroup } from "./stores/condition.ts";
import type { ConditionNode } from "./types";

const host: ConditionNode = { column: "Host", op: "eq", values: ["WS01"], negated: false };
const event: ConditionNode = { column: "EventID", op: "eq", values: ["4624"], negated: false };
const match = (column: string, value: string): ConditionNode => ({ column, op: "in", values: [value], negated: false });
const range = (column: string, start: string, end: string): ConditionNode => ({ kind: "and", children: [
  { column, op: "regex", values: ["\\S"], negated: false },
  { column, op: "ge", values: [start], negated: false },
  { column, op: "le", values: [end], negated: false },
] });
const rangeGroup = (state: ReturnType<typeof createConditionState>, index = 0) => {
  const node = get(state.conditionTree).children[index];
  assert.equal(node.type, "group");
  return node as EditorGroup;
};

test("statistics drilldowns AND the existing rules, preserving NOT and literal arrays", () => {
  const state = createConditionState();
  const existing: ConditionNode = { kind: "and", children: [host,
    { column: "message", op: "contains", values: ['a,b', '"quoted"', ""], negated: true }] };
  const original = presetCondition(existing)!;
  state.conditionTree.set(original);
  const originalSnapshot = structuredClone(original);
  const selected = 'C:\\logs\\user: alice,bob';
  state.filterByValue("path", selected);
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [...existing.children, match("path", selected)] });
  assert.deepEqual(original, originalSnapshot, "the existing editor tree is not mutated");
  assert.equal(original.children.length, 2);
});

test("statistics drilldowns wrap a complete OR expression before narrowing", () => {
  const state = createConditionState();
  const existing: ConditionNode = { kind: "or", children: [host, event] };
  state.conditionTree.set(presetCondition(existing)!);
  state.filterByValues([{ column: "user", value: "" }, { column: "command", value: "a,b" }]);
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [existing, match("user", ""), match("command", "a,b")] });
  const tree = get(state.conditionTree);
  state.filterByValues([]);
  assert.equal(get(state.conditionTree), tree, "empty drilldowns do not change rules");
});

test("timeline ranges preserve prior AND/OR rules and include a blank-value guard", () => {
  for (const kind of ["and", "or"] as const) {
    const state = createConditionState();
    const existing: ConditionNode = { kind, children: [host, event] };
    state.conditionTree.set(presetCondition(existing)!);
    state.filterByRange("Timestamp", "2026-11-14T00:00:00.000000001Z", "2026-11-14T01:00:00.000000009Z");
    assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [
      ...(kind === "and" ? existing.children : [existing]),
      range("Timestamp", "2026-11-14T00:00:00.000000001Z", "2026-11-14T01:00:00.000000009Z"),
    ] });
  }
});

test("adjusting a timeline range updates only its own group after other drilldowns and edits", () => {
  const state = createConditionState();
  state.conditionTree.set(presetCondition(host)!);
  const hostId = (get(state.conditionTree).children[0] as EditorCondition).id;
  state.filterByRange("Timestamp", "start-1", "end-1");
  state.filterByValue("EventID", "4624");
  state.conditionTree.update(root => appendGroupPath(root, ["user"], [{ value: "alice", label: "alice", count: 1, children: [] }]));
  let ruleId = "";
  state.conditionTree.update(root => {
    const added = appendEmptyColumnRule(root, "command");
    ruleId = added.ruleId;
    return added.tree;
  });
  state.patchCondition(ruleId, { value: "powershell" });
  state.patchCondition(hostId, { value: "WS02" });
  state.filterByRange("Timestamp", "start-2", "end-2");
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [
    { ...host, values: ["WS02"] }, range("Timestamp", "start-2", "end-2"), match("EventID", "4624"),
    { column: "user", op: "eq", values: ["alice"], negated: false },
    { column: "command", op: "contains", values: ["powershell"], negated: false },
  ] });
});

test("timeline and time-analysis controls adjust separate owned ranges", () => {
  const state = createConditionState();
  state.filterByRange("Timestamp", "a", "b");
  state.filterByRange("OtherTime", "c", "d", "time-analysis");
  state.filterByRange("NewTimestamp", "e", "f");
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [
    range("NewTimestamp", "e", "f"), range("OtherTime", "c", "d"),
  ] });
});

test("editing a generated range turns it into a user rule that later selections preserve", () => {
  for (const edit of [
    (state: ReturnType<typeof createConditionState>, group: EditorGroup) => state.patchCondition(group.children[1].id, { value: "user-start" }),
    (state: ReturnType<typeof createConditionState>, group: EditorGroup) => state.patchCondition(group.children[1].id, { negated: true }),
    (state: ReturnType<typeof createConditionState>, group: EditorGroup) => state.removeNode(group.children[0].id),
    (state: ReturnType<typeof createConditionState>, group: EditorGroup) => state.setGroupKind(group.id, "or"),
    (state: ReturnType<typeof createConditionState>, group: EditorGroup) => state.addCondition(group.id, "user-rule"),
  ]) {
    const state = createConditionState();
    state.filterByRange("Timestamp", "a", "b");
    const group = rangeGroup(state);
    edit(state, group);
    const edited = structuredClone(get(state.conditionTree).children[0]);
    state.filterByRange("Timestamp", "c", "d");
    assert.equal(get(state.conditionTree).children.length, 2);
    assert.deepEqual(get(state.conditionTree).children[0], edited);
    assert.deepEqual((get(state.conditionPayload) as { children: ConditionNode[] }).children.at(-1), range("Timestamp", "c", "d"));
  }
});

test("removing an owned range or changing the surrounding OR expression retires ownership", () => {
  const state = createConditionState();
  state.filterByRange("Timestamp", "a", "b");
  state.removeNode(rangeGroup(state).id);
  state.filterByRange("Timestamp", "c", "d");
  assert.deepEqual(get(state.conditionPayload), range("Timestamp", "c", "d"));
  state.filterByValue("Host", "WS01");
  state.setGroupKind("root", "or");
  const userExpression = structuredClone(get(state.conditionPayload));
  state.filterByRange("Timestamp", "e", "f");
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [userExpression, range("Timestamp", "e", "f")] });
});

test("history and preset restoration reset range ownership even when restored node IDs match", () => {
  const state = createConditionState();
  state.filterByRange("Timestamp", "a", "b");
  const saved = structuredClone(get(state.conditionTree));
  state.filterByRange("Timestamp", "c", "d");
  state.conditionTree.set(saved);
  state.filterByRange("Timestamp", "e", "f");
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [range("Timestamp", "a", "b"), range("Timestamp", "e", "f")] });
  state.conditionTree.set(presetCondition(event)!);
  state.filterByRange("Timestamp", "g", "h");
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [event, range("Timestamp", "g", "h")] });
  state.conditionTree.set({ type: "group", id: "root", kind: "and", collapsed: false, children: [] });
  state.filterByRange("Timestamp", "i", "j");
  assert.deepEqual(get(state.conditionPayload), range("Timestamp", "i", "j"));
});

test("collapse and reorder preserve ownership, and separate investigations do not share it", () => {
  const a = createConditionState(), b = createConditionState();
  a.filterByRange("Timestamp", "a", "b");
  a.filterByValue("Host", "WS01");
  const group = rangeGroup(a);
  a.toggleCollapsed(group.id);
  a.moveNode(group.id, 1);
  a.filterByRange("Timestamp", "c", "d");
  assert.deepEqual(get(a.conditionPayload), { kind: "and", children: [match("Host", "WS01"), range("Timestamp", "c", "d")] });
  b.filterByRange("Timestamp", "e", "f");
  assert.deepEqual(get(b.conditionPayload), range("Timestamp", "e", "f"));
  assert.equal(get(a.conditionTree).children.length, 2);
});
