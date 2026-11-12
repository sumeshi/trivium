import assert from "node:assert/strict";
import test from "node:test";
import { get } from "svelte/store";
import { appendEmptyColumnRule } from "./appliedFilterSummary.ts";
import { createConditionState } from "./stores/condition.ts";
import type { EditorCondition, EditorGroup } from "./stores/condition.ts";
import type { CompareOp } from "./types";

const leaf = (op: CompareOp, value = "", exact = false): EditorCondition => ({
  type: "condition", id: "leaf", column: "message", op, value, exact, negated: false,
});
const group = (children: EditorGroup["children"], kind: "and" | "or" = "and"): EditorGroup => ({
  type: "group", id: "root", kind, collapsed: false, children,
});

test("a header rule starts inactive, then narrows the existing OR expression after text entry", () => {
  const state = createConditionState();
  const first = { ...leaf("eq", "failed"), id: "first" };
  const second = { ...leaf("eq", "blocked"), id: "second" };
  state.conditionTree.set(group([first, second], "or"));
  const previous = get(state.conditionPayload);
  const added = appendEmptyColumnRule(get(state.conditionTree), "host");
  state.conditionTree.set(added.tree);
  assert.deepEqual(get(state.conditionPayload), previous);
  assert.equal(get(state.conditionTree).children.length, 2, "the editable blank rule remains in the tree");
  state.patchCondition(added.ruleId, { value: "workstation" });
  assert.deepEqual(get(state.conditionPayload), {
    kind: "and", children: [previous, { column: "host", op: "contains", values: ["workstation"], negated: false }],
  });
});

test("only actually empty text-match rules are pending; whitespace remains a searchable value", () => {
  const state = createConditionState();
  for (const op of ["contains", "startsWith", "endsWith"] as const) {
    state.conditionTree.set(group([leaf(op)]));
    assert.equal(get(state.conditionPayload), null, op);
    state.patchCondition("leaf", { value: " " });
    assert.deepEqual(get(state.conditionPayload), { column: "message", op, values: [" "], negated: false });
    state.patchCondition("leaf", { value: "", negated: true });
    assert.equal(get(state.conditionPayload), null, "negating an unfinished rule does not activate it");
  }
});

test("empty literal equality, regex, membership and ordered clauses preserve their existing payloads", () => {
  const state = createConditionState();
  for (const op of ["eq", "ne", "regex", "gt", "ge", "lt", "le"] as const) {
    state.conditionTree.set(group([leaf(op)]));
    assert.deepEqual(get(state.conditionPayload), { column: "message", op, values: [""], negated: false }, op);
  }
  state.conditionTree.set(group([leaf("in", "", true)]));
  assert.deepEqual(get(state.conditionPayload), { column: "message", op: "in", values: [""], negated: false });
  state.conditionTree.set(group([leaf("in")]));
  assert.deepEqual(get(state.conditionPayload), { column: "message", op: "in", values: [], negated: false });
});
