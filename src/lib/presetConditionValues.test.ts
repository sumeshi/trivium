import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { COMPARE_OPS, conditionValues, createConditionState, presetCondition } from "./stores/condition.ts";
import type { EditorCondition } from "./stores/condition.ts";
import type { ConditionNode } from "./types.ts";

const leaf = (state: ReturnType<typeof createConditionState>) => get(state.conditionTree).children[0] as EditorCondition;

test("preset conditions preserve every operator's literal value array, including empty arrays and text", () => {
  for (const { value: op } of COMPARE_OPS) {
    for (const values of [[], [""], ["a,b"], ["a,b", " carol ", "", "日本語", "x|y"]]) {
      for (const negated of [false, true]) {
        const condition = { column: "c1", op, values, negated };
        const state = createConditionState();
        state.conditionTree.set(presetCondition(condition)!);
        assert.deepEqual(get(state.conditionPayload), condition, JSON.stringify(condition));
        assert.deepEqual(conditionValues(leaf(state)), values);
      }
    }
  }
});

test("nested AND/OR and exact values survive repeated preset saves without aliasing", () => {
  const condition: ConditionNode = { kind: "and", children: [
    { column: "c1", op: "in", values: ["alice,bob", " carol ", ""], negated: true },
    { kind: "or", children: [
      { column: "c2", op: "eq", values: ["WS01", "WS02"], negated: false },
      { column: "c3", op: "contains", values: [], negated: false },
    ] },
  ] };
  const state = createConditionState();
  state.conditionTree.set(presetCondition(condition)!);
  assert.deepEqual(get(state.conditionPayload), condition);
  state.conditionTree.set(presetCondition(JSON.parse(JSON.stringify(get(state.conditionPayload))))!);
  assert.deepEqual(get(state.conditionPayload), condition);
  const values = conditionValues(leaf(state));
  values.push("consumer mutation");
  assert.deepEqual(get(state.conditionPayload), condition);
});

test("operator, column and NOT edits keep saved values; only an explicit value edit changes interpretation", () => {
  const state = createConditionState();
  state.conditionTree.set(presetCondition({ column: "c1", op: "eq", values: ["alice,bob", " carol ", ""], negated: false })!);
  const id = leaf(state).id;
  state.patchCondition(id, { op: "in", column: "c2", negated: true });
  assert.deepEqual(get(state.conditionPayload), { column: "c2", op: "in", values: ["alice,bob", " carol ", ""], negated: true });
  state.patchCondition(id, { value: "alice,bob, carol ", exact: false });
  assert.equal(leaf(state).sourceValues, undefined);
  assert.deepEqual(get(state.conditionPayload), { column: "c2", op: "in", values: ["alice", "bob", "carol"], negated: true });
  state.patchCondition(id, { op: "eq", value: " alice,bob " });
  assert.deepEqual(get(state.conditionPayload), { column: "c2", op: "eq", values: [" alice,bob "], negated: true });
});

test("an explicitly saved blank condition is distinct from a new unfinished text rule", () => {
  const state = createConditionState();
  state.conditionTree.set(presetCondition({ column: "c1", op: "contains", values: [""], negated: false })!);
  assert.deepEqual(get(state.conditionPayload), { column: "c1", op: "contains", values: [""], negated: false });
  state.patchCondition(leaf(state).id, { value: "" });
  assert.equal(get(state.conditionPayload), null, "actually editing back to blank makes a text rule pending");
  state.addCondition("root", "c2");
  assert.equal(get(state.conditionPayload), null);
});
