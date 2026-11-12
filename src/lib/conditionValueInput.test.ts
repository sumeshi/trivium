import assert from "node:assert/strict";
import test from "node:test";
import { conditionValues } from "./stores/condition.ts";
import { conditionValueHelp, createConditionValueChecker, utcPickerValue } from "./conditionValueInput.ts";
import type { ConditionValueCheckResult, ConditionValueCheckState } from "./conditionValueInput.ts";
import type { EditorCondition } from "./stores/condition.ts";

const row = (patch: Partial<EditorCondition>): EditorCondition => ({
  type: "condition", id: "row", column: "ts", op: "ge", value: "", negated: false, ...patch,
});
const response = (valid: boolean): ConditionValueCheckResult => ({ valid, hint: null, values: [] });
const turn = () => new Promise<void>((resolve) => setTimeout(resolve, 5));
const deferred = () => {
  let resolve!: (value: ConditionValueCheckResult) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<ConditionValueCheckResult>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};

test("validation consumes the exact query values without rounding or splitting exact source cells", () => {
  const source = "2026-11-12T09:30:00.123456789+09:00";
  assert.deepEqual(conditionValues(row({ value: source })), [source]);
  assert.deepEqual(conditionValues(row({ value: "18446744073709551615" })), ["18446744073709551615"]);
  assert.deepEqual(conditionValues(row({ op: "in", value: "a,b", exact: true })), ["a,b"]);
  assert.deepEqual(conditionValues(row({ op: "in", value: " a, b , ,c" })), ["a", "b", "c"]);
});

test("date picker composes an explicit UTC value and text equality does not claim timestamp equivalence", () => {
  assert.equal(utcPickerValue("2026-11-12", "09:30"), "2026-11-12T09:30:00Z");
  assert.equal(utcPickerValue("2026-11-12", "09:30:45"), "2026-11-12T09:30:45Z");
  assert.equal(utcPickerValue("", "09:30"), null);
  assert.equal(utcPickerValue("2026-11-12", ""), null);
  assert.match(conditionValueHelp("datetime", "eq").hint, /Exact text comparison/);
  assert.match(conditionValueHelp("datetime", "ge").hint, /9 fractional digits/);
  assert.match(conditionValueHelp("datetime", "ge").hint, /date-only/);
  assert.match(conditionValueHelp("string", "in", true).hint, /commas are part of it/);
});

test("changing column/operator/value cannot publish an old parser response", async () => {
  const old = deferred(), current = deferred();
  const published: ConditionValueCheckState[] = [];
  const seen: string[][] = [];
  const checker = createConditionValueChecker({ delay: 0, publish: (state) => published.push(state),
    validate: (input) => { seen.push(input.values); return input.kind === "datetime" ? old.promise : current.promise; } });
  const values = ["2026-11-12T09:30:00.123456789Z"];
  checker.schedule({ kind: "datetime", op: "ge", values });
  values[0] = "caller mutated";
  await turn();
  checker.schedule({ kind: "int", op: "gt", values: ["18446744073709551615"] });
  await turn();
  current.resolve(response(true)); await turn();
  old.resolve(response(false)); await turn();
  assert.deepEqual(seen[0], ["2026-11-12T09:30:00.123456789Z"]);
  assert.deepEqual(published.filter((state) => state.status === "checked"), [{ status: "checked", result: response(true) }]);
  checker.dispose();
});

test("closing the editor prevents late failures and cancels pending debounce work", async () => {
  const held = deferred();
  const published: ConditionValueCheckState[] = [];
  let calls = 0;
  const checker = createConditionValueChecker({ delay: 0, publish: (state) => published.push(state), validate: () => { calls += 1; return held.promise; } });
  checker.schedule({ kind: "datetime", op: "ge", values: ["bad"] }); await turn();
  checker.dispose(); held.reject(new Error("late parser failure")); await turn();
  assert.equal(calls, 1);
  assert.deepEqual(published, [{ status: "checking" }]);
  checker.schedule({ kind: "int", op: "gt", values: ["1"] }); await turn();
  assert.equal(calls, 1);
});


test("rapid typing checks only the latest value and disposal cancels pending debounce", async () => {
  const seen: string[][] = [];
  const checker = createConditionValueChecker({ delay: 10, publish: () => {}, validate: async (input) => { seen.push(input.values); return response(true); } });
  checker.schedule({ kind: "datetime", op: "ge", values: ["2026-"] });
  checker.schedule({ kind: "datetime", op: "ge", values: ["2026-11-12T00:00:00.123456789Z"] });
  await new Promise((resolve) => setTimeout(resolve, 25));
  assert.deepEqual(seen, [["2026-11-12T00:00:00.123456789Z"]]);
  checker.schedule({ kind: "datetime", op: "ge", values: ["not sent after close"] });
  checker.dispose();
  await new Promise((resolve) => setTimeout(resolve, 25));
  assert.equal(seen.length, 1);
});
