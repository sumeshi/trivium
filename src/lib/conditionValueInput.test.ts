import assert from "node:assert/strict";
import test from "node:test";
import { conditionValues } from "./stores/condition.ts";
import { conditionValueHelp, createConditionValueChecker, utcPickerValue } from "./conditionValueInput.ts";
import type { ConditionValueCheckResult, ConditionValueCheckState } from "./conditionValueInput.ts";
import type { EditorCondition } from "./stores/condition.ts";
import { runSvelteTest } from "../../tests/ui/headlessSvelte.ts";

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
  assert.match(conditionValueHelp("float", "ge").hint, /decimals and exponents/);
  assert.match(conditionValueHelp("int", "lt").hint, /keep their precision/);
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

test("the value control keeps successful checks quiet, help on hover and actual errors visible", () => runSvelteTest(String.raw`
const projectDetail=writable({project:{meta:{id:'case'}},column_kinds:{timestamp:'datetime',number:'float',text:'string'}});
const calls=[];
const backend=writable({validateConditionValues:request=>new Promise((resolve,reject)=>calls.push({request,resolve,reject}))});
globalThis.__workspace={backend,projectDetail,patchCondition(){}};
const Control=await compileHeadless('ConditionValueInput');
const node=(column,op,value)=>({type:'condition',id:'rule',column,op,value,negated:false});
const control=await mountHeadless(Control,{node:node('timestamp','ge','2026-11-20T00:00:00.123456789Z')});
const flush=async()=>{await Promise.resolve();await tick();};
const wait=()=>new Promise(resolve=>setTimeout(resolve,220));
for(const [column,op,value,hint] of [
 ['timestamp','ge','2026-11-20T00:00:00.123456789Z','9 fractional digits'],
 ['number','ge','1.5','decimals and exponents'],
 ['text','regex','(?i)error','Regular expression (Rust syntax)'],
]){
 control.$set({node:node(column,op,value)});await flush();
 assert.equal(control.$capture_state().checkMessage,'','pending checks must not add a text row');
 assert.match(control.$capture_state().inputHint,/Checking format/);
 await wait();
 calls.at(-1).resolve({valid:true,hint:'Parser detail on hover.',values:[{valid:true,error:null,parsedNanos:null}]});
 await flush();
 const state=control.$capture_state();
 assert.equal(state.checkMessage,'','successful datetime, number and regex checks need no visible acknowledgement');
 assert.equal(state.invalid,false);
 assert.ok(state.inputHint.includes(hint));
 assert.match(state.inputHint,/Parser detail on hover/);
 assert.doesNotMatch(state.inputHint,/accepted\./);
}
control.$set({node:node('timestamp','ge','bad-date')});await flush();await wait();
calls.at(-1).resolve({valid:false,hint:'Use an ISO timestamp.',values:[{valid:false,error:'Invalid timestamp.',parsedNanos:null}]});
await flush();
assert.equal(control.$capture_state().checkMessage,'Invalid timestamp.');
assert.equal(control.$capture_state().invalid,true);
control.$set({node:node('text','contains','')});await flush();
assert.equal(control.$capture_state().checkMessage,'');
assert.match(control.$capture_state().inputHint,/Enter text to apply this rule/);
control.$destroy();
`));
