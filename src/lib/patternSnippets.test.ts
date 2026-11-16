import assert from "node:assert/strict";
import { test } from "node:test";
import { get, writable } from "svelte/store";
import { createConditionState, presetCondition } from "./stores/condition.ts";
import type { EditorFormatCondition, EditorGroup } from "./stores/condition.ts";
import { emptyHistory, pushApplied, goBack } from "./viewState.ts";
import { savedViewFromUnknown } from "./viewStateMapping.ts";
import { describeCondition, clearFilterInputs } from "./appliedFilterSummary.ts";
import type { FormatDefinition } from "./formats.ts";
import { formatDefinitionKey, parseFormatDefinition } from "./formats.ts";
import { createWorkspaceController } from "./workspaceController.ts";
import type { LoadProjectResponse } from "./types.ts";

const ipv4: FormatDefinition = { schemaVersion: 1, id: "ipv4", name: "IPv4", pattern: "(?P<value>[0-9]+(?:\\.[0-9]+){3})", valueGroup: "value" };
const phone: FormatDefinition = { schemaVersion: 1, id: "phone-jp", name: "Phone (JP)", pattern: "0[0-9-]+" };
const ordinary = { column: "host", op: "eq" as const, values: ["WS01"], negated: false };

function fixture() {
  const columns = writable<readonly string[]>(["host", "command", "hidden"]);
  const selected = writable<readonly string[]>([]);
  return { columns, selected, state: createConditionState({ columns, searchColumns: selected }) };
}

test("search patterns use one frozen cellwise format predicate per chip and follow Search in", () => {
  const { state, selected, columns } = fixture();
  const original: EditorGroup = presetCondition({ kind: "or", children: [ordinary,
    { ...ordinary, column: "command", values: ["start"] }] })!;
  state.conditionTree.set(original);
  state.addPatternSnippet(ipv4);
  state.addPatternSnippet(phone);
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [
    { kind: "or", children: [ordinary, { ...ordinary, column: "command", values: ["start"] }] },
    { columns: ["host", "command", "hidden"], format: ipv4, negated: false },
    { columns: ["host", "command", "hidden"], format: phone, negated: false },
  ] });
  selected.set(["command", "hidden"]);
  const result = get(state.conditionPayload)!;
  assert.ok("children" in result);
  assert.deepEqual(result.children.slice(1).map(node => (node as unknown as { columns: string[] }).columns),
    [["command", "hidden"], ["command", "hidden"]]);
  // Presentation does not define scope; only actual source columns do.
  columns.set(["host", "command", "hidden", "new"]);
  selected.set([]);
  const all = get(state.conditionPayload)!;
  assert.ok("children" in all);
  assert.deepEqual((all.children[1] as unknown as { columns: string[] }).columns, ["host", "command", "hidden", "new"]);
});

test("snapshot mutation, duplicate selection and removing one pattern preserve the original rules", () => {
  const { state } = fixture();
  state.conditionTree.set(presetCondition(ordinary)!);
  const input = structuredClone(ipv4);
  state.addPatternSnippet(input);
  input.pattern = "changed";
  state.addPatternSnippet({ valueGroup: ipv4.valueGroup, pattern: ipv4.pattern, name: ipv4.name,
    id: ipv4.id, schemaVersion: 1 });
  assert.equal(get(state.patternSnippets).length, 1, "JSON field order must not duplicate a restored chip");
  assert.equal(get(state.patternSnippets)[0].format.pattern, ipv4.pattern);
  state.addPatternSnippet(phone);
  state.removePatternSnippet(get(state.patternSnippets)[0].id);
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [ordinary,
    { columns: ["host", "command", "hidden"], format: phone, negated: false }] });
  state.removePatternSnippet(get(state.patternSnippets)[0].id);
  assert.deepEqual(get(state.conditionPayload), ordinary);
});

test("drilldowns wrap an OR tree once and keep chips at the root instead of duplicating them", () => {
  const { state } = fixture();
  state.conditionTree.set(presetCondition({ kind: "or", children: [ordinary,
    { ...ordinary, column: "command" }] })!);
  state.addPatternSnippet(ipv4);
  const snippet = structuredClone(get(state.patternSnippets)[0]);
  state.filterByValue("host", "WS01");
  state.filterByFormat(["command", "hidden"], ipv4, "192.0.2.1");
  const tree = get(state.conditionTree);
  assert.deepEqual(tree.snippets, [snippet]);
  assert.equal(tree.children[0].type, "group");
  assert.equal((tree.children[0] as EditorGroup).snippets, undefined);
  const condition = tree.children[2] as EditorFormatCondition;
  assert.deepEqual(condition.columns, ["command", "hidden"]);
  assert.equal(condition.equals, "192.0.2.1");
});

test("saved views and history preserve selected pattern definitions and their removal", () => {
  const { state } = fixture();
  const before = structuredClone(get(state.conditionTree));
  const history = emptyHistory(before, 7);
  state.addPatternSnippet(ipv4);
  const after = get(state.conditionTree);
  const next = pushApplied(history, after, 7, 0);
  assert.equal(next.index, 1);
  assert.deepEqual(goBack(next)?.entries[0].condition, before);
  const saved = savedViewFromUnknown(JSON.parse(JSON.stringify({ condition: after, search: "error", searchColumns: ["command"],
    flagFilter: "all", sortKey: null, sortDirection: "asc", hiddenColumns: [], columnWidths: {}, offset: 0 })), ["host", "command", "hidden"]);
  assert.ok(saved);
  assert.deepEqual(saved.condition.snippets, after.snippets);
  assert.equal(savedViewFromUnknown({ ...saved, condition: { ...saved.condition,
    snippets: [{ id: "broken", format: { ...ipv4, schemaVersion: 9 } }] } }, ["host"]), null);
});

test("a preset restores equivalent visible multi-column format rules with exact capture matching", () => {
  const definition = structuredClone(ipv4);
  const wire = { columns: ["command", "hidden"], format: definition, equals: "192.0.2.1", negated: true };
  const restored = presetCondition(wire)!;
  const state = createConditionState();
  state.conditionTree.set(restored);
  definition.name = "edited catalog";
  assert.deepEqual(get(state.conditionPayload), { ...wire, format: ipv4 });
  assert.equal(restored.children[0].type, "format");
  assert.match(describeCondition(get(state.conditionPayload), new Map([["command", "CommandLine"]])),
    /^NOT \(CommandLine, hidden matches IPv4 = "192\.0\.2\.1"\)$/);
});

test("case workspaces get full data columns and clear filters also removes chips", () => {
  const workspace = createWorkspaceController();
  workspace.projectDetail.set({ columns: ["a", "b"], columns_meta: [] } as unknown as LoadProjectResponse);
  workspace.hiddenColumns.set(new Set(["b"]));
  workspace.addPatternSnippet(ipv4);
  assert.deepEqual(get(workspace.conditionPayload), { columns: ["a", "b"], format: ipv4, negated: false });
  workspace.searchColumns.set(["b"]);
  assert.deepEqual(get(workspace.conditionPayload), { columns: ["b"], format: ipv4, negated: false });
  clearFilterInputs(workspace);
  assert.deepEqual(get(workspace.patternSnippets), []);
  assert.equal(get(workspace.conditionPayload), null);
  workspace.dispose();
});

test("definition parsing validates structure while retaining literal expressions for Rust", () => {
  const expression = '(?P<value><alice@example.org>)';
  const parsed = parseFormatDefinition({ ...ipv4, pattern: expression });
  assert.equal(parsed.pattern, expression);
  assert.equal(formatDefinitionKey(parsed), formatDefinitionKey(JSON.parse(JSON.stringify(parsed))));
  assert.throws(() => parseFormatDefinition({ ...ipv4, classifier: { kind: "run-script" } }), /Invalid pattern/);
  assert.throws(() => parseFormatDefinition({ ...ipv4, examples: [{ text: "test", values: [42] }] }), /Invalid pattern/);
});
