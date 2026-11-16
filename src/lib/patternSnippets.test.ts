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
const email: FormatDefinition = { schemaVersion: 1, id: "email", name: "Email", pattern: "[a-z]+@[a-z]+" };
const ordinary = { column: "host", op: "eq" as const, values: ["WS01"], negated: false };

test("pattern identity and saved snapshots include their optional validation", () => {
  const checked: FormatDefinition = { ...ipv4, validation: "ipv4-cidr" };
  assert.notEqual(formatDefinitionKey(checked), formatDefinitionKey(ipv4));
  assert.deepEqual(parseFormatDefinition(JSON.parse(JSON.stringify(checked))), checked);
  assert.equal(formatDefinitionKey(checked), formatDefinitionKey({ validation: "ipv4-cidr", ...ipv4 }));
  assert.throws(() => parseFormatDefinition({ ...ipv4, validation: "execute-code" }), /Invalid pattern/);
  const { state } = fixture();
  state.addPatternSnippet(ipv4);
  state.addPatternSnippet(checked);
  assert.equal(get(state.patternSnippets).length, 2);
  const tree = get(state.conditionTree);
  const saved = savedViewFromUnknown(JSON.parse(JSON.stringify({ condition: tree, search: "", searchColumns: [],
    flagFilter: "all", sortKey: null, sortDirection: "asc", hiddenColumns: [], columnWidths: {}, offset: 0 })), ["host", "command", "hidden"]);
  assert.ok(saved);
  assert.deepEqual(saved.condition.snippets, tree.snippets);
});

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
  state.addPatternSnippet(email);
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [
    { kind: "or", children: [ordinary, { ...ordinary, column: "command", values: ["start"] }] },
    { columns: ["host", "command", "hidden"], format: ipv4, negated: false },
    { columns: ["host", "command", "hidden"], format: email, negated: false },
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
  state.addPatternSnippet(email);
  state.removePatternSnippet(get(state.patternSnippets)[0].id);
  assert.deepEqual(get(state.conditionPayload), { kind: "and", children: [ordinary,
    { columns: ["host", "command", "hidden"], format: email, negated: false }] });
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
  state.filterByValue("command", "TOKEN");
  const tree = get(state.conditionTree);
  assert.deepEqual(tree.snippets, [snippet]);
  assert.equal(tree.children[0].type, "group");
  assert.equal((tree.children[0] as EditorGroup).snippets, undefined);
  const payload = get(state.conditionPayload)!;
  assert.equal("kind" in payload && payload.kind, "and");
  assert.equal(tree.children.length, 3);
  assert.ok("children" in payload);
  assert.equal(payload.children.length, 2);
  assert.deepEqual(payload.children[1], { columns: ["host", "command", "hidden"], format: ipv4, negated: false });
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
  assert.throws(() => parseFormatDefinition({ ...ipv4, valueGroup: 42 }), /Invalid pattern/);
  assert.throws(() => parseFormatDefinition({ ...ipv4, examples: [{ text: "test", values: [42] }] }), /Invalid pattern/);
});

test("older pattern metadata neither changes matching identity nor returns in a parsed definition", () => {
  const previous = { ...ipv4, classifier: { kind: "ipv4-cidr", default: "Public",
    rules: [{ cidr: "10.0.0.0/8", label: "Private" }] } };
  const parsed = parseFormatDefinition(JSON.parse(JSON.stringify(previous)));
  assert.deepEqual(parsed, ipv4);
  assert.equal(formatDefinitionKey(previous), formatDefinitionKey(ipv4));
  const { state } = fixture();
  state.addPatternSnippet(previous);
  state.addPatternSnippet(ipv4);
  assert.equal(get(state.patternSnippets).length, 1);
});
