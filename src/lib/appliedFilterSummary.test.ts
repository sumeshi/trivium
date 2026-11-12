import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";
import { describeCondition, describeAppliedFilters, clearFilterInputs, appendEmptyColumnRule } from "./appliedFilterSummary.ts";
import { createWorkspaceController } from "./workspaceController.ts";
import { normalizeRow } from "./components/project_view/state.ts";
import type { AppliedFilters } from "./components/project_view/state.ts";
import type { ConditionNode } from "./types.ts";
import type { EditorGroup } from "./stores/condition.ts";

const labels = new Map([["c1", "Host"], ["c2", "Timestamp"], ["c3", "Message"]]);
const base = (): AppliedFilters => ({ search: "", flag: "all", bookmarkedOnly: false, searchColumns: [], condition: null });

test("summary preserves nested AND/OR, negation, label mappings and exact timestamp/value text", () => {
  const condition: ConditionNode = { kind: "and", children: [
    { kind: "or", children: [
      { column: "c1", op: "eq", values: ["server-a"], negated: false },
      { column: "c3", op: "in", values: ["a,b", "line\nvalue"], negated: true },
    ] },
    { column: "c2", op: "ge", values: ["2026-11-12T09:00:00.123456789+09:00"], negated: false },
  ] };
  assert.equal(describeCondition(condition, labels),
    '((Host = "server-a" OR NOT (Message is one of ["a,b", "line\\nvalue"])) AND Timestamp ≥ "2026-11-12T09:00:00.123456789+09:00")');
  const summary = describeAppliedFilters({ ...base(), search: 'error | "failed login"', searchColumns: ["c3"],
    condition, flag: "critical|none", bookmarkedOnly: true }, labels);
  assert.equal(summary.active, true);
  assert.deepEqual(summary.filters.map((part) => part.label), ["Search", "Search in", "Flags", "Bookmarks", "Rules"]);
  assert.equal(summary.filters[1].value, "Message");
  assert.equal(summary.filters[2].value, "Critical + Unflagged");
  assert.match(summary.text, /123456789\+09:00/);
});

test("all flag populations and a column scope without text do not falsely report filtered rows", () => {
  const scopeOnly = describeAppliedFilters({ ...base(), searchColumns: ["c1"] }, labels);
  assert.equal(scopeOnly.active, false);
  assert.equal(scopeOnly.text, "No filters applied");
  const allFlags = describeAppliedFilters({ ...base(), flag: "safe|suspicious|critical|none",
    condition: { kind: "and", children: [] } }, labels);
  assert.equal(allFlags.active, false);
  assert.equal(describeAppliedFilters({ ...base(), flag: "priority" }, labels).filters[0].value, "Suspicious + Critical");
});

test("applied status remains the requested snapshot while live inputs and editor change", () => {
  const workspace = createWorkspaceController();
  workspace.appliedFilters.set({ ...base(), search: "applied", flag: "safe" });
  workspace.search.set("draft edit");
  workspace.flagFilter.set("critical");
  workspace.filterByValue("c1", "unapplied-value");
  const description = describeAppliedFilters(get(workspace.appliedFilters), labels);
  assert.match(description.text, /Search: applied/);
  assert.match(description.text, /Flags: Safe/);
  assert.doesNotMatch(description.text, /draft edit|Critical|unapplied-value/);
  workspace.dispose();
});

test("clear resets every filter input and preserves visible columns, sort, annotations and applied snapshot until query dispatch", () => {
  const workspace = createWorkspaceController();
  workspace.search.set("old search"); workspace.searchColumns.set(["c1"]);
  workspace.flagFilter.set("critical"); workspace.bookmarkedOnly.set(true);
  workspace.filterByValue("c1", "evidence");
  workspace.hiddenColumns.set(new Set(["c2"])); workspace.sortKey.set("c1"); workspace.sortDirection.set("desc");
  const row = normalizeRow({ row_index: 9, cells: { c1: { kind: "string", text: "evidence" } },
    flag: "critical", memo: "keep note", tags: ["reviewed"], bookmarked: true });
  workspace.registerLoadedPage(0, 0, [row]);
  workspace.appliedFilters.set({ ...base(), search: "old search", flag: "critical", condition: get(workspace.conditionPayload) });
  const appliedBefore = structuredClone(get(workspace.appliedFilters));
  clearFilterInputs(workspace);
  assert.deepEqual({ search: get(workspace.search), searchColumns: get(workspace.searchColumns), flag: get(workspace.flagFilter),
    bookmark: get(workspace.bookmarkedOnly), condition: get(workspace.conditionPayload) },
  { search: "", searchColumns: [], flag: "all", bookmark: false, condition: null });
  assert.deepEqual(get(workspace.appliedFilters), appliedBefore);
  assert.deepEqual([...get(workspace.hiddenColumns)], ["c2"]);
  assert.equal(get(workspace.sortKey), "c1"); assert.equal(get(workspace.sortDirection), "desc");
  assert.deepEqual(get(workspace.rowsCache).get(9), row);
  workspace.dispose();
});

test("header-created rules preserve the old OR subtree, immutable source and unique editor IDs", () => {
  const tree: EditorGroup = { type: "group", id: "root", kind: "or", collapsed: true, children: [
    { type: "condition", id: "a", column: "c1", op: "eq", value: "a", negated: false },
    { type: "condition", id: "b", column: "c1", op: "eq", value: "b", negated: false },
  ] };
  const original = structuredClone(tree);
  const added = appendEmptyColumnRule(tree, "c3");
  assert.deepEqual(tree, original);
  assert.equal(added.tree.kind, "and"); assert.equal(added.tree.collapsed, false);
  const previous = added.tree.children[0];
  assert.equal(previous.type, "group");
  if (previous.type !== "group") throw new Error("missing previous OR group");
  assert.equal(previous.kind, "or"); assert.deepEqual(previous.children, tree.children);
  const next = added.tree.children[1];
  assert.deepEqual(next, { type: "condition", id: added.ruleId, column: "c3", op: "contains", value: "", negated: false });
  assert.equal(new Set([added.tree.id, previous.id, "a", "b", next.id]).size, 5);
});
