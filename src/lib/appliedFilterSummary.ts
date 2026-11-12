import type { ConditionNode } from "./types";
import type { AppliedFilters } from "./components/project_view/state";
import type { WorkspaceController } from "./workspaceController";
import { parseFlagFilter } from "./flagFilters.ts";

const operators: Record<string, string> = {
  eq: "=", ne: "≠", gt: ">", lt: "<", ge: "≥", le: "≤",
  contains: "contains", startsWith: "starts with", endsWith: "ends with",
  regex: "matches regex", in: "is one of",
};
const flagNames: Record<string, string> = {
  safe: "Safe", suspicious: "Suspicious", critical: "Critical", none: "Unflagged",
};

/** Keep grouping and exact values intact; this describes the applied wire AST. */
export function describeCondition(node: ConditionNode | null, labels: ReadonlyMap<string, string>): string {
  if (!node) return "";
  if ("children" in node) {
    const children = node.children.map((child) => describeCondition(child, labels)).filter(Boolean);
    if (children.length === 0) return "";
    if (children.length === 1) return children[0];
    return `(${children.join(` ${node.kind.toUpperCase()} `)})`;
  }
  const column = node.column ? labels.get(node.column) ?? node.column : "Any search column";
  const values = node.values.map((value) => JSON.stringify(value));
  const value = node.op === "in" ? `[${values.join(", ")}]` : values.join(", ");
  const expression = `${column} ${operators[node.op] ?? node.op} ${value}`;
  return node.negated ? `NOT (${expression})` : expression;
}

export interface AppliedFilterDescription {
  filters: Array<{ label: string; value: string }>;
  active: boolean;
  text: string;
}

/** Search scope alone does not hide rows; all four flag selections are unrestricted. */
export function describeAppliedFilters(filters: AppliedFilters, labels: ReadonlyMap<string, string>): AppliedFilterDescription {
  const result: AppliedFilterDescription["filters"] = [];
  if (filters.search.trim()) {
    result.push({ label: "Search", value: filters.search });
    result.push({ label: "Search in", value: filters.searchColumns.length
      ? filters.searchColumns.map((id) => labels.get(id) ?? id).join(", ") : "All data columns" });
  }
  const selectedFlags = parseFlagFilter(filters.flag);
  if (selectedFlags && selectedFlags.length > 0 && selectedFlags.length < 4) {
    result.push({ label: "Flags", value: selectedFlags.map((flag) => flagNames[flag]).join(" + ") });
  }
  if (filters.bookmarkedOnly) result.push({ label: "Bookmarks", value: "Bookmarked rows only" });
  const rules = describeCondition(filters.condition, labels);
  if (rules) result.push({ label: "Rules", value: rules });
  return { filters: result, active: result.length > 0,
    text: result.length ? result.map(({ label, value }) => `${label}: ${value}`).join(" · ") : "No filters applied" };
}

/** Clear investigation filters without altering columns, sorting or evidence annotations. */
export function clearFilterInputs(workspace: Pick<WorkspaceController,
  "search" | "searchColumns" | "flagFilter" | "bookmarkedOnly" | "conditionTree">): void {
  workspace.search.set("");
  workspace.searchColumns.set([]);
  workspace.flagFilter.set("all");
  workspace.bookmarkedOnly.set(false);
  workspace.conditionTree.set({ type: "group", id: "root", kind: "and", collapsed: false, children: [] });
}

/** Adding a new rule narrows an existing OR group once the value is entered. */
export function appendEmptyColumnRule(tree: import("./stores/condition").EditorGroup, column: string): {
  tree: import("./stores/condition").EditorGroup; ruleId: string;
} {
  const ruleId = crypto.randomUUID();
  const rule: import("./stores/condition").EditorCondition = {
    type: "condition", id: ruleId, column, op: "contains", value: "", negated: false,
  };
  const existing = structuredClone(tree);
  const children = existing.kind === "or" && existing.children.length > 0
    ? [{ ...existing, id: crypto.randomUUID() }, rule] : [...existing.children, rule];
  return { tree: { ...existing, kind: "and", collapsed: false, children }, ruleId };
}
