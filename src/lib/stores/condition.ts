import { derived, writable } from "svelte/store";
import type { CompareOp, ConditionNode } from "../types";
import {
  appendFilterCondition,
  type FilterConditionOp,
} from "../conditionFilters";

export const COMPARE_OPS: Array<{ value: CompareOp; label: string }> = [
  { value: "eq", label: "=" },
  { value: "ne", label: "≠" },
  { value: "gt", label: ">" },
  { value: "lt", label: "<" },
  { value: "ge", label: "≥" },
  { value: "le", label: "≤" },
  { value: "contains", label: "contains" },
  { value: "startsWith", label: "starts" },
  { value: "endsWith", label: "ends" },
  { value: "regex", label: "regex" },
  { value: "in", label: "in" },
];

export interface EditorCondition {
  type: "condition";
  id: string;
  column: string;
  op: CompareOp;
  value: string;
  /** One `in` value, kept whole when it contains a comma. Cleared once the field is edited. */
  exact?: boolean;
  negated: boolean;
}

export interface EditorGroup {
  type: "group";
  id: string;
  kind: "and" | "or";
  collapsed: boolean;
  children: EditorNode[];
}

export type EditorNode = EditorCondition | EditorGroup;

const ROOT_ID = "root";

function rootGroup(): EditorGroup {
  return { type: "group", id: ROOT_ID, kind: "and", collapsed: false, children: [] };
}

export const conditionTree = writable<EditorGroup>(rootGroup());

function leaf(column: string): EditorCondition {
  return {
    type: "condition",
    id: crypto.randomUUID(),
    column,
    op: "contains",
    value: "",
    negated: false,
  };
}

function groupNode(): EditorGroup {
  return {
    type: "group",
    id: crypto.randomUUID(),
    kind: "and",
    collapsed: false,
    children: [],
  };
}

function mapNode(node: EditorNode, visit: (node: EditorNode) => EditorNode): EditorNode {
  const next = visit(node);
  if (next.type !== "group") {
    return next;
  }
  return {
    ...next,
    children: next.children.map((child) => mapNode(child, visit)),
  };
}

function edit(update: (root: EditorGroup) => EditorGroup): void {
  conditionTree.update(update);
}

/** Replaces the editor with one `in` condition so a stats value filters the table. */
export function filterByValue(column: string, value: string): void {
  const row: EditorCondition = {
    type: "condition",
    id: crypto.randomUUID(),
    column,
    op: "in",
    value,
    exact: true,
    negated: false,
  };
  conditionTree.set({
    type: "group",
    id: ROOT_ID,
    kind: "and",
    collapsed: false,
    children: [row],
  });
}

/** Replaces the editor with an AND of `in` conditions, one per grouping level. */
export function filterByValues(filters: Array<{ column: string; value: string }>): void {
  const children: EditorCondition[] = filters.map(({ column, value }) => ({
    type: "condition",
    id: crypto.randomUUID(),
    column,
    op: "in",
    value,
    exact: true,
    negated: false,
  }));
  conditionTree.set({
    type: "group",
    id: ROOT_ID,
    kind: "and",
    collapsed: false,
    children,
  });
}

/**
 * Adds an exact-match condition without replacing the current tree.
 * Used by the table's cell context menu; the value stays structured.
 */
export function addFilterCondition(
  column: string,
  op: FilterConditionOp,
  value: string
): void {
  conditionTree.update((root) => appendFilterCondition(root, column, op, value));
}

/** Replaces the editor with `column >= start AND column <= end`. */
export function filterByRange(column: string, start: string, end: string): void {
  const bound = (op: "ge" | "le", value: string): EditorCondition => ({
    type: "condition",
    id: crypto.randomUUID(),
    column,
    op,
    value,
    exact: true,
    negated: false,
  });
  conditionTree.set({
    type: "group",
    id: ROOT_ID,
    kind: "and",
    collapsed: false,
    children: [bound("ge", start), bound("le", end)],
  });
}

export function addCondition(groupId: string, column: string): void {
  const child = leaf(column);
  edit((root) => mapNode(root, (node) => {
    if (node.type === "group" && node.id === groupId) {
      return { ...node, children: [...node.children, child] };
    }
    return node;
  }) as EditorGroup);
}

export function addGroup(groupId: string): void {
  const child = groupNode();
  edit((root) => mapNode(root, (node) => {
    if (node.type === "group" && node.id === groupId) {
      return { ...node, children: [...node.children, child] };
    }
    return node;
  }) as EditorGroup);
}

export function removeNode(id: string): void {
  if (id === ROOT_ID) {
    return;
  }
  edit((root) => mapNode(root, (node) => {
    if (node.type !== "group") {
      return node;
    }
    return { ...node, children: node.children.filter((child) => child.id !== id) };
  }) as EditorGroup);
}

export function moveNode(id: string, delta: -1 | 1): void {
  if (id === ROOT_ID) {
    return;
  }
  edit((root) => mapNode(root, (node) => {
    if (node.type !== "group") {
      return node;
    }
    const index = node.children.findIndex((child) => child.id === id);
    const next = index + delta;
    if (index < 0 || next < 0 || next >= node.children.length) {
      return node;
    }
    const children = [...node.children];
    const [item] = children.splice(index, 1);
    children.splice(next, 0, item);
    return { ...node, children };
  }) as EditorGroup);
}

export function setGroupKind(id: string, kind: "and" | "or"): void {
  edit((root) => mapNode(root, (node) => {
    if (node.type === "group" && node.id === id) {
      return { ...node, kind };
    }
    return node;
  }) as EditorGroup);
}

export function toggleCollapsed(id: string): void {
  edit((root) => mapNode(root, (node) => {
    if (node.type === "group" && node.id === id) {
      return { ...node, collapsed: !node.collapsed };
    }
    return node;
  }) as EditorGroup);
}

export function patchCondition(id: string, patch: Partial<EditorCondition>): void {
  edit((root) => mapNode(root, (node) => {
    if (node.type === "condition" && node.id === id) {
      return { ...node, ...patch, id: node.id, type: "condition" };
    }
    return node;
  }) as EditorGroup);
}

function conditionValues(row: EditorCondition): string[] {
  if (row.op === "in" && row.exact === true) {
    return [row.value];
  }
  if (row.op === "in") {
    return row.value
      .split(",")
      .map((part) => part.trim())
      .filter((part) => part.length > 0);
  }
  return [row.value];
}

function toNode(node: EditorNode): ConditionNode | null {
  if (node.type === "condition") {
    if (node.column.trim().length === 0) {
      return null;
    }
    return {
      column: node.column,
      op: node.op,
      values: conditionValues(node),
      negated: node.negated,
    };
  }
  const children = node.children
    .map((child) => toNode(child))
    .filter((child): child is ConditionNode => child != null);
  if (children.length === 0) {
    return null;
  }
  if (children.length === 1) {
    return children[0];
  }
  return { kind: node.kind, children };
}

const COMPARE_OP_SET = new Set<CompareOp>(COMPARE_OPS.map((option) => option.value));

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function conditionFromNode(value: unknown): EditorCondition | null {
  if (!isRecord(value)) {
    return null;
  }
  const op = value.op;
  if (typeof op !== "string" || !COMPARE_OP_SET.has(op as CompareOp)) {
    return null;
  }
  const values = Array.isArray(value.values)
    ? value.values.filter((item): item is string => typeof item === "string")
    : [];
  const typedOp = op as CompareOp;
  const exact = typedOp === "in" && values.length === 1;
  return {
    type: "condition",
    id: crypto.randomUUID(),
    column: typeof value.column === "string" ? value.column : "",
    op: typedOp,
    value: typedOp === "in" && !exact ? values.join(",") : values[0] ?? "",
    exact,
    negated: value.negated === true,
  };
}

function fromNode(value: unknown): EditorNode | null {
  if (!isRecord(value)) {
    return null;
  }
  if (value.kind === "and" || value.kind === "or") {
    const children = Array.isArray(value.children)
      ? value.children
          .map((child) => fromNode(child))
          .filter((child): child is EditorNode => child != null)
      : [];
    return {
      type: "group",
      id: crypto.randomUUID(),
      kind: value.kind,
      collapsed: false,
      children,
    };
  }
  return conditionFromNode(value);
}

/** Returns an editor tree for a preset condition, or null when it is absent. */
export function presetCondition(node: unknown): EditorGroup | null {
  if (node == null) {
    return null;
  }
  const converted = fromNode(node);
  if (!converted) {
    return null;
  }
  if (converted.type === "group") {
    return { ...converted, id: ROOT_ID };
  }
  return {
    type: "group",
    id: ROOT_ID,
    kind: "and",
    collapsed: false,
    children: [converted],
  };
}

export const conditionPayload = derived(conditionTree, ($tree) => toNode($tree));
