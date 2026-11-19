import { derived, writable } from "svelte/store";
import type { Readable } from "svelte/store";
import type { CompareOp, ConditionNode } from "../types";
import type { FormatDefinition } from "../formats";
import { isFormatDefinition, formatDefinitionKey } from "../formats.ts";
import {
  appendFilterCondition, appendConditionsAnd,
  type FilterConditionOp,
} from "../conditionFilters.ts";

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
  /** Structured preset values remain literal until the value field is edited. */
  sourceValues?: string[];
  negated: boolean;
}

export interface EditorGroup {
  type: "group";
  id: string;
  kind: "and" | "or";
  collapsed: boolean;
  children: EditorNode[];
  /** Search chips belong to the root expression and combine with Rules by AND. */
  snippets?: FormatSnippet[];
}

export interface FormatSnippet {
  id: string;
  format: FormatDefinition;
}

export interface EditorFormatCondition {
  type: "format";
  id: string;
  columns: string[];
  format: FormatDefinition;
  equals?: string;
  negated: boolean;
}

export type EditorNode = EditorCondition | EditorFormatCondition | EditorGroup;

/** Newly added text-match rules stay inactive until their value is entered. */
export function isIncompleteTextCondition(row: Pick<EditorCondition, "op" | "value" | "sourceValues">): boolean {
  return row.sourceValues === undefined && row.value.length === 0
    && (row.op === "contains" || row.op === "startsWith" || row.op === "endsWith");
}

export function conditionValues(row: EditorCondition): string[] {
  if (row.sourceValues !== undefined) return [...row.sourceValues];
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

const ROOT_ID = "root";

function rootGroup(): EditorGroup {
  return { type: "group", id: ROOT_ID, kind: "and", collapsed: false, children: [] };
}

/** One editor tree per active investigation. */
export function createConditionState(scope: {
  columns?: Readable<readonly string[]>;
  searchColumns?: Readable<readonly string[]>;
} = {}) {
  const tree = writable<EditorGroup>(rootGroup());
  // Only untouched ranges added by this analysis control can be replaced. A
  // restored tree or a user-edited range becomes an ordinary investigation rule.
  const ranges = new Map<string, { id: string; signature: string }>();
  const signature = (node: EditorNode) => JSON.stringify(node,
    (key, value: unknown) => key === "collapsed" ? undefined : value);
  const conditionTree = {
    subscribe: tree.subscribe,
    set(root: EditorGroup): void {
      ranges.clear();
      tree.set(root);
    },
    update(update: (root: EditorGroup) => EditorGroup): void {
      tree.update((root) => {
        const next = update(root);
        for (const [source, owned] of ranges) {
          const node = next.kind === "and" && next.children.find((child) => child.id === owned.id);
          if (!node || signature(node) !== owned.signature) ranges.delete(source);
        }
        return next;
      });
    },
  };

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

  /** Drilldowns narrow the current expression while keeping literal values whole. */
  function filterByValue(column: string, value: string): void {
    filterByValues([{ column, value }]);
  }

  function filterByValues(filters: Array<{ column: string; value: string }>): void {
    const children: EditorCondition[] = filters.map(({ column, value }) => ({
      type: "condition", id: crypto.randomUUID(), column, op: "in", value,
      exact: true, negated: false,
    }));
    conditionTree.update((root) => appendConditionsAnd(root, children));
  }

  /**
   * Adds an exact-match condition without replacing the current tree.
   * Used by the table's cell context menu; the value stays structured.
   */
  function addFilterCondition(
    column: string,
    op: FilterConditionOp,
    value: string
  ): void {
    conditionTree.update((root) => appendFilterCondition(root, column, op, value));
  }

  /** Add a range; subsequent adjustments replace only this control's own range. */
  function filterByRange(column: string, start: string, end: string, source = "timeline"): void {
    const bound = (op: "regex" | "ge" | "le", value: string): EditorCondition => ({
      type: "condition", id: crypto.randomUUID(), column, op, value,
      exact: true, negated: false,
    });
    const range: EditorGroup = {
      type: "group", id: crypto.randomUUID(), kind: "and", collapsed: false,
      children: [bound("regex", "\\S"), bound("ge", start), bound("le", end)],
    };
    conditionTree.update((root) => {
      const owned = ranges.get(source);
      const next = owned
        ? { ...root, children: root.children.map((node) => node.id === owned.id ? range : node) }
        : appendConditionsAnd(root, [range]);
      ranges.set(source, { id: range.id, signature: signature(range) });
      return next;
    });
  }

  /** Remove this control's period, including an intact period restored from a view. */
  function clearRangeFilter(column: string, source = "timeline"): void {
    const owned = ranges.get(source);
    const otherRanges = new Set([...ranges].filter(([name]) => name !== source).map(([, range]) => range.id));
    const canonicalRange = (node: EditorNode) => {
      if (node.type !== "group" || node.kind !== "and" || node.children.length !== 3) return false;
      const [guard, first, last] = node.children;
      if (guard.type !== "condition" || first.type !== "condition" || last.type !== "condition") return false;
      return [guard, first, last].every(value => value.column === column && !value.negated && conditionValues(value).length === 1)
        && guard.op === "regex" && conditionValues(guard)[0] === "\\S" && first.op === "ge" && last.op === "le";
    };
    conditionTree.update(root => {
      // OR and mixed manual groups cannot be safely widened by pruning children.
      if (canonicalRange(root) && !otherRanges.has(root.id)) return { ...root, children: [] };
      if (root.kind !== "and") return root;
      const children = root.children.filter(node => {
        if (otherRanges.has(node.id)) return true;
        if (owned?.id === node.id && owned.signature === signature(node)) return false;
        return !canonicalRange(node) || node.type === "group" && Boolean(node.snippets?.length);
      });
      return children.length === root.children.length ? root : { ...root, children };
    });
    ranges.delete(source);
  }

  function addCondition(groupId: string, column: string): void {
    const child = leaf(column);
    edit((root) => mapNode(root, (node) => {
      if (node.type === "group" && node.id === groupId) {
        return { ...node, children: [...node.children, child] };
      }
      return node;
    }) as EditorGroup);
  }

  function addGroup(groupId: string): void {
    const child = groupNode();
    edit((root) => mapNode(root, (node) => {
      if (node.type === "group" && node.id === groupId) {
        return { ...node, children: [...node.children, child] };
      }
      return node;
    }) as EditorGroup);
  }

  function addFormatCondition(groupId: string, columns: readonly string[], format: FormatDefinition, equals?: string): void {
    const child: EditorFormatCondition = { type: "format", id: crypto.randomUUID(),
      columns: [...columns], format: structuredClone(format), negated: false,
      ...(equals === undefined ? {} : { equals }) };
    edit(root => mapNode(root, node => node.type === "group" && node.id === groupId
      ? { ...node, children: [...node.children, child] } : node) as EditorGroup);
  }

  function patchFormatCondition(id: string, patch: Partial<Pick<EditorFormatCondition, "columns" | "format" | "equals" | "negated">>): void {
    const snapshot = structuredClone(patch);
    edit(root => mapNode(root, node => node.type === "format" && node.id === id
      ? { ...node, ...snapshot } : node) as EditorGroup);
  }

  function addPatternSnippet(format: FormatDefinition): void {
    const snapshot = structuredClone(format);
    conditionTree.update(root => root.snippets?.some(snippet => formatDefinitionKey(snippet.format) === formatDefinitionKey(snapshot))
      ? root : { ...root, snippets: [...(root.snippets ?? []), { id: crypto.randomUUID(), format: snapshot }] });
  }

  function removePatternSnippet(id: string): void {
    conditionTree.update(root => ({ ...root, snippets: root.snippets?.filter(snippet => snippet.id !== id) }));
  }

  function removeNode(id: string): void {
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

  function moveNode(id: string, delta: -1 | 1): void {
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

  function setGroupKind(id: string, kind: "and" | "or"): void {
    edit((root) => mapNode(root, (node) => {
      if (node.type === "group" && node.id === id) {
        return { ...node, kind };
      }
      return node;
    }) as EditorGroup);
  }

  function toggleCollapsed(id: string): void {
    edit((root) => mapNode(root, (node) => {
      if (node.type === "group" && node.id === id) {
        return { ...node, collapsed: !node.collapsed };
      }
      return node;
    }) as EditorGroup);
  }

  function patchCondition(id: string, patch: Partial<EditorCondition>): void {
    edit((root) => mapNode(root, (node) => {
      if (node.type === "condition" && node.id === id) {
        return {
          ...node, ...patch,
          // Operator, column and NOT edits keep the same literal values.
          ...(Object.hasOwn(patch, "value") ? { sourceValues: undefined } : {}),
          id: node.id, type: "condition",
        };
      }
      return node;
    }) as EditorGroup);
  }


  function toNode(node: EditorNode): ConditionNode | null {
    if (node.type === "condition") {
      if (node.column.trim().length === 0 || isIncompleteTextCondition(node)) {
        return null;
      }
      return {
        column: node.column,
        op: node.op,
        values: conditionValues(node),
        negated: node.negated,
      };
    }
    if (node.type === "format") {
      return { columns: [...node.columns], format: structuredClone(node.format),
        negated: node.negated, ...(node.equals === undefined ? {} : { equals: node.equals }) };
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


  const patternSnippets = derived(conditionTree, root => root.snippets ?? []);
  const conditionPayload = derived([conditionTree, scope.columns ?? writable<readonly string[]>([]),
    scope.searchColumns ?? writable<readonly string[]>([])], ([root, columns, selected]) => {
    const known = new Set(columns);
    const targets = selected.length ? selected.filter(column => known.has(column)) : [...columns];
    const manual = toNode(root);
    const children: ConditionNode[] = manual ? [manual] : [];
    for (const snippet of root.snippets ?? []) children.push({
      columns: [...targets], format: structuredClone(snippet.format), negated: false,
    });
    return children.length === 0 ? null : children.length === 1 ? children[0] : { kind: "and" as const, children };
  });

  return { conditionTree, filterByValue, filterByValues, addFilterCondition, filterByRange, clearRangeFilter, addCondition, addGroup,
    addFormatCondition, patchFormatCondition, patternSnippets, addPatternSnippet, removePatternSnippet,
    removeNode, moveNode, setGroupKind, toggleCollapsed, patchCondition, conditionPayload };
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
    value: values.length > 1 ? values.join(",") : values[0] ?? "",
    exact,
    sourceValues: values,
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
  if (isFormatDefinition(value.format) && Array.isArray(value.columns)
    && value.columns.every(column => typeof column === "string")
    && (value.equals === undefined || typeof value.equals === "string")) {
    return { type: "format", id: crypto.randomUUID(), columns: [...value.columns],
      format: structuredClone(value.format), negated: value.negated === true,
      ...(value.equals === undefined ? {} : { equals: value.equals }) };
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
