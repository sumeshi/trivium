import type { EditorGroup, EditorNode } from "./stores/condition";

export interface HistoryEntry {
  condition: EditorGroup;
  offset: number;
}

export interface History {
  entries: HistoryEntry[];
  index: number;
}

export interface SavedView {
  condition: EditorGroup;
  hiddenColumns: string[];
  sortKey: string | null;
  sortDirection: "asc" | "desc";
  offset: number;
}

const HISTORY_LIMIT = 50;

const COMPARE_OPS = new Set([
  "eq",
  "ne",
  "gt",
  "lt",
  "ge",
  "le",
  "contains",
  "startsWith",
  "endsWith",
  "regex",
  "in",
]);

export function cloneTree(tree: EditorGroup): EditorGroup {
  return JSON.parse(JSON.stringify(tree)) as EditorGroup;
}

function structureKey(node: EditorNode): string {
  if (node.type === "condition") {
    return `c:${node.column}:${node.op}:${node.negated ? 1 : 0}`;
  }
  return `g:${node.kind}:${node.children.map((child) => structureKey(child)).join(",")}`;
}

export function emptyHistory(condition: EditorGroup, offset: number): History {
  return { entries: [{ condition: cloneTree(condition), offset }], index: 0 };
}

/** A value edit keeps the current entry. A structural change pushes, and drops forward entries. */
export function pushApplied(
  history: History,
  condition: EditorGroup,
  previousOffset: number,
  nextOffset: number
): History {
  const tree = cloneTree(condition);
  const current = history.entries[history.index];
  if (current && structureKey(current.condition) === structureKey(tree)) {
    const entries = history.entries.slice(0, history.index + 1);
    entries[history.index] = { ...current, condition: tree };
    return { entries, index: history.index };
  }
  const entries = history.entries.slice(0, history.index + 1);
  if (entries[history.index]) {
    entries[history.index] = { ...entries[history.index], offset: previousOffset };
  }
  entries.push({ condition: tree, offset: nextOffset });
  const overflow = entries.length - HISTORY_LIMIT;
  if (overflow > 0) {
    return { entries: entries.slice(overflow), index: entries.length - 1 - overflow };
  }
  return { entries, index: entries.length - 1 };
}

export function withOffset(history: History, offset: number): History {
  const current = history.entries[history.index];
  if (!current || current.offset === offset) {
    return history;
  }
  const entries = history.entries.slice();
  entries[history.index] = { ...current, offset };
  return { entries, index: history.index };
}

export function goBack(history: History): History | null {
  if (history.index <= 0) {
    return null;
  }
  return { entries: history.entries, index: history.index - 1 };
}

export function goForward(history: History): History | null {
  if (history.index >= history.entries.length - 1) {
    return null;
  }
  return { entries: history.entries, index: history.index + 1 };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isNode(value: unknown): boolean {
  if (!isRecord(value) || typeof value.type !== "string") {
    return false;
  }
  if (value.type === "condition") {
    return (
      typeof value.id === "string" &&
      typeof value.column === "string" &&
      typeof value.op === "string" &&
      COMPARE_OPS.has(value.op) &&
      typeof value.value === "string" &&
      typeof value.negated === "boolean" &&
      (value.exact === undefined || typeof value.exact === "boolean")
    );
  }
  if (value.type === "group") {
    return (
      typeof value.id === "string" &&
      (value.kind === "and" || value.kind === "or") &&
      typeof value.collapsed === "boolean" &&
      Array.isArray(value.children) &&
      value.children.every((child) => isNode(child))
    );
  }
  return false;
}

export function parseSavedView(text: string, columns: readonly string[]): SavedView | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  if (!isRecord(parsed) || !isNode(parsed.condition) || (parsed.condition as { type?: string }).type !== "group") {
    return null;
  }
  const known = new Set(columns);
  const hiddenColumns = Array.isArray(parsed.hiddenColumns)
    ? parsed.hiddenColumns.filter((name): name is string => typeof name === "string" && known.has(name))
    : [];
  const sortKey = typeof parsed.sortKey === "string" && known.has(parsed.sortKey) ? parsed.sortKey : null;
  const sortDirection = parsed.sortDirection === "desc" ? "desc" : "asc";
  const offset = typeof parsed.offset === "number" && Number.isInteger(parsed.offset) && parsed.offset >= 0
    ? parsed.offset
    : 0;
  return {
    condition: parsed.condition as EditorGroup,
    hiddenColumns,
    sortKey,
    sortDirection,
    offset,
  };
}
