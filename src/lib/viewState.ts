import type { EditorGroup, EditorNode } from "./stores/condition";
import type { FlagFilterValue } from "./types";
import { savedViewFromUnknown } from "./viewStateMapping.ts";

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
  search: string;
  searchColumns: string[];
  flagFilter: FlagFilterValue;
  bookmarkedOnly: boolean;
  hiddenColumns: string[];
  sortKey: string | null;
  sortDirection: "asc" | "desc";
  /** Persisted widths keyed by data column id. Rebuilt when absent. */
  columnWidths: Record<string, number>;
  offset: number;
  /** Result-relative row at the top of the viewport. Preferred over `offset`. */
  logicalRow?: number;
}

const HISTORY_LIMIT = 50;

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

export function parseSavedView(text: string, columns: readonly string[]): SavedView | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  return savedViewFromUnknown(parsed, columns);
}
