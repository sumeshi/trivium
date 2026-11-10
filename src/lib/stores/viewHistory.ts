import { writable } from "svelte/store";
import type { EditorGroup } from "./condition";
import {
  emptyHistory,
  goBack as stepBack,
  goForward as stepForward,
  parseSavedView,
  pushApplied,
  withOffset,
  type History,
  type SavedView,
} from "../viewState";

const STORAGE_PREFIX = "trivium:view:";

export const canGoBack = writable(false);
export const canGoForward = writable(false);
export const historyRestore = writable<{
  projectId: string | null;
  tree: EditorGroup;
  offset: number;
  nonce: number;
} | null>(null);

let history: History = emptyHistory(
  { type: "group", id: "root", kind: "and", collapsed: false, children: [] },
  0
);
let nonce = 0;
let historyProjectId: string | null = null;

/**
 * Bind the history to a project. A queued restore for the previous project is
 * dropped so it cannot be applied to the newly selected project.
 */
export function setHistoryProject(projectId: string | null): void {
  historyProjectId = projectId;
  historyRestore.set(null);
}

function publish(): void {
  canGoBack.set(history.index > 0);
  canGoForward.set(history.index < history.entries.length - 1);
}

export function resetHistory(tree: EditorGroup, offset: number): void {
  history = emptyHistory(tree, offset);
  publish();
}

export function recordAppliedCondition(
  previousOffset: number,
  tree: EditorGroup,
  nextOffset: number
): void {
  history = pushApplied(history, tree, previousOffset, nextOffset);
  publish();
}

export function rememberOffset(offset: number): void {
  history = withOffset(history, offset);
}

export function goBack(): void {
  const next = stepBack(history);
  if (!next) {
    return;
  }
  history = next;
  publish();
  const entry = history.entries[history.index];
  historyRestore.set({
    projectId: historyProjectId,
    tree: entry.condition,
    offset: entry.offset,
    nonce: ++nonce,
  });
}

export function goForward(): void {
  const next = stepForward(history);
  if (!next) {
    return;
  }
  history = next;
  publish();
  const entry = history.entries[history.index];
  historyRestore.set({
    projectId: historyProjectId,
    tree: entry.condition,
    offset: entry.offset,
    nonce: ++nonce,
  });
}

export function loadProjectView(projectId: string, columns: readonly string[]): SavedView | null {
  try {
    const text = localStorage.getItem(STORAGE_PREFIX + projectId);
    if (!text) {
      return null;
    }
    return parseSavedView(text, columns);
  } catch {
    return null;
  }
}

export function saveProjectView(projectId: string, view: SavedView): void {
  try {
    localStorage.setItem(STORAGE_PREFIX + projectId, JSON.stringify(view));
  } catch {
    // Ignore a full or blocked store. This state is not saved on the server.
  }
}
