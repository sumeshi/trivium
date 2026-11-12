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
} from "../viewState.ts";

const STORAGE_PREFIX = "trivium:view:";

/** History has the same lifetime as its investigation. */
export function createViewHistory() {
  const canGoBack = writable(false);
  const canGoForward = writable(false);
  const historyRestore = writable<{
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
  function setHistoryProject(projectId: string | null): void {
    historyProjectId = projectId;
    historyRestore.set(null);
  }

  function publish(): void {
    canGoBack.set(history.index > 0);
    canGoForward.set(history.index < history.entries.length - 1);
  }

  function resetHistory(tree: EditorGroup, offset: number): void {
    history = emptyHistory(tree, offset);
    publish();
  }

  function snapshotHistory(): History {
    return structuredClone(history);
  }

  function restoreHistory(snapshot: History): void {
    history = structuredClone(snapshot);
    historyRestore.set(null);
    publish();
  }

  function recordAppliedCondition(
    previousOffset: number,
    tree: EditorGroup,
    nextOffset: number
  ): void {
    history = pushApplied(history, tree, previousOffset, nextOffset);
    publish();
  }

  function rememberOffset(offset: number): void {
    history = withOffset(history, offset);
  }

  function goBack(): void {
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

  function goForward(): void {
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

  return { canGoBack, canGoForward, historyRestore, setHistoryProject, resetHistory, snapshotHistory, restoreHistory, recordAppliedCondition, rememberOffset, goBack, goForward };
}

/**
 * Legacy localStorage view storage.
 *
 * New saves go to the engine (`saveViewState`), which keeps view state per
 * project and writes it atomically. This reader remains only to migrate a
 * project created before the engine view state existed; once it is read, the
 * caller writes the same view to the engine and later loads use the engine.
 *
 * @deprecated Use `Backend.loadViewState`.
 */
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

/**
 * @deprecated Use `Backend.saveViewState`; this is only for the migration path
 * when a legacy save must be mirrored.
 */
export function saveProjectView(projectId: string, view: SavedView): void {
  try {
    localStorage.setItem(STORAGE_PREFIX + projectId, JSON.stringify(view));
  } catch {
    // Ignore a full or blocked store. This state is not saved on the server.
  }
}
