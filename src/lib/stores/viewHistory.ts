import { writable } from "svelte/store";
import type { EditorGroup } from "./condition";
import type { GroupFilterSnapshot } from "../grouping";
import {
  emptyHistory,
  goBack as stepBack,
  goForward as stepForward,
  pushApplied,
  withOffset,
  type History,
} from "../viewState.ts";

/** History has the same lifetime as its investigation. */
export function createViewHistory() {
  const canGoBack = writable(false);
  const canGoForward = writable(false);
  const historyRestore = writable<{
    projectId: string | null;
    tree: EditorGroup;
    offset: number;
    nonce: number;
    groupFilter?: GroupFilterSnapshot;
  } | null>(null);

  let history: History = emptyHistory(
    { type: "group", id: "root", kind: "and", collapsed: false, children: [] },
    0
  );
  let nonce = 0;
  let historyProjectId: string | null = null;
  let pendingOffset: number | null = null;
  const flushOffset = () => {
    if (pendingOffset === null) return;
    history = withOffset(history, pendingOffset);
    pendingOffset = null;
  };

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
    pendingOffset = null;
    history = emptyHistory(tree, offset);
    publish();
  }

  function snapshotHistory(): History {
    flushOffset();
    return structuredClone(history);
  }

  function restoreHistory(snapshot: History): void {
    pendingOffset = null;
    history = structuredClone(snapshot);
    historyRestore.set(null);
    publish();
  }

  function recordAppliedCondition(
    previousOffset: number,
    tree: EditorGroup,
    nextOffset: number,
    groupFilter: GroupFilterSnapshot | null = null,
  ): void {
    flushOffset();
    history = pushApplied(history, tree, previousOffset, nextOffset, groupFilter);
    publish();
  }

  function rememberOffset(offset: number): void {
    pendingOffset = offset;
  }

  function goBack(): void {
    flushOffset();
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
      ...(entry.groupFilter ? { groupFilter: structuredClone(entry.groupFilter) } : {}),
    });
  }

  function goForward(): void {
    flushOffset();
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
      ...(entry.groupFilter ? { groupFilter: structuredClone(entry.groupFilter) } : {}),
    });
  }

  return { canGoBack, canGoForward, historyRestore, setHistoryProject, resetHistory, snapshotHistory, restoreHistory, recordAppliedCondition, rememberOffset, goBack, goForward };
}
