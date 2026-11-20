import { createWorkspaceState } from "./workspaceState.ts";
import { createConditionState } from "./stores/condition.ts";
import { createViewHistory } from "./stores/viewHistory.ts";
import { createOwnedRequests } from "./workspaceRequests.ts";
import { derived, get } from "svelte/store";
import { createIocEditor } from "./iocEditor.ts";
import type { IocEntry } from "./types";

/** A mounted investigation owns data; inactive tabs retain only snapshots. */
export function createWorkspaceController(onIocsSaved: (projectId: string, entries: IocEntry[]) => void = () => {}) {
  const state = createWorkspaceState();
  const iocEditor = createIocEditor({
    backend: () => get(state.backend),
    project: () => get(state.projectDetail),
    annotationsChanged: () => state.annotationRefreshRequested.update(value => value + 1),
    saved: (projectId, entries) => {
      state.projectDetail.update(detail => detail?.project.meta.id === projectId ? { ...detail, iocs: entries } : detail);
      onIocsSaved(projectId, entries);
    },
  });
  const conditions = createConditionState({
    columns: derived(state.projectDetail, detail => detail?.columns ?? []),
    searchColumns: state.searchColumns,
  });
  const history = createViewHistory();
  const cleanups = new Set<() => void>();
  let disposed = false;
  const onDispose = (cleanup: () => void) => {
    if (disposed) cleanup();
    else cleanups.add(cleanup);
    return () => cleanups.delete(cleanup);
  };
  return {
    ...state,
    ...conditions,
    ...history,
    makeGroupRulesEditable() {
      conditions.makeGroupRulesEditable();
      // Conversion changes provenance, not the query or its current scroll.
      history.recordAppliedCondition(0, get(conditions.conditionTree), 0, null);
    },
    iocEditor,
    onDispose,
    createReadScope() {
      const scope = createOwnedRequests();
      const unregister = onDispose(() => scope.dispose());
      return { ...scope, dispose() { unregister(); scope.dispose(); } };
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      for (const cleanup of [...cleanups]) cleanup();
      cleanups.clear();
      iocEditor.dispose();
      state.dispose();
      const empty = { type: "group" as const, id: "root", kind: "and" as const, collapsed: false, children: [] };
      conditions.conditionTree.set(empty);
      history.setHistoryProject(null);
      history.resetHistory(empty, 0);
    },
  };
}

export type WorkspaceController = ReturnType<typeof createWorkspaceController>;
