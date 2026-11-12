import { createWorkspaceState } from "./workspaceState.ts";
import { createConditionState } from "./stores/condition.ts";
import { createViewHistory } from "./stores/viewHistory.ts";
import { createOwnedRequests } from "./workspaceRequests.ts";

/** A mounted investigation owns data; inactive tabs retain only snapshots. */
export function createWorkspaceController() {
  const state = createWorkspaceState();
  const conditions = createConditionState();
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
      state.dispose();
      const empty = { type: "group" as const, id: "root", kind: "and" as const, collapsed: false, children: [] };
      conditions.conditionTree.set(empty);
      history.setHistoryProject(null);
      history.resetHistory(empty, 0);
    },
  };
}

export type WorkspaceController = ReturnType<typeof createWorkspaceController>;
