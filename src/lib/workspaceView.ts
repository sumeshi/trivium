import { get } from "svelte/store";
import type { Backend } from "./backend";
import type { LoadProjectResponse, ViewState } from "./types";
import type { WorkspaceController } from "./workspaceController";
import type { WorkspaceSession } from "./workspaceSession";
import type { SavedView } from "./viewState";
import { cloneTree } from "./viewState.ts";
import { loadProjectView } from "./stores/viewHistory.ts";
import { saveWorkspaceView, waitForWorkspaceSave } from "./workspaceSession.ts";
import { savedViewToViewState, viewStateToSavedView, viewStateWarnings } from "./viewStateMapping.ts";

interface ViewOptions {
  backend: Backend;
  isReady: () => boolean;
  columnWidths: () => Map<string, number>;
  logicalRow: () => number;
  scrollLeft: () => number;
  signature: () => string;
  notify: (message: string) => void;
}

/** View persistence and lightweight tab snapshots share a single projection. */
export function createWorkspaceView(workspace: WorkspaceController, options: ViewOptions) {
  let writable = true;
  let disposed = false;
  let watching = false;
  const subscriptions: Array<() => void> = [];
  let timer: ReturnType<typeof setTimeout> | null = null;
  const notify = (message: string) => { if (!disposed) options.notify(message); };
  const cancelScheduled = () => { if (timer) clearTimeout(timer); timer = null; };
  const buildViewState = (): ViewState => ({
    version: 1,
    search: get(workspace.search),
    searchColumns: [...get(workspace.searchColumns)],
    condition: cloneTree(get(workspace.conditionTree)) as unknown as Record<string, unknown>,
    flagFilter: get(workspace.flagFilter),
    bookmarkedOnly: get(workspace.bookmarkedOnly),
    sortKey: get(workspace.sortKey),
    sortDirection: get(workspace.sortDirection),
    hiddenColumns: Array.from(get(workspace.hiddenColumns)),
    ...(get(workspace.frozenThrough) !== undefined ? { frozenThrough: get(workspace.frozenThrough) } : {}),
    columnWidths: Object.fromEntries(options.columnWidths()),
    logicalRow: options.logicalRow(),
  });
  const snapshotSession = (): WorkspaceSession | null => {
    const projectId = get(workspace.currentProjectId);
    if (disposed || !options.isReady() || !projectId) return null;
    workspace.rememberOffset(options.logicalRow());
    return {
      projectId, view: buildViewState(), selectedRowIds: [...get(workspace.selectedRowIds)],
      selectionSignature: options.signature(), history: workspace.snapshotHistory(),
      scrollLeft: options.scrollLeft(), writable,
    };
  };
  const flushPersist = async (projectId: string | null, notifyOnError = true) => {
    if (disposed || !projectId || !writable || !options.isReady() || projectId !== get(workspace.currentProjectId)) return;
    const state = buildViewState();
    try { await saveWorkspaceView(options.backend, projectId, state); }
    catch (error) {
      console.error("[view] failed to save view state:", error);
      if (notifyOnError) notify("Failed to save view settings. Evidence and annotations are unchanged.");
    }
  };
  const schedulePersist = () => {
    if (disposed || !writable || !options.isReady()) return;
    cancelScheduled();
    timer = setTimeout(() => { timer = null; void flushPersist(get(workspace.currentProjectId)); }, 250);
  };

  // Subscribe here so viewport updates do not invalidate the parent component
  // or rebuild a saved-view object on every scroll. Snapshot only after idle.
  const watch = () => {
    if (watching || disposed) return;
    watching = true;
    for (const store of [workspace.search, workspace.searchColumns, workspace.flagFilter,
      workspace.bookmarkedOnly, workspace.sortKey, workspace.sortDirection,
      workspace.hiddenColumns, workspace.frozenThrough, workspace.conditionTree,
      workspace.scrollTop, workspace.scrollLeft]) {
      let initial = true;
      subscriptions.push(store.subscribe(() => {
        if (initial) { initial = false; return; }
        schedulePersist();
      }));
    }
  };

  const loadSavedView = async (
    detail: LoadProjectResponse,
    session: WorkspaceSession | null,
    isCurrent: () => boolean,
  ): Promise<SavedView | null | undefined> => {
    cancelScheduled();
    writable = session?.writable ?? true;
    const projectId = detail.project.meta.id;
    let state: ViewState | null = session?.view ?? null;
    let status: "ok" | "missing" | "unreadable" | "newer" = session ? "ok" : "missing";
    let loadFailed = false;
    if (!session) {
      try {
        await waitForWorkspaceSave(projectId);
        if (disposed || !isCurrent()) return undefined;
        const loaded = await options.backend.loadViewState({ projectId });
        state = loaded.state;
        status = loaded.status;
      } catch (error) { console.error("[view] failed to load view state:", error); loadFailed = true; }
    }
    if (disposed || !isCurrent()) return undefined;
    let saved: SavedView | null = null;
    if (state) {
      saved = viewStateToSavedView(state, detail.columns);
      if (!saved) {
        writable = false;
        notify("Saved view settings could not be restored. The file is kept; changes in this session will not overwrite it.");
      } else {
        const warnings = viewStateWarnings(state, detail.columns);
        if (warnings.length > 0) notify(`Saved view settings used unknown values: ${warnings.join(", ")}. Showing defaults for those fields.`);
      }
    }
    if (loadFailed || status === "unreadable" || status === "newer") {
      writable = false;
      notify(status === "newer"
        ? "Saved view settings were written by a newer version. The file is kept; changes in this session will not overwrite it."
        : "Saved view settings could not be read. The file is kept; changes in this session will not overwrite it.");
      return saved;
    }
    if (state) return saved;
    if (status === "missing") {
      const legacy = loadProjectView(projectId, detail.columns);
      if (legacy) {
        void saveWorkspaceView(options.backend, projectId, savedViewToViewState(legacy))
          .catch((error) => {
            console.error("[view] failed to migrate legacy view state:", error);
            notify("Failed to migrate saved view settings. Evidence and annotations are unchanged.");
          });
      }
      return legacy;
    }
    return null;
  };
  workspace.onDispose(() => {
    disposed = true;
    cancelScheduled();
    for (const unsubscribe of subscriptions) unsubscribe();
  });
  return { buildViewState, snapshotSession, flushPersist, schedulePersist, watch, cancelScheduled, loadSavedView };
}
