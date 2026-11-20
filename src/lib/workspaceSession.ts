import type { Backend } from "./backend";
import type { ViewState } from "./types";
import type { History } from "./viewState";
import { acceptSave } from "./saveDrain.ts";
import type { ExtractPanelState } from "./extractTypes";
import type { GroupPanelState } from "./grouping";

/** Inactive tabs retain controls and row IDs, never row data or page caches. */
export interface WorkspaceSession {
  projectId: string;
  view: ViewState;
  selectedRowIds: number[];
  selectionSignature: string;
  history: History;
  scrollLeft: number;
  writable: boolean;
  extractPanel?: ExtractPanelState;
  groupPanel?: GroupPanelState;
}

/** Explicit projection prevents a future caller from retaining page payloads. */
export function copyWorkspaceSession(session: WorkspaceSession): WorkspaceSession {
  return {
    projectId: session.projectId,
    view: structuredClone(session.view),
    selectedRowIds: [...session.selectedRowIds],
    selectionSignature: session.selectionSignature,
    history: structuredClone(session.history),
    scrollLeft: session.scrollLeft,
    writable: session.writable,
    ...(session.extractPanel ? { extractPanel: structuredClone(session.extractPanel) } : {}),
    ...(session.groupPanel ? { groupPanel: structuredClone(session.groupPanel) } : {}),
  };
}

const pendingSaves = new Map<string, Promise<void>>();

/** Serialize writes across old/new component instances for the same project. */
export function saveWorkspaceView(
  backend: Pick<Backend, "saveViewState">,
  projectId: string,
  state: ViewState
): Promise<void> {
  return acceptSave(() => {
    const snapshot = structuredClone(state);
    const previous = pendingSaves.get(projectId) ?? Promise.resolve();
    const next = previous.catch(() => {}).then(() => backend.saveViewState({ projectId, state: snapshot }));
    pendingSaves.set(projectId, next);
    void next.finally(() => {
      if (pendingSaves.get(projectId) === next) pendingSaves.delete(projectId);
    }).catch(() => {});
    return next;
  });
}

/** A just-closed tab must not reload a file before its last save completes. */
export async function waitForWorkspaceSave(projectId: string): Promise<void> {
  let pending = pendingSaves.get(projectId);
  while (pending) {
    await pending.catch(() => {});
    const latest = pendingSaves.get(projectId);
    if (latest === pending) return;
    pending = latest;
  }
}
