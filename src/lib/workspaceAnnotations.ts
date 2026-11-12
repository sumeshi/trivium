import { get } from "svelte/store";
import { annotationVersions, bulkAnnotationVersions } from "./annotationVersions.ts";
import type { WorkspaceController } from "./workspaceController";

interface AnnotationCallbacks {
  single: () => void;
  bulk: () => void;
}

/** Observe changes after a case's restored revision baseline, never during setup. */
export function createWorkspaceAnnotationObserver(workspace: WorkspaceController, callbacks: AnnotationCallbacks) {
  let active = false;
  let disposed = false;
  let lastSingle = 0;
  let lastBulk = 0;
  const revisions = () => {
    const projectId = get(workspace.currentProjectId) ?? "";
    return { single: get(annotationVersions)[projectId] ?? 0, bulk: get(bulkAnnotationVersions)[projectId] ?? 0 };
  };
  const changed = () => {
    if (!active || disposed) return;
    const next = revisions();
    const single = next.single !== lastSingle, bulk = next.bulk !== lastBulk;
    lastSingle = next.single;
    lastBulk = next.bulk;
    if (bulk) callbacks.bulk();
    else if (single) callbacks.single();
  };
  const subscriptions = [annotationVersions.subscribe(changed), bulkAnnotationVersions.subscribe(changed)];
  workspace.onDispose(() => {
    disposed = true;
    active = false;
    for (const unsubscribe of subscriptions) unsubscribe();
  });
  return {
    initialize() {
      if (disposed) return;
      const current = revisions();
      lastSingle = current.single;
      lastBulk = current.bulk;
      active = true;
    },
    suspend() { active = false; },
  };
}
