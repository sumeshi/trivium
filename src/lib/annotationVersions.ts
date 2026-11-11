import { writable } from "svelte/store";

// Keep only a counter per project. A successful annotation write invalidates
// open analyses even when the total number of flagged rows did not change.
const versions = writable<Record<string, number>>({});
export const annotationVersions = { subscribe: versions.subscribe };
const bulkVersions = writable<Record<string, number>>({});
export const bulkAnnotationVersions = { subscribe: bulkVersions.subscribe };

export function markAnnotationsChanged(projectId: string): void {
  versions.update((current) => ({ ...current, [projectId]: (current[projectId] ?? 0) + 1 }));
}

/** Bulk writes can complete after their drawer closes; cached rows still change. */
export function markBulkAnnotationsChanged(projectId: string): void {
  markAnnotationsChanged(projectId);
  bulkVersions.update((current) => ({ ...current, [projectId]: (current[projectId] ?? 0) + 1 }));
}

export function forgetAnnotationVersion(projectId: string): void {
  versions.update((current) => {
    const next = { ...current };
    delete next[projectId];
    return next;
  });
  bulkVersions.update((current) => {
    const next = { ...current };
    delete next[projectId];
    return next;
  });
}
