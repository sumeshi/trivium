/** Pending annotations outlive a tab; reopening that case waits for their commit. */
const pendingWrites = new Map<string, Promise<unknown>>();

/** Preserve mutation order within a case without blocking another case. */
export function queueAnnotationWrite<T>(projectId: string, write: () => Promise<T>): Promise<T> {
  const previous = pendingWrites.get(projectId) ?? Promise.resolve();
  const next = previous.catch(() => {}).then(write);
  pendingWrites.set(projectId, next);
  void next.finally(() => {
    if (pendingWrites.get(projectId) === next) pendingWrites.delete(projectId);
  }).catch(() => {});
  return next;
}

/** A newly mounted case must not query a snapshot older than its pending edits. */
export async function waitForAnnotationWrites(projectId: string): Promise<void> {
  let pending = pendingWrites.get(projectId);
  while (pending) {
    await pending.catch(() => {});
    const latest = pendingWrites.get(projectId);
    if (latest === pending) return;
    pending = latest;
  }
}
