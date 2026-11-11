const pendingWrites = new Map<string, Promise<void>>();

/** Order metadata writes across old/new tab components for the same project. */
export function queueHiddenColumnWrite(
  projectId: string,
  columns: readonly string[],
  write: (snapshot: string[]) => Promise<void>
): Promise<void> {
  const snapshot = [...columns];
  const previous = pendingWrites.get(projectId) ?? Promise.resolve();
  const next = previous.catch(() => {}).then(() => write(snapshot));
  pendingWrites.set(projectId, next);
  void next.finally(() => {
    if (pendingWrites.get(projectId) === next) pendingWrites.delete(projectId);
  }).catch(() => {});
  return next;
}

/** Revisiting a tab must read metadata after all of its queued writes settle. */
export async function waitForHiddenColumnWrites(projectId: string): Promise<void> {
  let pending = pendingWrites.get(projectId);
  while (pending) {
    await pending.catch(() => {});
    const latest = pendingWrites.get(projectId);
    if (latest === pending) return;
    pending = latest;
  }
}
