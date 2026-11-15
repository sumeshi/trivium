import { acceptSave } from "./saveDrain.ts";

interface AnnotationQueue {
  tail: Promise<unknown>;
  failures: number;
  acknowledgedFailures: number;
}

/** Pending annotations outlive a tab; failed saves remain visible to an export. */
const pendingWrites = new Map<string, AnnotationQueue>();

function queueFor(projectId: string): AnnotationQueue {
  let queue = pendingWrites.get(projectId);
  if (!queue) {
    queue = { tail: Promise.resolve(), failures: 0, acknowledgedFailures: 0 };
    pendingWrites.set(projectId, queue);
  }
  return queue;
}

function enqueue<T>(projectId: string, queue: AnnotationQueue, operation: () => Promise<T>): Promise<T> {
  const next = queue.tail.catch(() => {}).then(operation);
  queue.tail = next;
  void next.finally(() => {
    if (queue.tail !== next || pendingWrites.get(projectId) !== queue) return;
    if (queue.failures === queue.acknowledgedFailures) pendingWrites.delete(projectId);
    else queue.tail = Promise.resolve(); // Keep only the failure counters, not its error/stack.
  }).catch(() => {});
  return next;
}

/** Preserve mutation order within a case without blocking another case. */
export function queueAnnotationWrite<T>(projectId: string, write: () => Promise<T>): Promise<T> {
  return acceptSave(() => {
    const queue = queueFor(projectId);
    return enqueue(projectId, queue, async () => {
      try {
        return await write();
      } catch (error) {
        queue.failures += 1;
        throw error;
      }
    });
  });
}

/** A strict snapshot runs after earlier saves and before later ones. */
export function queueAfterAnnotationWrites<T>(projectId: string, read: () => Promise<T>): Promise<T> {
  return acceptSave(() => {
    const queue = queueFor(projectId);
    const acknowledgedAtRequest = queue.acknowledgedFailures;
    return enqueue(projectId, queue, async () => {
      if (queue.failures > acknowledgedAtRequest) {
        // Every export requested before this warning must stop. A fresh explicit
        // retry may export the confirmed state after the user has checked it.
        queue.acknowledgedFailures = queue.failures;
        throw new Error("Export stopped because an annotation could not be saved. Check the annotations before retrying the export.");
      }
      return read();
    });
  });
}

/** A successfully deleted case no longer needs its remembered save failures. */
export function forgetAnnotationWrites(projectId: string): void {
  pendingWrites.delete(projectId);
}

/** A newly mounted case must not query a snapshot older than its pending edits. */
export async function waitForAnnotationWrites(projectId: string): Promise<void> {
  let pending = pendingWrites.get(projectId)?.tail;
  while (pending) {
    await pending.catch(() => {});
    const latest = pendingWrites.get(projectId)?.tail;
    if (latest === pending) return;
    pending = latest;
  }
}
