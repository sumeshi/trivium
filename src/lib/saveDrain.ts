/** Accepted saves outlive components and tabs; closing drains this shared set. */
const pending = new Set<Promise<unknown>>();
let sealed = false;
let failures = 0;
let acknowledgedFailures = 0;
let draining: Promise<void> | null = null;

/** Seal only after accepted flows and the final active-view flush have drained. */
export function sealSaveAdmission(): void {
  sealed = true;
}

/** A failed close keeps the app usable while the user checks or retries saves. */
export function reopenSaveAdmission(): void {
  sealed = false;
}

/** Track an already accepted callback without rejecting it after admission seals. */
export function trackAcceptedSave<T>(operation: Promise<T>): Promise<T> {
  pending.add(operation);
  void operation.then(
    () => { pending.delete(operation); },
    () => { failures += 1; pending.delete(operation); },
  );
  return operation;
}

/** Gate at enqueue time. Its accepted callback may execute after admission seals. */
export function acceptSave<T>(start: () => Promise<T>): Promise<T> {
  if (sealed) return Promise.reject(new Error("The app is closing. Wait for it to finish saving."));
  try {
    return trackAcceptedSave(start());
  } catch (error) {
    return trackAcceptedSave(Promise.reject(error));
  }
}

/** Wait every accepted save/export, including work owned by closed tabs. */
export function flushAllSaves(): Promise<void> {
  if (draining) return draining;
  const acknowledgedAtStart = acknowledgedFailures;
  const operation = (async () => {
    while (pending.size > 0) await Promise.allSettled([...pending]);
    if (failures > acknowledgedAtStart) {
      // One close attempt must not consume a failure then silently retry itself.
      // A later explicit close can continue after the user has checked the state.
      acknowledgedFailures = failures;
      throw new Error("A save or export failed. Check your changes before closing again.");
    }
  })();
  draining = operation;
  void operation.finally(() => { if (draining === operation) draining = null; }).catch(() => {});
  return operation;
}
