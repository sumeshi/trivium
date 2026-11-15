import { get, writable } from "svelte/store";
import { errorMessage } from "./errors.ts";

export interface AppCloseState {
  phase: "idle" | "waiting" | "recovering" | "canceling" | "failed";
  message: string;
}

export interface AppCloseOperations {
  unsavedDraft: () => string | null;
  notifyDraft: (message: string) => void;
  freeze: () => void;
  drain: () => Promise<void>;
  flushActive: () => Promise<void>;
  seal: () => void;
  finish: () => Promise<void>;
  cancelNative: (acknowledgeFailures: boolean) => Promise<void>;
  resume: () => void;
}

/** Keep startup inert until listening and replay any close requested before JS. */
export async function installAppCloseListener(operations: {
  listen: (request: () => void) => Promise<() => void>;
  pending: () => Promise<boolean>;
  current: () => boolean;
  enable: () => void;
  request: () => void;
}): Promise<() => void> {
  let ready = false, earlyRequest = false;
  const stop = await operations.listen(() => {
    if (!operations.current()) return;
    if (ready) operations.request();
    else earlyRequest = true;
  });
  try {
    if (!operations.current()) { stop(); return () => {}; }
    const pending = await operations.pending();
    if (!operations.current()) { stop(); return () => {}; }
    operations.enable();
    ready = true;
    if (pending || earlyRequest) operations.request();
    return stop;
  } catch (error) {
    stop();
    throw error;
  }
}

/** Native X/Alt-F4 requests share one close attempt. Never abandon accepted work. */
export function createAppClose(operations: AppCloseOperations) {
  const state = writable<AppCloseState>({ phase: "idle", message: "" });
  let generation = 0;
  let disposed = false;
  let active: Promise<void> | null = null;

  function request(explicitRetry = false): Promise<void> {
    const phase = get(state).phase;
    if (disposed || phase === "waiting" || phase === "recovering" || phase === "canceling" || (phase === "failed" && !explicitRetry)) {
      return active ?? Promise.resolve();
    }
    const draft = operations.unsavedDraft();
    if (draft) {
      state.set({ phase: "idle", message: "" });
      operations.notifyDraft(draft);
      return Promise.resolve();
    }
    const attempt = ++generation;
    const current = () => !disposed && attempt === generation;
    state.set({ phase: "waiting", message: "Saving changes before closing…" });
    const operation = (async () => {
      let observedFailure = false;
      try {
        operations.freeze();
        // Accepted flows may enqueue their later write after a preview/import.
        // UI is frozen, but admission stays open until those flows finish.
        try { await operations.drain(); }
        catch (error) { observedFailure = true; throw error; }
        if (!current()) return;
        let flushFailure: unknown;
        let flushFailed = false;
        try { await operations.flushActive(); }
        catch (error) { flushFailed = true; flushFailure = error; }
        if (!current()) return;
        // Even a rejected active save must reach the strict global drain, so
        // its failure is reported once rather than poisoning the next retry.
        try { await operations.drain(); }
        catch (error) { observedFailure = true; throw error; }
        if (flushFailed) { observedFailure = true; throw flushFailure; }
        if (!current()) return;
        operations.seal();
        try { await operations.finish(); }
        catch (error) { observedFailure = true; throw error; }
      } catch (error) {
        if (!current()) return;
        let message = errorMessage(error, "Changes could not be saved.");
        state.set({ phase: "recovering", message });
        await Promise.resolve();
        if (!current()) return;
        try { await operations.cancelNative(observedFailure); }
        catch { message += " Could not reset the close request; keep the app open and try again."; }
        if (!current()) return;
        operations.resume();
        state.set({ phase: "failed", message });
      }
    })();
    active = operation;
    void operation.finally(() => { if (active === operation) active = null; }).catch(() => {});
    return operation;
  }

  function keepOpen(): Promise<void> {
    // A pending strict drain must finish and show its error before any
    // cancellation can acknowledge it. Long exports keep their normal work.
    if (disposed || ["waiting", "recovering", "canceling"].includes(get(state).phase)) return active ?? Promise.resolve();
    const attempt = ++generation;
    state.set({ phase: "canceling", message: "Keeping the application open…" });
    const operation = (async () => {
      try {
        // The native guard cancels an in-progress finish as well as queued ones.
        await operations.cancelNative(false);
        if (disposed || generation !== attempt) return;
        operations.resume();
        state.set({ phase: "idle", message: "" });
      } catch (error) {
        if (disposed || generation !== attempt) return;
        operations.resume();
        state.set({ phase: "failed", message: errorMessage(error, "Could not cancel closing. Try again.") });
      }
    })();
    active = operation;
    void operation.finally(() => { if (active === operation) active = null; }).catch(() => {});
    return operation;
  }

  return {
    state: { subscribe: state.subscribe }, request, keepOpen,
    retry: () => request(true),
    isBlocked: () => get(state).phase !== "idle",
    dispose: () => { disposed = true; generation += 1; },
  };
}
