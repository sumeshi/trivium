/** Own native read jobs independently of a component's rendered DOM. */
export function createOwnedRequests() {
  const pending = new Map<string, { id: string; backend: object }>();
  let disposed = false;

  const cancel = (entry: { id: string; backend: object }) => {
    // Read scopes remain usable with older transports and small test backends.
    const method: unknown = Reflect.get(entry.backend, "cancelRequest");
    if (typeof method !== "function") return;
    try {
      void Promise.resolve(method.call(entry.backend, entry.id)).catch(() => {});
    } catch {
      // A transport can already be gone during application teardown.
    }
  };

  const cancelAll = () => {
    for (const entry of pending.values()) cancel(entry);
    pending.clear();
  };

  return {
    async run<T>(key: string, backend: object, work: (requestId: string) => Promise<T>): Promise<T> {
      if (disposed) throw new Error("Read request scope is disposed.");
      const previous = pending.get(key);
      if (previous) cancel(previous);
      const entry = { id: crypto.randomUUID(), backend };
      pending.set(key, entry);
      try {
        return await work(entry.id);
      } finally {
        if (pending.get(key) === entry) pending.delete(key);
      }
    },
    cancel(key: string) {
      const entry = pending.get(key);
      if (entry) { cancel(entry); pending.delete(key); }
    },
    cancelAll,
    dispose() {
      if (disposed) return;
      disposed = true;
      cancelAll();
    },
  };
}

export type OwnedRequests = ReturnType<typeof createOwnedRequests>;
