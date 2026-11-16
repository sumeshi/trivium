import { writable } from "svelte/store";
import type { Backend } from "./backend";
import type { FormatCatalogEntry, FormatDefinition } from "./formats";
import { acceptSave } from "./saveDrain.ts";
import { errorMessage } from "./errors.ts";

export interface FormatCatalogBackend {
  listFormats(): Promise<FormatCatalogEntry[]>;
  validateFormat(definition: FormatDefinition): Promise<void>;
  saveFormat(definition: FormatDefinition): Promise<FormatCatalogEntry>;
  deleteFormat(id: string): Promise<void>;
}

export const formats = writable<FormatCatalogEntry[]>([]);
export const formatCatalogError = writable<string | null>(null);
let activeBackend: Backend | null = null;
let loaded = false;
let revision = 0;
let loading: Promise<void> | null = null;
let writes = Promise.resolve();

export function formatService(backend: Backend): FormatCatalogBackend {
  const service = backend as Backend & Partial<FormatCatalogBackend>;
  if (!service.listFormats || !service.validateFormat || !service.saveFormat || !service.deleteFormat) {
    throw new Error("Pattern definitions are unavailable.");
  }
  return service as Backend & FormatCatalogBackend;
}

/** One catalog per app backend; loading a picker never starts a data query. */
export function ensureFormatsLoaded(backend: Backend, force = false): Promise<void> {
  if (activeBackend !== backend) {
    activeBackend = backend; loaded = false; loading = null; revision += 1;
    formats.set([]); formatCatalogError.set(null);
  }
  if (loading && !force) return loading;
  if (loaded && !force) return Promise.resolve();
  const generation = ++revision;
  const operation = (async () => {
    try {
      const entries = await formatService(backend).listFormats();
      if (activeBackend !== backend || revision !== generation) return;
      formats.set(entries.map(entry => ({ ...entry, definition: structuredClone(entry.definition) })));
      formatCatalogError.set(null); loaded = true;
    } catch (error) {
      if (activeBackend === backend && revision === generation) {
        formatCatalogError.set(errorMessage(error, "Could not load pattern definitions."));
      }
      throw error;
    }
  })();
  loading = operation;
  void operation.finally(() => { if (loading === operation) loading = null; }).catch(() => {});
  return operation;
}

function enqueue<T>(work: () => Promise<T>): Promise<T> {
  return acceptSave(() => {
    const next = writes.catch(() => {}).then(work);
    writes = next.then(() => {}, () => {});
    return next;
  });
}

export function saveCustomFormat(backend: Backend, definition: FormatDefinition): Promise<FormatCatalogEntry> {
  const snapshot = structuredClone(definition);
  return enqueue(async () => {
    await ensureFormatsLoaded(backend);
    const entry = await formatService(backend).saveFormat(snapshot);
    if (activeBackend === backend) {
      // A reload may have read the directory before this write committed.
      revision += 1;
      formats.update(current => {
        const next = current.filter(known => known.definition.id !== entry.definition.id);
        return [...next, { ...entry, definition: structuredClone(entry.definition) }]
          .sort((a, b) => Number(b.builtin) - Number(a.builtin) || a.definition.name.localeCompare(b.definition.name));
      });
    }
    return entry;
  });
}

export function deleteCustomFormat(backend: Backend, id: string): Promise<void> {
  return enqueue(async () => {
    await ensureFormatsLoaded(backend);
    await formatService(backend).deleteFormat(id);
    if (activeBackend === backend) {
      revision += 1;
      formats.update(current => current.filter(entry => entry.definition.id !== id));
    }
  });
}
