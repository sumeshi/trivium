import { writable } from 'svelte/store';
import type { Backend, PreviewImportArgs } from './backend';
import type { ImportPreview } from './types';
import { errorMessage } from './errors.ts';
import { createOwnedRequests } from './workspaceRequests.ts';

export interface ImportPreviewState {
  loading: boolean;
  result: ImportPreview | null;
  error: string;
}

/** Only an enabled preview supplies a request; no file reads happen otherwise. */
export function createImportPreview(backend: Pick<Backend, 'previewImport' | 'cancelRequest'>) {
  const state = writable<ImportPreviewState>({ loading: false, result: null, error: '' });
  const reads = createOwnedRequests();
  let signature = '';
  let generation = 0;
  let disposed = false;

  function configure(request: Omit<PreviewImportArgs, 'requestId'> | null): void {
    if (disposed) return;
    const snapshot = request ? structuredClone(request) : null;
    const next = JSON.stringify(snapshot);
    if (signature === next) return;
    signature = next;
    const owner = ++generation;
    reads.cancelAll();
    state.set({ loading: snapshot !== null, result: null, error: '' });
    if (!snapshot) return;
    void reads.run('import-preview', backend,
      requestId => backend.previewImport({ ...snapshot, requestId }))
      .then(result => {
        if (!disposed && owner === generation) state.set({ loading: false, result, error: '' });
      }, error => {
        if (!disposed && owner === generation) state.set({
          loading: false, result: null, error: errorMessage(error, 'Preview unavailable.'),
        });
      });
  }

  return {
    state: { subscribe: state.subscribe },
    configure,
    dispose() {
      if (disposed) return;
      disposed = true;
      generation += 1;
      reads.dispose();
    },
  };
}
