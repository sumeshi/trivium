import { writable } from 'svelte/store';
import { createOwnedRequests } from './workspaceRequests.ts';
import { errorMessage } from './errors.ts';
import type { ExtractBackend, ExtractPage, ExtractSources } from './extractTypes';

export const EXTRACT_PAGE_SIZE = 100;
export const EXTRACT_SOURCE_PAGE_SIZE = 50;
export interface ExtractViewState {
  page: ExtractPage | null;
  offset: number;
  loading: boolean;
  error: string | null;
  selectedValue: string | null;
  sources: ExtractSources | null;
  sourcesLoading: boolean;
  sourcesError: string | null;
  sourcesOffset: number;
}

/** One active result view keeps a single ranking page and one source page. */
export function createExtractView(backend: ExtractBackend, resultId: string) {
  const reads = createOwnedRequests();
  const state = writable<ExtractViewState>({ page: null, offset: 0, loading: false, error: null,
    selectedValue: null, sources: null, sourcesLoading: false, sourcesError: null, sourcesOffset: 0 });
  let pageRevision = 0, sourceRevision = 0;
  let disposed = false;

  async function loadPage(offset = 0) {
    if (disposed) return;
    const ticket = ++pageRevision;
    const safeOffset = Math.max(0, Math.floor(offset / EXTRACT_PAGE_SIZE) * EXTRACT_PAGE_SIZE);
    state.update(value => ({ ...value, page: null, loading: true, error: null, offset: safeOffset }));
    try {
      const page = await reads.run('page', backend, requestId => backend.extractValuesPage({
        resultId, offset: safeOffset, limit: EXTRACT_PAGE_SIZE, requestId,
      }));
      if (!disposed && pageRevision === ticket) state.update(value => ({ ...value, page, loading: false }));
    } catch (error) {
      if (!disposed && pageRevision === ticket) state.update(value => ({ ...value, loading: false,
        error: errorMessage(error, 'Could not load the extracted values.') }));
    }
  }

  async function selectValue(value: string, offset = 0) {
    if (disposed) return;
    const ticket = ++sourceRevision;
    const safeOffset = Math.max(0, Math.floor(offset / EXTRACT_SOURCE_PAGE_SIZE) * EXTRACT_SOURCE_PAGE_SIZE);
    state.update(current => ({ ...current, selectedValue: value, sources: null, sourcesOffset: safeOffset,
      sourcesLoading: true, sourcesError: null }));
    try {
      const sources = await reads.run('sources', backend, requestId => backend.extractValueSources({
        resultId, value, offset: safeOffset, limit: EXTRACT_SOURCE_PAGE_SIZE, requestId,
      }));
      if (!disposed && sourceRevision === ticket) state.update(current => ({ ...current, sources, sourcesLoading: false }));
    } catch (error) {
      if (!disposed && sourceRevision === ticket) state.update(current => ({ ...current, sourcesLoading: false,
        sourcesError: errorMessage(error, 'Could not load the original occurrences.') }));
    }
  }

  const closeSources = () => {
    sourceRevision += 1;
    reads.cancel('sources');
    state.update(value => ({ ...value, selectedValue: null, sources: null, sourcesOffset: 0,
      sourcesLoading: false, sourcesError: null }));
  };
  return { state: { subscribe: state.subscribe }, loadPage, selectValue, closeSources,
    dispose() { if (disposed) return; disposed = true; reads.dispose(); },
  };
}
