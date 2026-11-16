import { get, writable } from 'svelte/store';
import { errorMessage } from './errors.ts';
import type { JobInfo, JobProgress } from './types';
import type { ExtractBackend, ExtractIntent, ExtractPending, ExtractResult, ExtractTab } from './extractTypes';

export interface ExtractTabsState {
  tabs: ExtractTab[];
  pending: ExtractPending | null;
  error: { projectId: string; message: string } | null;
}

/** An analysis is not a saved project. Scans outlive their originating drawer. */
export function createExtractTabs(backend: ExtractBackend, notify: (message: string) => void = () => {}) {
  const state = writable<ExtractTabsState>({ tabs: [], pending: null, error: null });
  let revision = 0;
  let disposed = false;
  const release = async (resultId: string) => {
    try { await backend.releaseExtractResult(resultId); }
    catch (error) { if (!disposed) notify(errorMessage(error, 'Could not release the extracted result.')); }
  };

  const cancel = () => {
    const pending = get(state).pending;
    if (!pending) return;
    revision += 1;
    state.update(value => ({ ...value, pending: null }));
    void backend.cancelRequest(pending.requestId).catch(() => {});
  };

  async function start(input: ExtractIntent): Promise<ExtractTab | null> {
    if (disposed) return null;
    if (get(state).pending) throw new Error('An extraction is already running. Cancel it before starting another.');
    if (!input.request.columns.length) throw new Error('Choose at least one data column.');
    const intent = structuredClone(input);
    const ticket = ++revision;
    const requestId = crypto.randomUUID();
    state.update(value => ({ ...value, error: null, pending: { ...intent, requestId,
      jobId: null, done: 0, total: null, message: 'Scanning…' } }));
    let result: ExtractResult;
    try {
      result = await backend.extractValues({ ...intent.request, requestId });
    } catch (error) {
      if (!disposed && ticket === revision) {
        const message = errorMessage(error, 'Extraction failed.');
        state.update(value => ({ ...value, pending: null,
          error: { projectId: intent.request.projectId, message } }));
        notify(message);
      }
      return null;
    }
    if (disposed || ticket !== revision) {
      await release(result.resultId);
      return null;
    }
    const tab: ExtractTab = { ...intent, id: result.resultId, result };
    state.update(value => ({ ...value, pending: null, tabs: [...value.tabs, tab] }));
    return tab;
  }

  const close = async (id: string) => {
    if (!get(state).tabs.some(tab => tab.id === id)) return;
    state.update(value => ({ ...value, tabs: value.tabs.filter(tab => tab.id !== id) }));
    await release(id);
  };
  const closeProject = async (projectId: string) => {
    if (get(state).pending?.request.projectId === projectId) cancel();
    const ids = get(state).tabs.filter(tab => tab.request.projectId === projectId).map(tab => tab.id);
    state.update(value => ({ ...value, tabs: value.tabs.filter(tab => tab.request.projectId !== projectId) }));
    await Promise.all(ids.map(release));
  };
  const updatePending = (change: (pending: ExtractPending) => ExtractPending) => {
    state.update(value => value.pending ? { ...value, pending: change(value.pending) } : value);
  };

  return {
    state: { subscribe: state.subscribe }, start, cancel, close, closeProject,
    started(job: JobInfo) {
      const pending = get(state).pending;
      if (pending && job.requestId === pending.requestId) updatePending(value => ({ ...value, jobId: job.id }));
    },
    progress(progress: JobProgress) {
      if (get(state).pending?.jobId !== progress.id) return;
      updatePending(value => ({ ...value, done: progress.done, total: progress.total,
        message: progress.message || 'Scanning…' }));
    },
    finished(_job: JobInfo) { /* The owned IPC result, not a job event, completes the tab. */ },
    dispose() {
      if (disposed) return;
      cancel();
      disposed = true;
      const ids = get(state).tabs.map(tab => tab.id);
      state.set({ tabs: [], pending: null, error: null });
      for (const id of ids) void release(id);
    },
  };
}
