import { get, writable } from 'svelte/store';
import { errorMessage } from './errors.ts';
import type { JobInfo, JobProgress, ProjectSummary } from './types';
import type { ExtractBackend, ExtractIntent, ExtractPending } from './extractTypes';

export interface ExtractScanState {
  pending: ExtractPending | null;
  error: { projectId: string; message: string } | null;
}

/** Keep only the in-flight request. Completed CSVs use normal project tabs. */
export function createExtractScan(backend: ExtractBackend, notify: (message: string) => void = () => {}) {
  const state = writable<ExtractScanState>({ pending: null, error: null });
  let revision = 0;
  let disposed = false;

  const cancel = () => {
    const pending = get(state).pending;
    if (!pending) return;
    revision += 1;
    state.update(value => ({ ...value, pending: null }));
    void backend.cancelRequest(pending.requestId).catch(() => {});
  };

  async function start(input: ExtractIntent): Promise<ProjectSummary | null> {
    if (disposed) return null;
    if (get(state).pending) throw new Error('An extraction is already running. Cancel it before starting another.');
    if (!input.request.columns.length) throw new Error('Choose at least one data column.');
    const intent = structuredClone(input);
    const ticket = ++revision;
    const requestId = crypto.randomUUID();
    state.set({ error: null, pending: { ...intent, requestId,
      jobId: null, done: 0, total: null, message: 'Scanning…' } });
    let result: ProjectSummary;
    try {
      result = await backend.extractValues({ ...intent.request, requestId });
    } catch (error) {
      if (!disposed && ticket === revision) {
        const message = errorMessage(error, 'Extraction failed.');
        state.set({ pending: null, error: { projectId: intent.request.projectId, message } });
        notify(message);
      }
      return null;
    }
    if (disposed || ticket !== revision) {
      // Cancellation can arrive after native atomic publication. The accepted
      // caller awaits cleanup, including when normal close is draining saves.
      await backend.deleteProject(result.meta.id);
      return null;
    }
    state.set({ pending: null, error: null });
    return result;
  }

  const updatePending = (change: (pending: ExtractPending) => ExtractPending) => {
    state.update(value => value.pending ? { ...value, pending: change(value.pending) } : value);
  };
  return {
    state: { subscribe: state.subscribe }, start, cancel,
    started(job: JobInfo) {
      const pending = get(state).pending;
      if (pending && job.requestId === pending.requestId) updatePending(value => ({ ...value, jobId: job.id }));
    },
    progress(progress: JobProgress) {
      if (get(state).pending?.jobId !== progress.id) return;
      updatePending(value => ({ ...value, done: progress.done, total: progress.total,
        message: progress.message || 'Scanning…' }));
    },
    finished(_job: JobInfo) { /* The owned IPC result completes the scan. */ },
    dispose() { if (disposed) return; cancel(); disposed = true; },
  };
}
