import { get, writable } from 'svelte/store';
import type { Backend } from './backend';
import type { IocEntry, LoadProjectResponse } from './types';
import { createCellIocEntry, iocBulkArgs, iocEntryError, normalizeIocFlag, prepareIocEntries } from './iocRules.ts';
import { errorMessage } from './errors.ts';
import { acceptSave } from './saveDrain.ts';

export interface IocEditorState {
  entries: IocEntry[];
  busy: boolean;
  error: string;
  notice: string;
}

interface EditorOptions {
  backend: () => Backend | null;
  project: () => LoadProjectResponse | null;
  annotationsChanged: () => void;
  saved: (projectId: string, entries: IocEntry[]) => void;
}

/** A case owns its drafts and accepted operations independently of the drawer. */
export function createIocEditor(options: EditorOptions) {
  const state = writable<IocEditorState>({ entries: [], busy: false, error: '', notice: '' });
  let owner = '';
  let generation = 0;
  let disposed = false;
  let baseline: IocEntry[] = [];
  const definitions = (entries: readonly IocEntry[]) => entries.map(({ id: _, ...entry }) => structuredClone(entry));
  const withIds = (entries: readonly IocEntry[]) => definitions(entries).map(entry => ({ ...entry,
    flag: normalizeIocFlag(entry.flag), id: crypto.randomUUID() }));
  const publishEntries = (entries: IocEntry[]) => state.update(current => ({ ...current, entries, error: '', notice: '' }));
  const editable = () => !disposed && !get(state).busy;
  const report = (error: unknown) => state.update(current => ({ ...current, error: errorMessage(error), notice: '' }));
  const replaceSaved = (entries: IocEntry[]) => {
    const next = withIds(entries);
    baseline = definitions(next);
    state.update(current => ({ ...current, entries: next }));
    options.saved(owner, definitions(next));
  };
  const validated = (): IocEntry[] | null => {
    if (!editable()) return null;
    try {
      const entries = prepareIocEntries(definitions(get(state).entries));
      entries.forEach((entry, index) => {
        const error = iocEntryError(entry, options.project()?.columns ?? []);
        if (error) throw new Error(`IOC rule ${index + 1}: ${error}`);
      });
      return entries;
    }
    catch (error) { report(error); return null; }
  };

  async function run(work: (api: Backend, projectId: string, current: () => boolean) => Promise<void>): Promise<boolean> {
    if (!editable()) return false;
    const api = options.backend(), projectId = owner;
    if (!api || !projectId || options.project()?.project.meta.id !== projectId) {
      report(new Error('Investigation is unavailable.'));
      return false;
    }
    const ticket = generation;
    const current = () => !disposed && generation === ticket && owner === projectId;
    state.update(value => ({ ...value, busy: true, error: '', notice: '' }));
    try {
      // The complete accepted flow survives hiding or disposing its panel.
      await acceptSave(() => work(api, projectId, current));
      return true;
    } catch (error) {
      if (current()) report(error);
      return false;
    } finally {
      if (current()) state.update(value => ({ ...value, busy: false }));
    }
  }

  const notice = (message: string) => state.update(current => ({ ...current, notice: message }));
  const append = (entry: IocEntry): string => {
    if (!editable()) return '';
    const id = crypto.randomUUID();
    publishEntries([...get(state).entries, { ...structuredClone(entry), id }]);
    return id;
  };

  return {
    state: { subscribe: state.subscribe },
    initialize(projectId: string, entries: readonly IocEntry[]) {
      if (disposed) return;
      generation += 1;
      owner = projectId;
      const next = withIds(entries);
      baseline = definitions(next);
      state.set({ entries: next, busy: false, error: '', notice: '' });
    },
    add: () => append({ flag: 'critical', tag: '', query: '' }),
    addCell: (column: string, value: string) => append(createCellIocEntry(column, value)),
    update(id: string, field: 'flag' | 'tag' | 'query', value: string) {
      if (!editable()) return;
      publishEntries(get(state).entries.map(entry => entry.id !== id ? entry
        : { ...entry, [field]: field === 'flag' ? normalizeIocFlag(value) : value }));
    },
    updateCell(id: string, column: string, value: string) {
      if (!editable()) return;
      publishEntries(get(state).entries.map(entry => entry.id !== id ? entry
        : { ...entry, cell: { column, value } }));
    },
    remove(id: string) {
      if (editable()) publishEntries(get(state).entries.filter(entry => entry.id !== id));
    },
    discard() {
      if (editable()) publishEntries(withIds(baseline));
    },
    hasUnsavedChanges: () => !disposed && JSON.stringify(definitions(get(state).entries)) !== JSON.stringify(baseline),
    isBusy: () => !disposed && get(state).busy,
    save(): Promise<boolean> {
      const entries = validated();
      if (!entries) return Promise.resolve(false);
      return run(async (api, projectId, current) => {
        await api.saveIocs({ projectId, entries });
        if (current()) { replaceSaved(entries); notice('Rules saved.'); }
      });
    },
    importFile(path: string): Promise<boolean> {
      return run(async (api, projectId, current) => {
        const entries = await api.importIocs({ projectId, path });
        if (current()) { replaceSaved(entries); notice('Rules imported.'); }
      });
    },
    exportFile(destination: string): Promise<boolean> {
      const entries = validated();
      if (!entries) return Promise.resolve(false);
      if (!entries.length) { report(new Error('No rules to export.')); return Promise.resolve(false); }
      return run(async (api, projectId, current) => {
        await api.exportIocs({ projectId, destination, entries, allowOverwrite: true });
        if (current()) notice('Rules exported.');
      });
    },
    apply(id: string): Promise<boolean> {
      if (!editable()) return Promise.resolve(false);
      const entry = get(state).entries.find(rule => rule.id === id);
      if (!entry) return Promise.resolve(false);
      const error = iocEntryError(entry, options.project()?.columns ?? []);
      if (error) { report(new Error(error)); return Promise.resolve(false); }
      let args;
      try { args = iocBulkArgs(owner, structuredClone(entry)); }
      catch (error) { report(error); return Promise.resolve(false); }
      return run(async (api, _projectId, current) => {
        const result = await api.bulkApply(args, () => true);
        if (result && current()) {
          notice(`${result.updated.toLocaleString()} rows annotated.`);
          options.annotationsChanged();
        }
      });
    },
    undo(): Promise<boolean> {
      return run(async (api, projectId, current) => {
        const result = await api.bulkUndo({ projectId });
        if (current()) { notice(`${result.updated.toLocaleString()} rows restored.`); options.annotationsChanged(); }
      });
    },
    dispose() {
      disposed = true;
      generation += 1;
      owner = '';
      baseline = [];
      state.set({ entries: [], busy: false, error: '', notice: '' });
    },
  };
}
