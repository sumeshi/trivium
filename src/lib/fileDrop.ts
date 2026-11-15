import { writable } from 'svelte/store';

export type FileDropEvent =
  | { type: 'enter' | 'drop'; paths: readonly string[] }
  | { type: 'over' }
  | { type: 'leave' };
export type FileDropState = { supported: number; ignored: number; blocked: string | null };
export const DATA_FILE_TYPES = 'CSV / JSON / JSONL / NDJSON';
export const UNSUPPORTED_DROP = `Only ${DATA_FILE_TYPES} files are supported.`;

/** Native file paths stay on disk. Dropping only stages the import form. */
export function createFileDrop(options: {
  blocked: () => string | null;
  select: (paths: string[]) => void;
  notify: (message: string) => void;
}) {
  const state = writable<FileDropState | null>(null);
  let hover: FileDropState | null = null;
  let disposed = false;
  const clear = () => { if (hover) { hover = null; state.set(null); } };
  const files = (paths: readonly string[]) => {
    const unique = [...new Set(paths)];
    const accepted = unique.filter(path => /\.(csv|json|jsonl|ndjson)$/i.test(path));
    return { accepted, ignored: unique.length - accepted.length };
  };
  return {
    state: { subscribe: state.subscribe },
    clear,
    handle(event: FileDropEvent) {
      if (disposed) return;
      if (event.type === 'leave') { clear(); return; }
      if (event.type === 'over') {
        if (!hover) return;
        const blocked = options.blocked();
        if (blocked !== hover.blocked) { hover = { ...hover, blocked }; state.set(hover); }
        return;
      }
      const { accepted, ignored } = files(event.paths);
      if (event.type === 'enter') {
        if (!event.paths.length) { clear(); return; }
        hover = { supported: accepted.length, ignored, blocked: options.blocked() };
        state.set(hover);
        return;
      }
      clear();
      const blocked = options.blocked();
      if (blocked) { options.notify(blocked); return; }
      if (accepted.length) options.select(accepted);
      if (ignored) options.notify(accepted.length
        ? `${ignored} unsupported item${ignored === 1 ? '' : 's'} skipped. ${UNSUPPORTED_DROP}`
        : UNSUPPORTED_DROP);
    },
    dispose() { clear(); disposed = true; },
  };
}
