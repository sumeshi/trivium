import assert from 'node:assert/strict';
import { test } from 'node:test';
import { get } from 'svelte/store';
import { createIocEditor } from './iocEditor.ts';
import { flushAllSaves } from './saveDrain.ts';
import type { Backend, BulkAnnotateArgs } from './backend';
import type { IocEntry, LoadProjectResponse } from './types';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const stored: IocEntry = { flag: 'critical', tag: 'review', query: 'message:beacon' };
const definitions = (entries: IocEntry[]) => entries.map(({ id: _, ...entry }) => entry);
function fixture(overrides: Partial<Backend> = {}) {
  const calls: Array<{ name: string; args: unknown }> = [];
  const saved: Array<{ projectId: string; entries: IocEntry[] }> = [];
  let changed = 0;
  let project = { project: { meta: { id: 'case-a' } }, columns: ['c000001', 'c1'], iocs: [structuredClone(stored)] } as LoadProjectResponse;
  const api = {
    saveIocs: async args => { calls.push({ name: 'save', args: structuredClone(args) }); },
    importIocs: async args => { calls.push({ name: 'import', args }); return [{ ...stored, tag: 'imported' }]; },
    exportIocs: async args => { calls.push({ name: 'export', args: structuredClone(args) }); },
    bulkApply: async args => { calls.push({ name: 'apply', args: structuredClone(args) }); return { updated: 2 }; },
    bulkUndo: async args => { calls.push({ name: 'undo', args }); return { updated: 2 }; },
    ...overrides,
  } as Backend;
  const editor = createIocEditor({ backend: () => api, project: () => project,
    annotationsChanged: () => { changed += 1; },
    saved: (projectId, entries) => { saved.push({ projectId, entries }); if (project.project.meta.id === projectId) project.iocs = entries; },
  });
  editor.initialize('case-a', [stored]);
  return { editor, api, calls, saved, get changed() { return changed; }, get project() { return project; },
    switchCase() {
      project = { ...project, project: { meta: { id: 'case-b' } } } as LoadProjectResponse;
      editor.initialize('case-b', []);
    } };
}

test('IOC drafts retain edits and exact cell values until explicit Save or Discard', () => {
  const f = fixture();
  assert.equal(f.editor.hasUnsavedChanges(), false);
  const queryId = f.editor.add();
  assert.equal(get(f.editor.state).entries.at(-1)?.flag, 'critical');
  f.editor.update(queryId, 'query', 'host:evil');
  const value = '  "quoted" | -value:\\path\n18446744073709551615  ';
  const cellId = f.editor.addCell('legacy column:"', value);
  f.editor.update(cellId, 'tag', 'triage');
  assert.deepEqual(get(f.editor.state).entries.at(-1)?.cell, { column: 'legacy column:"', value });
  f.editor.updateCell(cellId, '__proto__', '');
  assert.deepEqual(get(f.editor.state).entries.at(-1)?.cell, { column: '__proto__', value: '' });
  assert.equal(f.editor.hasUnsavedChanges(), true);
  assert.deepEqual(f.calls, [], 'creating or changing a rule must never annotate or save implicitly');
  f.editor.discard();
  assert.deepEqual(definitions(get(f.editor.state).entries), [stored]);
  assert.equal(f.editor.hasUnsavedChanges(), false);
  f.editor.dispose();
});

test('Save normalizes an owned snapshot and updates definitions without annotations or a case reload', async () => {
  const f = fixture();
  const id = get(f.editor.state).entries[0].id!;
  f.editor.update(id, 'query', ' host:evil ');
  f.editor.update(id, 'tag', ' investigated ');
  const value = ' "full source"\n ';
  f.editor.addCell('c000001', value);
  assert.equal(await f.editor.save(), true);
  const entries = [{ ...stored, query: 'host:evil', tag: 'investigated' },
    { flag: 'critical', tag: '', query: '', cell: { column: 'c000001', value } }];
  assert.deepEqual(f.calls, [{ name: 'save', args: { projectId: 'case-a', entries } }]);
  assert.deepEqual(f.saved, [{ projectId: 'case-a', entries }]);
  assert.equal(f.editor.hasUnsavedChanges(), false);
  assert.equal(f.changed, 0);
  assert.equal(get(f.editor.state).notice, 'Rules saved.');
  f.editor.update(get(f.editor.state).entries[0].id!, 'tag', 'new draft');
  assert.deepEqual(f.saved[0].entries, entries, 'subsequent drafts cannot mutate the saved snapshot');
  f.editor.dispose();
});

test('Export includes unsaved definitions while Import replaces the saved baseline without applying them', async () => {
  const f = fixture();
  f.editor.addCell('c1', '');
  assert.equal(await f.editor.exportFile('/export.csv'), true);
  assert.equal(f.editor.hasUnsavedChanges(), true);
  assert.deepEqual(f.calls[0], { name: 'export', args: { projectId: 'case-a', destination: '/export.csv',
    entries: [stored, { flag: 'critical', tag: '', query: '', cell: { column: 'c1', value: '' } }], allowOverwrite: true } });
  assert.deepEqual(f.project.iocs, [stored]);
  assert.equal(await f.editor.importFile('/import.csv'), true);
  assert.deepEqual(definitions(get(f.editor.state).entries), [{ ...stored, tag: 'imported' }]);
  assert.equal(f.editor.hasUnsavedChanges(), false);
  assert.deepEqual(f.saved[0], { projectId: 'case-a', entries: [{ ...stored, tag: 'imported' }] });
  assert.equal(f.changed, 0);
  f.editor.dispose();
});

test('an accepted IOC Apply freezes its case and full literal through panel teardown and save drain', async () => {
  await flushAllSaves().catch(() => {});
  const reply = deferred<{ updated: number }>();
  let target: BulkAnnotateArgs | undefined;
  let permits: (() => boolean) | undefined;
  const f = fixture({ bulkApply: async (args, isCurrent) => { target = structuredClone(args); permits = isCurrent; return reply.promise; } });
  const id = f.editor.addCell('c000001', '"literal" | -x:\\y\n');
  const applying = f.editor.apply(id);
  assert.equal(f.editor.isBusy(), true);
  assert.equal(await f.editor.save(), false, 'a busy rule operation admits no second write');
  f.editor.updateCell(id, 'different', 'changed');
  f.editor.dispose();
  assert.equal(permits?.(), true, 'an accepted Apply survives panel/case disposal');
  assert.equal(target?.projectId, 'case-a');
  assert.equal(target?.flag, 'critical');
  assert.equal(target?.search, '');
  assert.deepEqual(target?.condition, { column: 'c000001', op: 'eq', values: ['"literal" | -x:\\y\n'], negated: false });
  let drained = false;
  const draining = flushAllSaves().then(() => { drained = true; });
  await Promise.resolve();
  assert.equal(drained, false);
  reply.resolve({ updated: 2 });
  assert.equal(await applying, true);
  await draining;
  assert.equal(f.changed, 0, 'the old case cannot refresh a replacement screen');
  assert.deepEqual(get(f.editor.state).entries, []);
});

test('late IOC saves and errors cannot replace another case draft or its saved definitions', async () => {
  for (const fails of [false, true]) {
    const reply = deferred<void>();
    const f = fixture({ saveIocs: () => reply.promise });
    f.editor.update(get(f.editor.state).entries[0].id!, 'tag', 'old draft');
    const saving = f.editor.save();
    f.switchCase();
    const nextId = f.editor.add();
    f.editor.update(nextId, 'query', 'new-case-only');
    if (fails) reply.reject(new Error('old case write failed')); else reply.resolve();
    assert.equal(await saving, !fails);
    assert.deepEqual(f.saved, []);
    assert.equal(get(f.editor.state).entries[0].query, 'new-case-only');
    assert.equal(get(f.editor.state).error, '');
    assert.equal(f.editor.isBusy(), false);
    f.editor.dispose();
  }
});

test('failed rule saves keep the editable draft for retry and invalid rules never start a write', async () => {
  const f = fixture({ saveIocs: async () => { throw new Error('disk full'); } });
  const id = get(f.editor.state).entries[0].id!;
  f.editor.update(id, 'tag', 'draft');
  assert.equal(await f.editor.save(), false);
  assert.equal(f.editor.hasUnsavedChanges(), true);
  assert.equal(get(f.editor.state).error, 'disk full');
  f.editor.discard();
  assert.deepEqual(definitions(get(f.editor.state).entries), [stored]);
  const invalid = f.editor.add();
  f.editor.update(invalid, 'query', '|');
  assert.equal(await f.editor.apply(invalid), false);
  assert.equal(await f.editor.save(), false);
  assert.deepEqual(f.calls, []);
  assert.equal(f.saved.length, 0);
  f.editor.dispose();
});

test('unavailable cell columns cannot be saved, exported or applied until corrected', async () => {
  const f = fixture();
  const id = f.editor.addCell('other-case-column', 'source value');
  assert.equal(await f.editor.apply(id), false);
  assert.equal(await f.editor.save(), false);
  assert.equal(await f.editor.exportFile('/rules.csv'), false);
  assert.match(get(f.editor.state).error, /column is unavailable/);
  assert.deepEqual(f.calls, []);
  f.editor.updateCell(id, 'c1', 'source value');
  assert.equal(await f.editor.save(), true);
  assert.deepEqual(f.saved[0].entries[1].cell, { column: 'c1', value: 'source value' });
  f.editor.dispose();
});

test('Apply and Undo use explicit case-wide operations and leave the rule draft unsaved', async () => {
  const f = fixture();
  const id = get(f.editor.state).entries[0].id!;
  f.editor.update(id, 'tag', 'unsaved tag');
  assert.equal(await f.editor.apply(id), true);
  assert.equal(await f.editor.undo(), true);
  assert.deepEqual(f.calls.map(call => call.name), ['apply', 'undo']);
  const args = f.calls[0].args as BulkAnnotateArgs;
  assert.equal(args.target, 'filtered');
  assert.equal(args.projectId, 'case-a');
  assert.equal(args.search, stored.query);
  assert.deepEqual(args.searchColumns, []);
  assert.equal(args.flagFilter, 'all');
  assert.deepEqual(args.add_tags, ['unsaved tag']);
  assert.equal(f.changed, 2);
  assert.equal(f.editor.hasUnsavedChanges(), true);
  assert.deepEqual(f.saved, []);
  f.editor.dispose();
});
