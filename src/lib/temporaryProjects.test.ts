import assert from 'node:assert/strict';
import { test } from 'node:test';
import { get } from 'svelte/store';
import { createProjectController } from './stores/projects.ts';
import type { Backend } from './backend.ts';
import type { ProjectSummary, LoadProjectResponse } from './types.ts';

function summary(id: string): ProjectSummary {
  return { meta: { id, name: id, source_file_name: `${id}.csv`, created_at: '2026-11-16',
    total_records: 3, flagged_records: 0, ioc_applied_records: 0, hidden_columns: [] } };
}
function detail(id: string): LoadProjectResponse {
  return { project: summary(id), columns: ['c1'], columns_meta: [], column_max_chars: {}, column_kinds: {},
    color_rules: [], iocs: [], hidden_columns: [], initial_rows: [] };
}
function fixture(overrides: Partial<Backend> = {}) {
  const loaded: string[] = [], deleted: string[] = [];
  const backend = { listProjects: async () => [summary('source')],
    loadProject: async (id: string) => { loaded.push(id); return detail(id); },
    deleteProject: async (id: string) => { deleted.push(id); }, ...overrides } as Backend;
  return { controller: createProjectController(backend), loaded, deleted };
}

test('generated CSVs open in the ordinary controller and survive Recent refreshes', async () => {
  const f = fixture();
  await f.controller.loadProjects(); await f.controller.selectProject('source');
  await f.controller.adoptTemporaryProject(summary('extract'), true);
  await f.controller.loadProjects(true);
  const state = get(f.controller.state);
  assert.equal(state.selectedProjectId, 'extract');
  assert.equal(state.projectDetail!.project.meta.id, 'extract');
  assert.deepEqual(state.temporaryProjectIds, ['extract']);
  assert.deepEqual(state.openProjectIds, ['source', 'extract']);
  assert.deepEqual(state.projects.filter(project => !state.temporaryProjectIds.includes(project.meta.id)).map(project => project.meta.id), ['source']);
  assert.deepEqual(f.loaded, ['source', 'extract']);
  f.controller.dispose();
});

test('inactive generated CSVs retain only summaries and do not steal the current tab', async () => {
  const f = fixture();
  await f.controller.selectProject('source');
  const original = summary('extract');
  await f.controller.adoptTemporaryProject(original, false);
  original.meta.name = 'changed outside';
  assert.equal(get(f.controller.state).selectedProjectId, 'source');
  assert.deepEqual(f.loaded, ['source']);
  assert.equal(get(f.controller.state).projects.find(project => project.meta.id === 'extract')!.meta.name, 'extract');
  f.controller.dispose();
});

test('deleting a generated tab waits for teardown and forgets it only after successful deletion', async () => {
  let succeeds = false, teardown = false;
  const f = fixture({ deleteProject: async id => {
    assert.equal(id, 'extract'); assert.ok(teardown);
    if (!succeeds) throw new Error('Could not remove generated data.');
  } });
  await f.controller.selectProject('source');
  await f.controller.adoptTemporaryProject(summary('extract'), true);
  await assert.rejects(f.controller.deleteProject('extract', async () => { teardown = true; }), /Could not remove/);
  assert.deepEqual(get(f.controller.state).temporaryProjectIds, ['extract']);
  assert.ok(get(f.controller.state).openProjectIds.includes('extract'));
  assert.equal(get(f.controller.state).selectedProjectId, 'extract');
  succeeds = true;
  await f.controller.deleteProject('extract', async () => { teardown = true; });
  await f.controller.loadProjects(true);
  assert.deepEqual(get(f.controller.state).temporaryProjectIds, []);
  assert.deepEqual(get(f.controller.state).openProjectIds, ['source']);
  assert.ok(!get(f.controller.state).projects.some(project => project.meta.id === 'extract'));
  f.controller.dispose();
});

test('normal close removes inactive generated CSVs before the flushed active one without loading a new case', async () => {
  const f = fixture();
  await f.controller.selectProject('source');
  await f.controller.adoptTemporaryProject(summary('active'), true);
  await f.controller.adoptTemporaryProject(summary('inactive'), false);
  const loads = [...f.loaded];
  await f.controller.cleanupTemporaryProjects(async () => {});
  assert.deepEqual(f.deleted, ['inactive', 'active']);
  assert.deepEqual(f.loaded, loads);
  assert.deepEqual(get(f.controller.state).temporaryProjectIds, []);
  assert.deepEqual(get(f.controller.state).openProjectIds, ['source']);
  assert.equal(get(f.controller.state).selectedProjectId, null);
  f.controller.dispose();
});

test('failed close cleanup retains its generated case and tab for a later retry', async () => {
  const f = fixture({ deleteProject: async () => { throw new Error('Storage is unavailable.'); } });
  await f.controller.adoptTemporaryProject(summary('extract'), true);
  await assert.rejects(f.controller.cleanupTemporaryProjects(), /Storage is unavailable/);
  assert.deepEqual(get(f.controller.state).temporaryProjectIds, ['extract']);
  assert.equal(get(f.controller.state).selectedProjectId, 'extract');
  assert.ok(get(f.controller.state).openProjectIds.includes('extract'));
  f.controller.dispose();
});
