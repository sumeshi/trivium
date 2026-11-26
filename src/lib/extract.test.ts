import assert from 'node:assert/strict';
import { test } from 'node:test';
import { get } from 'svelte/store';
import { createExtractScan } from './extractScan.ts';
import type { ExtractBackend, ExtractIntent } from './extractTypes.ts';
import type { ProjectSummary } from './types.ts';

function deferred<T>() {
  let resolve!: (value: T) => void, reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function intent(id = 'case-a'): ExtractIntent {
  return { sourceName: '調査 one.csv', request: { projectId: id, columns: ['c1'],
    scope: 'filtered', search: 'host:ws01',
    format: { schemaVersion: 1, id: 'ipv4', name: 'IPv4', pattern: '[0-9.]+' } } };
}
function summary(id: string): ProjectSummary {
  return { meta: { description: null, source_sha256: null, source_format: null, source_size: null, source_path: null, storage_version: 2, columns: [], id, name: 'Extract IPv4', source_file_name: 'Extract IPv4.csv',
    created_at: '2026-11-16', total_records: 4, flagged_records: 0,
    hidden_columns: [] } };
}
function backend(overrides: Partial<ExtractBackend> = {}): ExtractBackend {
  return { extractValues: async () => summary('generated-csv'),
    deleteProject: async () => {}, cancelRequest: async () => 1, ...overrides };
}

test('a scan freezes its applied query and returns a normal CSV project without retaining results', async () => {
  const pending = deferred<ProjectSummary>();
  let wire: Parameters<ExtractBackend['extractValues']>[0] | null = null;
  const scan = createExtractScan(backend({ extractValues: args => { wire = args; return pending.promise; } }));
  const input = intent();
  const operation = scan.start(input);
  input.request.search = 'changed'; input.request.columns.push('c2'); input.request.format.pattern = 'changed';
  assert.equal(wire!.search, 'host:ws01');
  assert.deepEqual(wire!.columns, ['c1']);
  assert.equal(wire!.format.pattern, '[0-9.]+');
  pending.resolve(summary('ready'));
  assert.equal((await operation)!.meta.id, 'ready');
  assert.deepEqual(get(scan.state), { pending: null, error: null });
  scan.dispose();
});

test('a cancelled late CSV is removed before completion and cannot overwrite a newer scan', async () => {
  const old = deferred<ProjectSummary>(), fresh = deferred<ProjectSummary>(), cleanup = deferred<void>();
  const removed: string[] = [], cancelled: string[] = [];
  let calls = 0;
  const scan = createExtractScan(backend({ extractValues: () => ++calls === 1 ? old.promise : fresh.promise,
    cancelRequest: async id => { cancelled.push(id); return 1; },
    deleteProject: async id => { removed.push(id); await cleanup.promise; } }));
  const first = scan.start(intent('a'));
  const oldRequest = get(scan.state).pending!.requestId;
  scan.cancel();
  const second = scan.start(intent('b'));
  const newRequest = get(scan.state).pending!.requestId;
  old.resolve(summary('old'));
  let settled = false;
  void first.then(() => { settled = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(settled, false, 'normal close must wait until the late generated CSV was removed');
  assert.deepEqual(removed, ['old']);
  assert.deepEqual(cancelled, [oldRequest]);
  assert.equal(get(scan.state).pending!.requestId, newRequest);
  cleanup.resolve(); assert.equal(await first, null);
  fresh.resolve(summary('new')); assert.equal((await second)!.meta.id, 'new');
  scan.dispose();
});

test('progress is request-owned and failed scans are errors rather than empty CSVs', async () => {
  const pending = deferred<ProjectSummary>();
  const notices: string[] = [];
  const scan = createExtractScan(backend({ extractValues: () => pending.promise }), message => notices.push(message));
  const operation = scan.start(intent());
  const requestId = get(scan.state).pending!.requestId;
  scan.started({ id: 1, kind: 'analysis', projectId: 'case-a', requestId: 'other-request' });
  scan.progress({ id: 1, kind: 'analysis', done: 999, total: 999, message: 'Wrong job' });
  assert.equal(get(scan.state).pending!.done, 0);
  scan.started({ id: 2, kind: 'analysis', projectId: 'case-a', requestId });
  scan.progress({ id: 2, kind: 'analysis', done: 4, total: 10, message: 'Scanning rows' });
  assert.equal(get(scan.state).pending!.done, 4);
  await assert.rejects(scan.start(intent('b')), /already running/);
  pending.reject(new Error('The complete aggregation exceeded its memory limit.'));
  assert.equal(await operation, null);
  assert.match(get(scan.state).error!.message, /memory limit/);
  assert.deepEqual(notices, ['The complete aggregation exceeded its memory limit.']);
  scan.dispose();
});

test('a cleanup failure after cancellation reaches the accepted close-drain caller', async () => {
  const pending = deferred<ProjectSummary>();
  const scan = createExtractScan(backend({ extractValues: () => pending.promise,
    deleteProject: async () => { throw new Error('Could not remove the temporary CSV.'); } }));
  const operation = scan.start(intent());
  scan.dispose();
  pending.resolve(summary('late'));
  await assert.rejects(operation, /Could not remove/);
});
