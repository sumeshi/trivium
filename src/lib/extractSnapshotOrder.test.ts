import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mockIPC, clearMocks } from '@tauri-apps/api/mocks';
import { createBackend } from './backend.ts';
import { flushAllSaves } from './saveDrain.ts';
import type { ExtractPayload } from './extractTypes';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(yes => { resolve = yes; });
  return { promise, resolve };
}
const turn = () => new Promise<void>(resolve => setImmediate(resolve));
const request = (projectId: string): ExtractPayload => ({ projectId, columns: ['c1'], scope: 'filtered',
  format: { schemaVersion: 1, id: 'test-pattern', name: 'Test', pattern: 'match' },
  search: 'original', flagFilter: 'safe', condition: null });
async function usingBackend(handler: Parameters<typeof mockIPC>[0], run: (backend: ReturnType<typeof createBackend>) => Promise<void>) {
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'window');
  Object.defineProperty(globalThis, 'window', { value: {}, configurable: true });
  mockIPC(handler);
  try { await run(createBackend()); }
  finally { clearMocks(); if (previous) Object.defineProperty(globalThis, 'window', previous); else Reflect.deleteProperty(globalThis, 'window'); }
}

test('extraction freezes its request after earlier annotations and before later writes', async () => {
  const saving = deferred(), scanning = deferred();
  const order: string[] = [];
  await usingBackend(async (command, wire) => {
    const payload = (wire as { payload: Record<string, unknown> }).payload;
    if (command === 'update_flag') {
      if (payload.flag === 'safe') await saving.promise;
      order.push(String(payload.flag)); return { entry: { flag: payload.flag }, revision: 1, flaggedRecords: 1 };
    }
    assert.equal(command, 'extract_values');
    assert.equal(payload.search, 'original');
    assert.deepEqual(payload.columns, ['c1']);
    assert.equal((payload.format as { pattern: string }).pattern, 'match');
    assert.equal(payload.requestId, undefined);
    order.push('scan'); await scanning.promise; return { resultId: 'snapshot' };
  }, async backend => {
    const first = backend.updateFlag({ projectId: 'extract-snapshot', rowIndex: 0, flag: 'safe' });
    const args = { ...request('extract-snapshot'), requestId: 'scan-owner' };
    const scan = backend.extractValues(args);
    const later = backend.updateFlag({ projectId: 'extract-snapshot', rowIndex: 0, flag: 'critical' });
    args.search = 'changed'; args.columns[0] = 'c2'; args.format.pattern = 'changed';
    await turn(); assert.deepEqual(order, []);
    saving.resolve(); await first; await turn(); assert.deepEqual(order, ['safe', 'scan']);
    scanning.resolve(); await Promise.all([scan, later]);
    assert.deepEqual(order, ['safe', 'scan', 'critical']);
    await flushAllSaves();
  });
});

test('cancelling an extraction queued behind a save never starts the scan or poisons closing', async () => {
  const saving = deferred();
  let scans = 0;
  await usingBackend(async command => {
    if (command === 'cancel_request_jobs') return 0;
    if (command === 'update_flag') { await saving.promise; return { entry: { flag: 'safe' }, revision: 1, flaggedRecords: 1 }; }
    if (command === 'extract_values') scans += 1;
    assert.fail(`Unexpected command ${command}`);
  }, async backend => {
    const first = backend.updateFlag({ projectId: 'extract-cancel-queued', rowIndex: 0, flag: 'safe' });
    const scan = backend.extractValues({ ...request('extract-cancel-queued'), requestId: 'cancel-before-scan' });
    const checked = assert.rejects(scan, /Cancelled/);
    await backend.cancelRequest('cancel-before-scan');
    saving.resolve(); await Promise.all([first, checked]);
    assert.equal(scans, 0);
    await flushAllSaves();
  });
});

test('a failed annotation stops extraction before target resolution and remains a genuine save error', async () => {
  let scans = 0;
  await usingBackend(command => {
    if (command === 'update_flag') throw new Error('save failed');
    scans += 1; assert.fail(`Unexpected command ${command}`);
  }, async backend => {
    await assert.rejects(backend.updateFlag({ projectId: 'extract-failed-save', rowIndex: 0, flag: 'safe' }));
    await assert.rejects(backend.extractValues(request('extract-failed-save')), /Extraction stopped because an annotation could not be saved/);
    assert.equal(scans, 0);
    await assert.rejects(flushAllSaves(), /save or export failed/);
    await flushAllSaves();
  });
});
