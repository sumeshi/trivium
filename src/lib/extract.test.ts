import assert from 'node:assert/strict';
import { test } from 'node:test';
import { get } from 'svelte/store';
import { createExtractTabs } from './extractTabs.ts';
import { createExtractView } from './extractView.ts';
import { extractFileName, extractValuePreview } from './extractPresentation.ts';
import { exportExtractWithConfirmation } from './extractExport.ts';
import type { ExtractBackend, ExtractIntent, ExtractPage, ExtractResult, ExtractSources } from './extractTypes.ts';

function deferred<T>() {
  let resolve!: (value: T) => void, reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const turn = () => new Promise(resolve => setImmediate(resolve));
function intent(id = 'case-a'): ExtractIntent {
  return { sourceName: '調査 one.csv', sourceColumns: [{ id: 'c1', label: 'CommandLine' }],
    request: { projectId: id, columns: ['c1'], scope: 'filtered', search: 'host:ws01',
      format: { schemaVersion: 1, id: 'ipv4', name: 'IPv4', pattern: '[0-9.]+', valueGroup: undefined } } };
}
function result(id: string, source = intent()): ExtractResult {
  return { resultId: id, projectId: source.request.projectId, format: structuredClone(source.request.format),
    columns: [...source.request.columns], scope: source.request.scope, sourceRows: 10, totalOccurrences: 3,
    matchedRows: 1, uniqueValues: 1, hasClassification: true };
}
function backend(overrides: Partial<ExtractBackend> = {}): ExtractBackend {
  return { extractValues: async args => result('result-a', { ...intent(args.projectId), request: args }),
    extractValuesPage: async args => ({ offset: args.offset, total: 1,
      rows: [{ value: '192.0.2.1', count: 3, rows: 1, scope: 'Special' }] }),
    extractValueSources: async args => ({ offset: args.offset, hasMore: false,
      rows: [{ rowIndex: 7, column: 'c1', start: 6, end: 15 }] }),
    exportExtract: async () => ({ rows: 1, columns: [], warnings: [] }),
    releaseExtractResult: async () => true, cancelRequest: async () => 1, ...overrides };
}

test('an extraction freezes its applied query and opens a metadata-only tab after completion', async () => {
  const pending = deferred<ExtractResult>();
  let wire: Parameters<ExtractBackend['extractValues']>[0] | null = null;
  const api = backend({ extractValues: args => { wire = args; return pending.promise; } });
  const tabs = createExtractTabs(api);
  const input = intent();
  const scan = tabs.start(input);
  input.request.search = 'changed'; input.request.columns.push('c2'); input.request.format.pattern = 'changed';
  assert.equal(get(tabs.state).tabs.length, 0);
  assert.equal(wire!.search, 'host:ws01');
  assert.deepEqual(wire!.columns, ['c1']);
  assert.equal(wire!.format.pattern, '[0-9.]+');
  pending.resolve(result('ready'));
  const tab = await scan;
  assert.equal(tab?.id, 'ready');
  assert.equal(tab?.request.search, 'host:ws01');
  assert.equal('page' in tab!, false);
  assert.equal('rows' in tab!, false);
  assert.equal(get(tabs.state).pending, null);
  tabs.dispose();
});

test('cancelled scans cannot overwrite a newer scan and late successful results are released', async () => {
  const old = deferred<ExtractResult>(), fresh = deferred<ExtractResult>();
  const released: string[] = [], cancelled: string[] = [];
  let calls = 0;
  const tabs = createExtractTabs(backend({ extractValues: () => ++calls === 1 ? old.promise : fresh.promise,
    cancelRequest: async id => { cancelled.push(id); return 1; },
    releaseExtractResult: async id => { released.push(id); return true; } }));
  const first = tabs.start(intent('a'));
  const oldRequest = get(tabs.state).pending!.requestId;
  tabs.cancel();
  const second = tabs.start(intent('b'));
  const newRequest = get(tabs.state).pending!.requestId;
  old.resolve(result('old', intent('a'))); await first;
  assert.deepEqual(released, ['old']);
  assert.deepEqual(cancelled, [oldRequest]);
  assert.equal(get(tabs.state).pending!.requestId, newRequest);
  fresh.resolve(result('new', intent('b'))); await second;
  assert.deepEqual(get(tabs.state).tabs.map(tab => tab.id), ['new']);
  await tabs.close('new');
  assert.deepEqual(released, ['old', 'new']);
  tabs.dispose();
});

test('scan progress is request-owned and failed scans stay errors rather than empty results', async () => {
  const pending = deferred<ExtractResult>();
  const notices: string[] = [];
  const tabs = createExtractTabs(backend({ extractValues: () => pending.promise }), message => notices.push(message));
  const scanning = tabs.start(intent());
  const requestId = get(tabs.state).pending!.requestId;
  tabs.started({ id: 1, kind: 'analysis', projectId: 'case-a', requestId: 'other-request' });
  tabs.progress({ id: 1, kind: 'analysis', done: 999, total: 999, message: 'Wrong job' });
  assert.equal(get(tabs.state).pending!.done, 0);
  tabs.started({ id: 2, kind: 'analysis', projectId: 'case-a', requestId });
  tabs.progress({ id: 2, kind: 'analysis', done: 4, total: 10, message: 'Scanning rows' });
  assert.equal(get(tabs.state).pending!.done, 4);
  pending.reject(new Error('The complete aggregation exceeded its memory limit.'));
  assert.equal(await scanning, null);
  assert.equal(get(tabs.state).tabs.length, 0);
  assert.match(get(tabs.state).error!.message, /memory limit/);
  assert.deepEqual(notices, ['The complete aggregation exceeded its memory limit.']);
  tabs.dispose();
});

test('closing a source case releases only its analysis tabs and cancels an owned scan', async () => {
  const released: string[] = [];
  let serial = 0;
  const tabs = createExtractTabs(backend({ extractValues: async args => result(`r${++serial}`, { ...intent(), request: args }),
    releaseExtractResult: async id => { released.push(id); return true; } }));
  await tabs.start(intent('a')); await tabs.start(intent('b')); await tabs.start(intent('a'));
  await tabs.closeProject('a');
  assert.deepEqual(get(tabs.state).tabs.map(tab => tab.result.projectId), ['b']);
  assert.deepEqual(released.sort(), ['r1', 'r3']);
  tabs.dispose(); await turn();
  assert.deepEqual(released.sort(), ['r1', 'r2', 'r3']);
});

test('a view ignores stale pages and stale source matches, and cancels only its owned reads', async () => {
  const pages = [deferred<ExtractPage>(), deferred<ExtractPage>()];
  const sources = [deferred<ExtractSources>(), deferred<ExtractSources>()];
  const cancelled: string[] = [];
  let pageCall = 0, sourceCall = 0;
  const view = createExtractView(backend({ extractValuesPage: () => pages[pageCall++].promise,
    extractValueSources: () => sources[sourceCall++].promise,
    cancelRequest: async id => { cancelled.push(id); return 1; } }), 'result');
  const first = view.loadPage(0), second = view.loadPage(100);
  pages[1].resolve({ offset: 100, total: 101, rows: [{ value: 'new', count: 1, rows: 1, scope: null }] }); await second;
  pages[0].resolve({ offset: 0, total: 1, rows: [{ value: 'stale', count: 9, rows: 1, scope: null }] }); await first;
  assert.equal(get(view.state).page!.rows[0].value, 'new');
  const firstSource = view.selectValue('192.0.2.1'), secondSource = view.selectValue('192.0.2.10');
  sources[1].resolve({ offset: 0, hasMore: false, rows: [{ rowIndex: 70, column: 'c2', start: 6, end: 16 }] }); await secondSource;
  sources[0].resolve({ offset: 0, hasMore: false, rows: [{ rowIndex: 1, column: 'c1', start: 0, end: 9 }] }); await firstSource;
  assert.equal(get(view.state).selectedValue, '192.0.2.10');
  assert.equal(get(view.state).sources!.rows[0].rowIndex, 70);
  assert.equal(cancelled.length, 2);
  view.dispose();
});

test('page failures are explicit, retries recover, and view disposal drops source reads', async () => {
  const source = deferred<ExtractSources>();
  let attempts = 0, cancelled = 0;
  const view = createExtractView(backend({ extractValuesPage: async () => {
    if (++attempts === 1) throw new Error('Extracted result expired. Run it again.');
    return { offset: 0, total: 0, rows: [] };
  }, extractValueSources: () => source.promise, cancelRequest: async () => { cancelled++; return 1; } }), 'expired');
  await view.loadPage();
  assert.equal(get(view.state).page, null);
  assert.match(get(view.state).error!, /expired/);
  await view.loadPage();
  assert.equal(get(view.state).error, null);
  assert.equal(get(view.state).page!.total, 0);
  const loading = view.selectValue('exact'); view.dispose();
  source.resolve({ offset: 0, hasMore: false, rows: [{ rowIndex: 1, column: 'c1', start: 0, end: 5 }] }); await loading;
  assert.equal(cancelled, 1);
  assert.equal(get(view.state).sources, null);
});

test('display previews and export names preserve raw values and Unicode filenames', () => {
  const raw = `最初\n${'x'.repeat(15)}😀tail`;
  assert.equal(extractValuePreview('192.0.2.1'), '192.0.2.1');
  assert.match(extractValuePreview(raw, 8), /↵.*…$/);
  assert.equal(extractValuePreview(`a😀b`, 2), 'a…');
  assert.equal(raw, `最初\n${'x'.repeat(15)}😀tail`);
  assert.equal(extractFileName('C:\\調査\\sample.csv', 'ipv4'), 'trivium-extract-ipv4-sample.csv');
  assert.equal(extractFileName('調査 one.jsonl', 'phone/jp'), 'trivium-extract-phone-jp-調査 one.csv');
});

test('extract export starts without overwrite permission and only retries after explicit confirmation', async () => {
  const calls: Array<Parameters<ExtractBackend['exportExtract']>[0]> = [];
  const api = backend({ exportExtract: async args => {
    calls.push(args);
    if (!args.allowOverwrite) throw new Error('Overwrite confirmation required: destination exists');
    return { rows: 4, columns: ['Value', 'Count', 'Rows', 'Scope'], warnings: [] };
  } });
  const input = { resultId: 'frozen-result', destination: '/selected/output.csv' };
  let prompts = 0;
  assert.equal(await exportExtractWithConfirmation(api, input, () => { prompts++; return false; }), null);
  assert.equal(prompts, 1);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].allowOverwrite, false);
  const exported = await exportExtractWithConfirmation(api, input, path => {
    assert.equal(path, '/selected/output.csv');
    input.resultId = 'later-result'; input.destination = '/later/output.csv';
    return true;
  });
  assert.equal(exported!.rows, 4);
  assert.deepEqual(calls.slice(1).map(({ resultId, destination, allowOverwrite }) => ({ resultId, destination, allowOverwrite })), [
    { resultId: 'frozen-result', destination: '/selected/output.csv', allowOverwrite: false },
    { resultId: 'frozen-result', destination: '/selected/output.csv', allowOverwrite: true },
  ]);
  assert.notEqual(calls[1].requestId, calls[2].requestId);
});

test('unrelated extract export errors never trigger an overwrite prompt', async () => {
  let prompted = false;
  await assert.rejects(exportExtractWithConfirmation(backend({ exportExtract: async () => {
    throw new Error('Project data files cannot be replaced.');
  } }), { resultId: 'result', destination: '/protected/data.parquet' }, () => { prompted = true; return true; }), /cannot be replaced/);
  assert.equal(prompted, false);
});
