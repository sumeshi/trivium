import assert from 'node:assert/strict';
import test from 'node:test';
import { get } from 'svelte/store';
import type { GetCellResult } from './backend';
import type { Cell } from './types';
import { createRowDetails, detailCopyValue, detailFilterValue } from './rowDetails.ts';
import type { DetailField } from './rowDetails';
import { createConditionState } from './stores/condition.ts';

const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const full = (text: string): GetCellResult => ({ text, kind: 'string', truncated: false, length: text.length });
const target = { projectId: 'case-a', rowIndex: 91, columnId: 'focus' };
const source = (text: string, truncated = false): Cell => ({ ...full(text), truncated });

function loaded(fields: ReadonlyMap<string, DetailField>, column: string): GetCellResult {
  const field = fields.get(column);
  assert.equal(field?.state, 'loaded');
  if (field?.state !== 'loaded') throw new Error('field is not loaded');
  return field.value;
}

test('one row expands only clicked/truncated/legacy fields, with at most two reads and exact text', async () => {
  let fields: ReadonlyMap<string, DetailField> = new Map();
  let running = 0, peak = 0;
  const calls: string[] = [];
  const pending = new Map<string, ReturnType<typeof deferred<GetCellResult>>>();
  const loader = createRowDetails({
    cancel() {}, changed(next) { fields = next; },
    read(request, column) {
      assert.deepEqual(request, target);
      calls.push(column); running += 1; peak = Math.max(peak, running);
      const requestResult = deferred<GetCellResult>(); pending.set(column, requestResult);
      return requestResult.promise.finally(() => { running -= 1; });
    },
  });
  const bigint = '18446744073709551615';
  const nanos = '2026-11-12T00:00:00.123456789Z';
  loader.load(target, ['bigint', 'nanos', 'wide', 'legacy', 'focus'], {
    bigint: source(bigint), nanos: source(nanos), wide: source('preview…', true),
    legacy: { text: 'old preview', kind: 'string' }, focus: source('focus preview', true),
  });
  assert.deepEqual(calls, ['focus', 'wide']);
  assert.equal(loaded(fields, 'bigint').text, bigint);
  assert.equal(loaded(fields, 'nanos').text, nanos);
  const exact = 'very long "quoted" value\n' + 'x'.repeat(1024);
  pending.get('focus')!.resolve(full(exact)); await turn();
  assert.deepEqual(calls, ['focus', 'wide', 'legacy']);
  pending.get('wide')!.resolve(full('all wide text')); pending.get('legacy')!.resolve(full('all legacy text'));
  await turn();
  assert.equal(peak, 2);
  assert.equal(loaded(fields, 'focus').text, exact);
  assert.equal(loaded(fields, 'wide').text, 'all wide text');
  assert.equal(detailFilterValue(loaded(fields, 'focus')), exact);
  assert.equal(detailCopyValue(loaded(fields, 'nanos')), nanos);
  loader.dispose(); assert.equal(fields.size, 0);
});

test('switching or closing a row cancels reads, clears full values, and rejects late replies', async () => {
  let fields: ReadonlyMap<string, DetailField> = new Map();
  let cancelled = 0;
  const old = deferred<GetCellResult>(), next = deferred<GetCellResult>();
  const calls: Array<{ projectId: string; rowIndex: number }> = [];
  const loader = createRowDetails({
    changed(value) { fields = value; }, cancel() { cancelled += 1; },
    read(request) { calls.push(request); return request.projectId === 'case-a' ? old.promise : next.promise; },
  });
  loader.load(target, ['focus'], {});
  loader.load({ projectId: 'case-b', rowIndex: 8, columnId: 'focus' }, ['focus'], {});
  old.resolve(full('obsolete case-a value')); await turn();
  assert.equal(fields.get('focus')?.state, 'loading');
  next.resolve(full('case-b value')); await turn();
  assert.equal(loaded(fields, 'focus').text, 'case-b value');
  loader.clear(); assert.equal(fields.size, 0);
  loader.dispose();
  loader.load(target, ['focus'], {});
  assert.equal(fields.size, 0);
  assert.equal(calls.length, 2);
  assert.ok(cancelled >= 3);
});

test('failed or truncated full reads never fall back to a preview and retry only that field', async () => {
  let fields: ReadonlyMap<string, DetailField> = new Map();
  let attempts = 0;
  const loader = createRowDetails({
    changed(next) { fields = next; }, cancel() {},
    async read() {
      attempts += 1;
      if (attempts === 1) throw new Error('read failed');
      if (attempts === 2) return { ...full('partial'), truncated: true };
      return full('exact fetched value');
    },
  });
  loader.load(target, ['focus', 'already-full'], { focus: source('preview', true), 'already-full': source('exact cached value') });
  await turn();
  assert.equal(fields.get('focus')?.state, 'error');
  assert.equal(detailCopyValue(undefined), null);
  loader.retry('focus'); await turn();
  assert.equal(fields.get('focus')?.state, 'error');
  loader.retry('focus'); await turn();
  assert.equal(loaded(fields, 'focus').text, 'exact fetched value');
  assert.equal(loaded(fields, 'already-full').text, 'exact cached value');
  assert.equal(attempts, 3);
  loader.dispose();
});

test('null, missing, empty and exact field filters keep their distinct meanings and preserve OR', () => {
  const explicitNull: GetCellResult = { text: null, kind: 'null', truncated: false, length: null };
  const missing: GetCellResult = { text: null, kind: 'missing', truncated: false, length: null };
  assert.equal(detailCopyValue(explicitNull), 'null');
  assert.equal(detailCopyValue(missing), null);
  assert.equal(detailFilterValue(explicitNull), null);
  assert.equal(detailFilterValue(missing), null);
  assert.equal(detailCopyValue(full('')), '');
  assert.equal(detailFilterValue(full('')), '');
  const conditions = createConditionState();
  conditions.conditionTree.set({ type: 'group', id: 'root', kind: 'or', collapsed: false, children: [
    { type: 'condition', id: 'a', column: 'host', op: 'eq', value: 'alpha', negated: false },
    { type: 'condition', id: 'b', column: 'host', op: 'eq', value: 'beta', negated: false },
  ] });
  const exact = 'c:\\evidence:"raw"\n' + '9'.repeat(400);
  conditions.addFilterCondition('message', 'ne', detailFilterValue(full(exact))!);
  const payload = get(conditions.conditionPayload);
  assert.ok(payload && 'kind' in payload);
  assert.equal(payload.kind, 'and');
  assert.deepEqual(payload.children[0], { kind: 'or', children: [
    { column: 'host', op: 'eq', values: ['alpha'], negated: false },
    { column: 'host', op: 'eq', values: ['beta'], negated: false },
  ] });
  assert.deepEqual(payload.children[1], { column: 'message', op: 'ne', values: [exact], negated: false });
});
