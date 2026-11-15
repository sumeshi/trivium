import assert from 'node:assert/strict';
import test from 'node:test';
import { createRowDetailsNavigation } from './rowDetailsNavigation.ts';
import type { DetailNavigationState, DetailResultScope } from './rowDetailsNavigation';
import type { LocateProjectRowResponse, QueryProjectRowsResponse } from './backend';
import type { ProjectRow } from './types';

const row = (id: number): ProjectRow => ({ row_index: id, cells: {}, flag: '', tags: [], bookmarked: false });
const result = (position: number, id: number, total = 3): QueryProjectRowsResponse => ({
  offset: position, rows: [row(id)], total_rows: 100, total_filtered_rows: total,
  annotated_total: 0, annotated_filtered: 0, annotation_revision: 0,
});
const scope: DetailResultScope = { key: 'case-a:filtered-desc', projectId: 'case-a', total: 3, query: {
  search: 'keep', searchColumns: ['message'], flagFilter: 'all', bookmarkedOnly: false,
  condition: null, sortKey: 'score', sortDirection: 'desc',
} };
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((yes) => { resolve = yes; });
  return { promise, resolve };
}

test('previous and next follow filtered descending result positions, reuse cache, and stop at boundaries', async () => {
  const order = [98, 41, 6];
  let current = { scope: scope.key, projectId: scope.projectId, rowIndex: 41 };
  let state: DetailNavigationState | undefined;
  const opened: number[] = [];
  const reads: number[] = [];
  let locates = 0;
  const navigation = createRowDetailsNavigation({
    current: () => current,
    position: (id) => order.indexOf(id),
    row: (position) => position === 0 ? row(98) : undefined,
    async locate() { locates += 1; throw new Error('Cached positions need no locate.'); },
    async read(request, position) {
      assert.deepEqual(request.query, scope.query);
      reads.push(position); return result(position, order[position]);
    },
    cancel() {}, changed(next) { state = next; },
    open(next) { opened.push(next.row_index); current = { ...current, rowIndex: next.row_index }; },
  });
  await navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, scope);
  await navigation.move(-1);
  await navigation.move(-1);
  assert.deepEqual(opened, [98]);
  assert.deepEqual(reads, []);
  assert.equal(state?.position, 0);
  await navigation.move(1);
  await navigation.move(1);
  await navigation.move(1);
  assert.deepEqual(opened, [98, 41, 6]);
  assert.deepEqual(reads, [1, 2]);
  assert.equal(state?.position, 2);
  assert.equal(locates, 0);
  navigation.dispose();
});

test('a row outside the page cache locates once by original id and reads only the adjacent position', async () => {
  let current = { scope: scope.key, projectId: scope.projectId, rowIndex: 41 };
  let state: DetailNavigationState | undefined;
  let locates = 0;
  const navigation = createRowDetailsNavigation({
    current: () => current, position: () => null, row: () => undefined,
    async locate(request, id) {
      assert.equal(id, 41); assert.equal(request.query.sortDirection, 'desc'); locates += 1;
      return { position: 1, total_rows: 100, total_filtered_rows: 3 };
    },
    async read(_request, position) { assert.equal(position, 2); return result(2, 6); },
    cancel() {}, changed(next) { state = next; },
    open(next) { current = { ...current, rowIndex: next.row_index }; },
  });
  await navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, scope);
  await navigation.move(1);
  // The dialog reacting to its new row must not issue another locate.
  await navigation.load({ projectId: 'case-a', rowIndex: 6, columnId: 'message' }, scope);
  assert.equal(locates, 1);
  assert.equal(state?.position, 2);
  navigation.dispose();
});

test('late locate and page responses cannot change a closed dialog, new tab or new filter result', async () => {
  let current: { scope: string; projectId: string; rowIndex: number } | null = {
    scope: scope.key, projectId: scope.projectId, rowIndex: 41,
  };
  const located = deferred<LocateProjectRowResponse>();
  const page = deferred<QueryProjectRowsResponse>();
  let state: DetailNavigationState | undefined;
  let cancelled = 0;
  const opened: number[] = [];
  const navigation = createRowDetailsNavigation({
    current: () => current, position: () => null, row: () => undefined,
    locate: () => located.promise, read: () => page.promise,
    cancel() { cancelled += 1; }, changed(next) { state = next; },
    open(next) { opened.push(next.row_index); },
  });
  const oldLocate = navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, scope);
  current = { scope: 'case-b:new', projectId: 'case-b', rowIndex: 2 };
  navigation.clear();
  located.resolve({ position: 1, total_rows: 100, total_filtered_rows: 3 });
  await oldLocate;
  assert.equal(state?.position, null);
  current = { scope: scope.key, projectId: scope.projectId, rowIndex: 41 };
  await navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, scope);
  const oldPage = navigation.move(1);
  current = { ...current, scope: 'different-filter' };
  navigation.clear();
  page.resolve(result(2, 6)); await oldPage;
  assert.deepEqual(opened, []);
  assert.equal(state?.position, null);
  current = null;
  assert.ok(cancelled >= 3);
  navigation.dispose();
});

test('missing rows and failed adjacent reads keep the original row and filters', async () => {
  const current = { scope: scope.key, projectId: scope.projectId, rowIndex: 41 };
  let state: DetailNavigationState | undefined;
  let matching = false;
  const navigation = createRowDetailsNavigation({
    current: () => current, position: () => null, row: () => undefined,
    async locate() { return { position: matching ? 1 : null, total_rows: 100, total_filtered_rows: 3 }; },
    async read() { throw new Error('read failed'); },
    cancel() {}, changed(next) { state = next; },
    open() { throw new Error('Failed navigation must not open another row.'); },
  });
  await navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, scope);
  assert.match(state?.error ?? '', /not in the current results/);
  navigation.clear(); matching = true;
  await navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, scope);
  await navigation.move(1);
  assert.equal(state?.position, 1);
  assert.equal(state?.busy, false);
  assert.equal(state?.error, 'read failed');
  assert.equal(current.rowIndex, 41);
  assert.equal(scope.query.search, 'keep');
  navigation.dispose();
});

test('changed flag filters and annotation versions ignore the old table position and row caches', async () => {
  let current = { scope: scope.key, projectId: scope.projectId, rowIndex: 41 };
  let state: DetailNavigationState | undefined;
  let locates = 0;
  let reads = 0;
  const opened: number[] = [];
  const navigation = createRowDetailsNavigation({
    current: () => current,
    // Old table cache says row 41 is second, followed by row 6.
    position: () => 1, row: () => row(6),
    async locate(request, id) {
      assert.equal(request.query.flagFilter, 'critical'); assert.equal(id, 41); locates += 1;
      return { position: 0, total_rows: 100, total_filtered_rows: 2 };
    },
    async read(_request, position) { assert.equal(position, 1); reads += 1; return result(1, 87, 2); },
    cancel() {}, changed(next) { state = next; },
    open(next) { opened.push(next.row_index); current = { ...current, rowIndex: next.row_index }; },
  });
  await navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, scope);
  const changed: DetailResultScope = { ...scope, key: 'critical:new-annotation-version',
    query: { ...scope.query, flagFilter: 'critical' }, total: 2 };
  current = { ...current, scope: changed.key };
  await navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, changed);
  assert.equal(state?.position, 0);
  await navigation.move(1);
  assert.deepEqual(opened, [87]);
  assert.equal(locates, 1);
  assert.equal(reads, 1);
  navigation.dispose();
});
