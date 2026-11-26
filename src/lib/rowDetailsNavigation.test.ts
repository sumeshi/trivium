import assert from 'node:assert/strict';
import test from 'node:test';
import { createRowDetailsNavigation } from './rowDetailsNavigation.ts';
import type { DetailNavigationState, DetailResultScope } from './rowDetailsNavigation';
import type { LocateProjectRowResponse, QueryProjectRowsResponse } from './backend';
import type { ProjectRow } from './types';

const row = (id: number): ProjectRow => ({ memo: null, color: null,  row_index: id, cells: {}, flag: '', tags: [], bookmarked: false });
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

test('previous and next follow fresh filtered descending result positions and stop at boundaries', async () => {
  const order = [98, 41, 6];
  let current = { scope: scope.key, projectId: scope.projectId, rowIndex: 41 };
  let state: DetailNavigationState | undefined;
  const opened: number[] = [];
  const reads: number[] = [];
  let locates = 0;
  const navigation = createRowDetailsNavigation({
    current: () => current,
    async locate(_request, id) {
      locates += 1;
      return { position: order.indexOf(id), total_rows: 100, total_filtered_rows: order.length };
    },
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
  assert.deepEqual(reads, [0]);
  assert.equal(state?.position, 0);
  await navigation.move(1);
  await navigation.move(1);
  await navigation.move(1);
  assert.deepEqual(opened, [98, 41, 6]);
  assert.deepEqual(reads, [0, 1, 2]);
  assert.equal(state?.position, 2);
  assert.equal(locates, 1);
  navigation.dispose();
});

test('opening a row locates once by original id and reads only the adjacent position', async () => {
  let current = { scope: scope.key, projectId: scope.projectId, rowIndex: 41 };
  let state: DetailNavigationState | undefined;
  let locates = 0;
  const navigation = createRowDetailsNavigation({
    current: () => current,
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
    current: () => current,
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
    current: () => current,
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

test('changed flag filters and annotation versions use fresh positions and row counts', async () => {
  let current = { scope: scope.key, projectId: scope.projectId, rowIndex: 41 };
  let state: DetailNavigationState | undefined;
  let locates = 0;
  let reads = 0;
  const opened: number[] = [];
  const navigation = createRowDetailsNavigation({
    current: () => current,
    async locate(request, id) {
      assert.equal(id, 41); locates += 1;
      return request.query.flagFilter === 'critical'
        ? { position: 0, total_rows: 100, total_filtered_rows: 2 }
        : { position: 1, total_rows: 100, total_filtered_rows: 3 };
    },
    async read(_request, position) { assert.equal(position, 1); reads += 1; return result(1, 87, 2); },
    cancel() {}, changed(next) { state = next; },
    open(next) { opened.push(next.row_index); current = { ...current, rowIndex: next.row_index }; },
  });
  await navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, scope);
  const changed: DetailResultScope = { ...scope, key: 'critical:new-annotation-version',
    query: { ...scope.query, flagFilter: 'critical' }, total: 3 };
  current = { ...current, scope: changed.key };
  await navigation.load({ projectId: 'case-a', rowIndex: 41, columnId: 'message' }, changed);
  assert.equal(state?.position, 0);
  assert.equal(state?.total, 2, 'the old table total does not override the located result count');
  await navigation.move(1);
  assert.deepEqual(opened, [87]);
  assert.equal(locates, 2);
  assert.equal(reads, 1);
  navigation.dispose();
});

test('first open after an annotation change ignores stale table positions and later metadata updates', async () => {
  // The table still shows Critical [0,1,2] while its refresh is debounced.
  // Row 0 was just changed to Safe, so the current result is already [1,2].
  const staleScope: DetailResultScope = { ...scope, key: 'critical:after-save', total: 3,
    query: { ...scope.query, flagFilter: 'critical' } };
  let current = { scope: staleScope.key, projectId: scope.projectId, rowIndex: 1 };
  let state: DetailNavigationState | undefined;
  let locates = 0;
  const reads: number[] = [];
  const opened: number[] = [];
  const navigation = createRowDetailsNavigation({
    current: () => current,
    async locate(_request, id) {
      assert.equal(id, 1); locates += 1;
      return { position: 0, total_rows: 3, total_filtered_rows: 2 };
    },
    async read(_request, position) { reads.push(position); return result(position, 2, 2); },
    cancel() {}, changed(next) { state = next; },
    open(next) { opened.push(next.row_index); current = { ...current, rowIndex: next.row_index }; },
  });
  await navigation.load({ projectId: 'case-a', rowIndex: 1, columnId: 'message' }, staleScope);
  assert.equal(state?.position, 0);
  assert.equal(state?.total, 2);
  // A refreshed table count has the same semantic key and must not disturb the
  // already fresh location or fetch it again.
  await navigation.load({ projectId: 'case-a', rowIndex: 1, columnId: 'message' }, { ...staleScope, total: 2 });
  await navigation.move(-1);
  assert.deepEqual(opened, [], 'Previous is a boundary, rather than reopening original row 1');
  assert.deepEqual(reads, []);
  await navigation.move(1);
  await navigation.load({ projectId: 'case-a', rowIndex: 2, columnId: 'message' }, { ...staleScope, total: 2 });
  assert.deepEqual(opened, [2]);
  assert.deepEqual(reads, [1]);
  assert.equal(locates, 1, 'own Next response and table count updates do not relaunch locate');
  navigation.dispose();
});
