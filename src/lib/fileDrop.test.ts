import assert from 'node:assert/strict';
import { test } from 'node:test';
import { get } from 'svelte/store';
import { createFileDrop, UNSUPPORTED_DROP } from './fileDrop.ts';

function fixture() {
  let blocked: string | null = null;
  const selected: string[][] = [], notices: string[] = [];
  const drop = createFileDrop({ blocked: () => blocked,
    select: paths => selected.push(paths), notify: message => notices.push(message) });
  return { drop, selected, notices, block: (message: string | null) => { blocked = message; } };
}

test('native drops stage distinct supported paths without reading or importing files', () => {
  const { drop, selected, notices } = fixture();
  const paths = ['/tmp/調査 one.CSV', 'C:\\logs\\two.JSONL', '/tmp/調査 one.CSV', '/tmp/three.ndjson', '/tmp/four.json'];
  drop.handle({ type: 'enter', paths });
  assert.deepEqual(get(drop.state), { supported: 4, ignored: 0, blocked: null });
  assert.deepEqual(selected, []);
  drop.handle({ type: 'drop', paths });
  assert.equal(get(drop.state), null);
  assert.deepEqual(selected, [[paths[0], paths[1], paths[3], paths[4]]]);
  assert.deepEqual(notices, []);
});

test('leave, Escape-style clear and disposal never change the pending selection', () => {
  const { drop, selected } = fixture();
  for (const cancel of [() => drop.handle({ type: 'leave' }), () => drop.clear()]) {
    drop.handle({ type: 'enter', paths: ['one.csv'] });
    cancel(); assert.equal(get(drop.state), null);
  }
  drop.handle({ type: 'enter', paths: ['one.csv'] });
  drop.dispose();
  drop.handle({ type: 'drop', paths: ['one.csv'] });
  assert.equal(get(drop.state), null);
  assert.deepEqual(selected, []);
});

test('unsupported files are reported and mixed drops stage only supported paths', () => {
  const { drop, selected, notices } = fixture();
  drop.handle({ type: 'enter', paths: ['notes.txt'] });
  assert.deepEqual(get(drop.state), { supported: 0, ignored: 1, blocked: null });
  drop.handle({ type: 'drop', paths: ['notes.txt'] });
  assert.deepEqual(selected, []);
  assert.equal(notices[0], UNSUPPORTED_DROP);
  drop.handle({ type: 'drop', paths: ['notes.txt', 'one.csv', 'notes.txt'] });
  assert.deepEqual(selected, [['one.csv']]);
  assert.equal(notices[1], `1 unsupported item skipped. ${UNSUPPORTED_DROP}`);
});

test('busy, closing or dialog state is checked again at drop time', () => {
  const { drop, selected, notices, block } = fixture();
  drop.handle({ type: 'enter', paths: ['one.csv'] });
  block('An import is already running.');
  drop.handle({ type: 'over' });
  assert.equal(get(drop.state)?.blocked, 'An import is already running.');
  block('Close the dialog before adding files.');
  drop.handle({ type: 'drop', paths: ['one.csv'] });
  assert.deepEqual(notices, ['Close the dialog before adding files.']);
  assert.deepEqual(selected, []);
  assert.equal(get(drop.state), null);
  block(null);
  drop.handle({ type: 'drop', paths: ['one.csv'] });
  assert.deepEqual(selected, [['one.csv']]);
});

test('over events neither activate an internal drag nor publish every mouse movement', () => {
  const { drop, block } = fixture();
  let changes = 0;
  const stop = drop.state.subscribe(() => changes++);
  drop.handle({ type: 'over' });
  drop.handle({ type: 'enter', paths: [] });
  assert.equal(changes, 1);
  drop.handle({ type: 'enter', paths: ['one.csv'] });
  for (let i = 0; i < 100; i++) drop.handle({ type: 'over' });
  assert.equal(changes, 2);
  block('Saving…');
  drop.handle({ type: 'over' });
  assert.equal(changes, 3);
  stop(); drop.dispose();
});
