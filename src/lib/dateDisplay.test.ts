import { test } from 'node:test';
import assert from 'node:assert/strict';
import { formatIsoCell } from './dateDisplay.ts';
import { copyText } from './ipcValues.ts';
import type { Cell } from './types';

const cell = (text: string, extra: Partial<Cell> = {}): Cell => ({ text, kind: 'string', truncated: false, length: text.length, ...extra });

test('ISO display uses the recognized datetime key for padded source text without changing copy text', () => {
  const source = `${'\u2002'.repeat(80)}2026/11/14\t03:04:05,123456789 UTC${' '.repeat(80)}`;
  const value = cell(source, { datetime_nanos: '1794625445123456789' });
  const before = structuredClone(value);
  assert.deepEqual(formatIsoCell(value), { text: '2026-11-14T03:04:05.123456789Z' });
  assert.equal(copyText(value), source);
  assert.deepEqual(value, before);
});

test('ISO display uses a recognized datetime key even when the source preview is truncated', () => {
  const source = `${' '.repeat(200)}2026/11/14 03:04:05,123456789 UTC`;
  const value = cell(source.slice(0, 64), { datetime_nanos: '1794625445123456789', truncated: true, length: source.length });
  const before = structuredClone(value);
  assert.deepEqual(formatIsoCell(value), { text: '2026-11-14T03:04:05.123456789Z' });
  assert.deepEqual(value, before);
  const full = cell(source, { datetime_nanos: '1794625445123456789' });
  assert.equal(copyText(full), source);
});

test('ISO display formats exact engine datetime keys without changing source or copy text', () => {
  for (const [source, datetime_nanos, expected] of [
    ['2026-11-14 01:02:03.123456789', '1794618123123456789', '2026-11-14T01:02:03.123456789Z'],
    ['2026-11-14T00:00:00.000000001+09:00', '1794582000000000001', '2026-11-13T15:00:00.000000001Z'],
    ['1969-12-31T23:59:59.999999999Z', '-1', '1969-12-31T23:59:59.999999999Z'],
    ['1970-01-01', '0', '1970-01-01T00:00:00Z'],
    ['2024/02/29', '1709164800000000000', '2024-02-29T00:00:00Z'],
    ['1677-09-21T00:12:43.145224192Z', '-9223372036854775808', '1677-09-21T00:12:43.145224192Z'],
    ['2262-04-11T23:47:16.854775807Z', '9223372036854775807', '2262-04-11T23:47:16.854775807Z'],
  ]) {
    const value = Object.freeze(cell(source, { datetime_nanos })), before = structuredClone(value);
    assert.deepEqual(formatIsoCell(value), { text: expected });
    assert.equal(copyText(value), source);
    assert.deepEqual(value, before);
    assert.equal(formatIsoCell(value), formatIsoCell(value));
  }
});

test('ISO display does not infer datetime recognition from source text when metadata is absent', () => {
  for (const source of ['2026-11-14T03:04:05.123456789Z', '2026/11/14 03:04:05 UTC', '11/12/2026', '18446744073709551615', 'ordinary text']) {
    const value = cell(source), before = structuredClone(value);
    assert.deepEqual(formatIsoCell(value), { text: source, error: 'Unrecognized timestamp.' });
    assert.equal(copyText(value), source);
    assert.deepEqual(value, before);
  }
});

test('truncated source previews without a datetime key keep the preview error', () => {
  const source = '2026-11-14';
  const value = cell(source, { truncated: true });
  assert.deepEqual(formatIsoCell(value), { text: `${source}…`, error: 'Timestamp preview is truncated.' });
  assert.equal(value.text, source);
});

test('ISO display keeps null, missing, empty and whitespace values unchanged', () => {
  assert.deepEqual(formatIsoCell(null), { text: '' });
  assert.deepEqual(formatIsoCell(undefined), { text: '' });
  assert.deepEqual(formatIsoCell(cell('', { kind: 'null', text: null })), { text: 'null' });
  assert.deepEqual(formatIsoCell(cell('', { kind: 'missing', text: null })), { text: '—' });
  for (const source of ['', '  ', '\t\r\n\u2002']) {
    assert.deepEqual(formatIsoCell(cell(source)), { text: source });
  }
});

