import { test } from 'node:test';
import assert from 'node:assert/strict';
import { formatIsoCell } from './dateDisplay.ts';
import type { Cell } from './types';

const cell = (text: string, extra: Partial<Cell> = {}): Cell => ({ text, kind: 'string', truncated: false, length: text.length, ...extra });

test('ISO display preserves nanoseconds and offset instants without changing source text', () => {
  for (const [source, expected] of [
    ['2026-11-14 01:02:03.123456789', '2026-11-14T01:02:03.123456789Z'],
    ['2026-11-14T00:00:00.000000001+09:00', '2026-11-13T15:00:00.000000001Z'],
    ['1969-12-31T23:59:59.999999999Z', '1969-12-31T23:59:59.999999999Z'],
    ['2024/02/29', '2024-02-29T00:00:00Z'],
  ]) {
    const value = cell(source), before = structuredClone(value);
    assert.deepEqual(formatIsoCell(value), { text: expected });
    assert.deepEqual(value, before);
    assert.equal(formatIsoCell(value), formatIsoCell(value));
  }
});

test('ISO display accepts engine-supported naive timestamp whitespace without altering evidence', () => {
  for (const date of ['2026-11-14', '2026/11/14']) {
    for (const separator of ['   ', '\t', '\n', '\r\n', '\v', '\f', ' \t\r\n ', '\u0085', '\u2002']) {
      const value = cell(`${date}${separator}03:04:05.123456789`), before = structuredClone(value);
      assert.deepEqual(formatIsoCell(value), { text: '2026-11-14T03:04:05.123456789Z' });
      assert.deepEqual(value, before);
    }
  }
  for (const source of [
    '2026-11-14  24:00:00',
    '2026/02/30   03:04:05',
    '2026-11-14   03:04:05 trailing',
    '2026-11-14\uFEFF03:04:05',
  ]) {
    const result = formatIsoCell(cell(source));
    assert.ok(result.error, source);
    assert.equal(result.text, source);
  }
});

test('flexible ISO display accepts unambiguous padding, minutes, fractions and explicit time zones', () => {
  for (const [source, expected] of [
    ['2026/1/4 3:4:5,123456789 UTC', '2026-01-04T03:04:05.123456789Z'],
    ['2026.1.4t3:4:5.123456789+0900', '2026-01-03T18:04:05.123456789Z'],
    ['2026-11-14   03:04', '2026-11-14T03:04:00Z'],
    ['2026-11-14   3:04:05', '2026-11-14T03:04:05Z'],
    ['2026-11-14   12:04:05.123456789+09:00', '2026-11-14T03:04:05.123456789Z'],
    ['2026/11/14\t03:04:05Z', '2026-11-14T03:04:05Z'],
    ['Sat, 14 Nov 2026 3:4:5,123456789 GMT', '2026-11-14T03:04:05.123456789Z'],
  ]) {
    const value = cell(source), before = structuredClone(value);
    assert.deepEqual(formatIsoCell(value), { text: expected }, source);
    assert.deepEqual(value, before);
  }
});

test('ISO display rejects invalid dates, ambiguous locale dates and truncated previews', () => {
  for (const source of ['2023-02-29', '2024-13-01', '2026-11-14T24:00:00Z', '11/12/2026', '1700000000', '2026-11-14T01:02:03.1234567891Z']) {
    const result = formatIsoCell(cell(source));
    assert.ok(result.error, source);
    assert.equal(result.text, source);
  }
  assert.ok(formatIsoCell(cell('2026-11-14', { truncated: true })).error);
  assert.deepEqual(formatIsoCell(cell('', { kind: 'null', text: null })), { text: 'null' });
  assert.deepEqual(formatIsoCell(cell('', { kind: 'missing', text: null })), { text: '—' });
  assert.deepEqual(formatIsoCell(cell('  ')), { text: '  ' });
});

