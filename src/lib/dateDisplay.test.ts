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

