import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { parseSourceTimestampNanos } from './sourceTimestamp.ts';
import { parseTimelineNanos, formatTimelineNanos } from './timelineRange.ts';
import { formatIsoCell } from './dateDisplay.ts';
import type { Cell } from './types';

test('source timestamp formats match the engine fixture without changing source values', () => {
  const cases = JSON.parse(readFileSync(new URL('../../tests/fixtures/datetime_formats.json', import.meta.url), 'utf8')) as Array<{ input: string; iso: string | null }>;
  for (const { input, iso } of cases) {
    const expected = iso === null ? null : parseTimelineNanos(iso);
    assert.equal(parseSourceTimestampNanos(input), expected, JSON.stringify(input));
    const cell: Cell = { text: input, kind: 'string', truncated: false, length: input.length };
    const before = structuredClone(cell);
    const formatted = formatIsoCell(cell);
    if (expected !== null) assert.deepEqual(formatted, { text: `${formatTimelineNanos(expected)}Z` }, JSON.stringify(input));
    else if (!/^\p{White_Space}*$/u.test(input)) {
      assert.ok(formatted.error, JSON.stringify(input));
      assert.equal(formatted.text, input);
    }
    assert.deepEqual(cell, before);
  }
});

test('source parsing enforces exact nanosecond boundaries and rejects excess precision and invalid normalization', () => {
  assert.equal(parseSourceTimestampNanos('1677-09-21T00:12:43.145224192Z'), -(1n << 63n));
  assert.equal(parseSourceTimestampNanos('2262-04-11T23:47:16.854775807Z'), (1n << 63n) - 1n);
  for (const input of [
    '1677-09-21T00:12:43.145224191Z', '2262-04-11T23:47:16.854775808Z',
    '2026-11-14T03:04:05.1234567890Z', '2026/11-14 03:04:05', '2026-2-30 03:04:05',
    '2026-11-14T03:04.5Z', '2026-11-14 03:04:60 UTC', '2026-11-14Z',
    'Fri, 14 Nov 2026 03:04:05 GMT', '14 Nov 2026 03:04:05 CST',
    '\uFEFF2026-11-14T03:04:05Z', '\uFEFF', `${' '.repeat(160)}2026-11-14`,
  ]) assert.equal(parseSourceTimestampNanos(input), null, input);
  assert.equal(parseSourceTimestampNanos('2026-11-14T03:04:05.000000001Z'), parseSourceTimestampNanos('2026/11/14 03:04:05,000000001 UTC'));
  assert.equal(parseSourceTimestampNanos('2026-11-14T03:04:05.000000002Z')! - parseSourceTimestampNanos('2026-11-14T03:04:05.000000001Z')!, 1n);
});
