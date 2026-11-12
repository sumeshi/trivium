import assert from "node:assert/strict";
import { test } from "node:test";
import type { HistogramBucket } from "./types";
import { initialTimelineBucket, timelineAxisTicks, timelineBucketSelection, timelineFlagMarkers } from "./timelinePresentation.ts";
import { parseTimelineNanos } from "./timelineRange.ts";

function bucket(flags: HistogramBucket["flags"]): HistogramBucket {
  return { start: "2026-11-12T00:00:00.000000001", end: "2026-11-12T00:00:00.000000101", end_inclusive: false,
    count: Object.values(flags).reduce((sum, count) => sum + count, 0), flags };
}

test("one annotated event stays discoverable independently of its count proportion", () => {
  const dense = bucket({ safe: 1, suspicious: 0, critical: 2, unflagged: 100_000 });
  const original = structuredClone(dense);
  assert.deepEqual(timelineFlagMarkers(dense), [{ flag: "safe", count: 1, row: 0 }, { flag: "critical", count: 2, row: 2 }]);
  assert.deepEqual(dense, original, "Presence markers must not inflate count bars or assign unflagged events to Safe");
  assert.deepEqual(timelineFlagMarkers(bucket({ safe: 0, suspicious: 0, critical: 0, unflagged: 8 })), []);
  assert.equal(initialTimelineBucket([bucket({ safe: 0, suspicious: 0, critical: 0, unflagged: 1_000_000 }), dense]), 1);
});

test("default inspection has a stable busiest-bucket fallback, including empty input", () => {
  assert.equal(initialTimelineBucket([]), null);
  assert.equal(initialTimelineBucket([bucket({ safe: 0, suspicious: 0, critical: 0, unflagged: 1 }), bucket({ safe: 0, suspicious: 0, critical: 0, unflagged: 4 })]), 1);
});

test("normal drawer widths have an intermediate axis label and exact nanosecond endpoints", () => {
  const range = { min: "2026-11-12T00:00:00.000000001", max: "2026-11-12T00:00:00.000000101" };
  const narrow = timelineAxisTicks(range, 340), wide = timelineAxisTicks(range, 1000);
  assert.equal(narrow.length, 3);
  assert.equal(wide.length, 5);
  assert.deepEqual(narrow.map((tick) => tick.position), [0, .5, 1]);
  assert.equal(parseTimelineNanos(narrow[1].value)! - parseTimelineNanos(range.min)!, 50n);
  assert.equal(narrow[0].secondary, ".000000001");
  assert.equal(narrow[2].secondary, ".000000101");
  assert.equal(parseTimelineNanos(wide.at(-1)!.value), parseTimelineNanos(range.max));
});

test("axis dates distinguish midnight crossings and offset values normalize only for display", () => {
  const ticks = timelineAxisTicks({ min: "2026-11-12T23:59:59.000000001Z", max: "2026-11-13T09:00:01.000000001+09:00" }, 500);
  assert.equal(ticks[0].secondary, "2026-11-12");
  assert.equal(ticks.at(-1)!.secondary, "2026-11-13");
  assert.equal(ticks.at(-1)!.primary, "00:00:01");
  const single = timelineAxisTicks({ min: "1969-12-31T23:59:59.999999999Z", max: "1969-12-31T23:59:59.999999999Z" }, 400);
  assert.equal(single.length, 1);
  assert.equal(single[0].position, .5);
  assert.equal(single[0].secondary, ".999999999");
  assert.deepEqual(timelineAxisTicks({ min: "bad", max: "2026-11-12" }, 400), []);
});


test("a one-nanosecond window never produces duplicate keyed tick values", () => {
  const ticks = timelineAxisTicks({ min: "2026-11-12T00:00:00.000000001", max: "2026-11-12T00:00:00.000000002" }, 1000);
  assert.equal(ticks.length, 2);
  assert.equal(new Set(ticks.map((tick) => tick.value)).size, ticks.length);
});


test("drag readout sums exact existing bucket counts and preserves endpoint inclusion", () => {
  const a = bucket({ safe: 1, suspicious: 0, critical: 0, unflagged: 100_000 });
  const b = { ...bucket({ safe: 0, suspicious: 2, critical: 1, unflagged: 4 }), end: "2026-11-12T00:00:01.000000003", end_inclusive: true };
  const selected = timelineBucketSelection([a, b], 0, 1)!;
  assert.equal(selected.start, a.start);
  assert.equal(selected.end, b.end);
  assert.equal(selected.end_inclusive, true);
  assert.equal(selected.count, a.count + b.count);
  assert.deepEqual(selected.flags, { safe: 1, suspicious: 2, critical: 1, unflagged: 100_004 });
  assert.equal(timelineBucketSelection([a], -1, 0), null);
  assert.equal(timelineBucketSelection([a], 1, 0), null);
});
