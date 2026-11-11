import assert from "node:assert/strict";
import { test } from "node:test";
import { formatTimelineNanos, panTimelineRange, parseTimelineNanos, shortTimelineLabel, zoomTimelineRange } from "./timelineRange.ts";

test("timestamp fractions preserve nanoseconds, offset equivalence and dates before the epoch", () => {
  const a = parseTimelineNanos("2024-01-01T00:00:00.123456789")!;
  const b = parseTimelineNanos("2024-01-01T00:00:00.123456790Z")!;
  assert.equal(b - a, 1n);
  assert.equal(parseTimelineNanos("2024-01-01T09:00:00.123456789+09:00"), a);
  assert.equal(formatTimelineNanos(a), "2024-01-01T00:00:00.123456789");
  assert.equal(formatTimelineNanos(-1n), "1969-12-31T23:59:59.999999999");
  assert.equal(parseTimelineNanos("2024-02-29"), parseTimelineNanos("2024-02-29T00:00:00"));
  assert.equal(parseTimelineNanos("2023-02-29"), null);
  assert.equal(parseTimelineNanos("2024-01-01T24:00:00"), null);
  assert.equal(parseTimelineNanos("not a timestamp"), null);
});

test("zoom and pan use exact nanosecond windows without a coarse overview grid", () => {
  const full = { min: "2024-01-01T00:00:00.000000001", max: "2024-01-01T00:00:00.000000101" };
  const zoom = zoomTimelineRange(full, full, .5)!;
  assert.equal(parseTimelineNanos(zoom.max)! - parseTimelineNanos(zoom.min)!, 50n);
  const moved = panTimelineRange(zoom, full, 1)!;
  assert.equal(moved.max, full.max);
  const expanded = zoomTimelineRange(moved, full, 10)!;
  assert.deepEqual(expanded, full);
  const single = { min: full.min, max: "2024-01-01T00:00:00.000000002" };
  assert.equal(parseTimelineNanos(zoomTimelineRange(single, full, .8)!.max)! - parseTimelineNanos(zoomTimelineRange(single, full, .8)!.min)!, 1n);
});

test("display labels signal fraction shortening and retain time zones", () => {
  assert.equal(shortTimelineLabel("2024-01-01T00:00:00.123456789Z"), "2024-01-01 00:00:00… Z");
  assert.equal(shortTimelineLabel("2024-01-01T09:00:00+09:00"), "2024-01-01 09:00:00 +09:00");
  assert.equal(shortTimelineLabel("2024-01-01"), "2024-01-01");
});
