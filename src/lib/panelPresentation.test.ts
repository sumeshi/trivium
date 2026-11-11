import assert from "node:assert/strict";
import { test } from "node:test";
import { durationLabel, durationNanos, rankedSpans } from "./panelPresentation.ts";

test("duration display distinguishes nanoseconds and honors timezone offsets", () => {
  assert.equal(durationLabel(durationNanos("2026-11-11T12:00:00.123456788", "2026-11-11T12:00:00.123456789")), "1ns");
  assert.equal(durationLabel(durationNanos("2026-11-11T12:00:00+09:00", "2026-11-11T03:00:01Z")), "1s");
  assert.equal(durationLabel(durationNanos("2026-11-11", "2026-11-12")), "1d");
  assert.equal(durationLabel(durationNanos("unknown", "2026-11-12")), "—");
  assert.equal(durationNanos("2026-11-12", "2026-11-11"), null);
});

test("span ranking is stable and never rewrites the source analysis", () => {
  const spans = [{ count: 2, id: "a" }, { count: 8, id: "b" }, { count: 2, id: "c" }];
  assert.deepEqual(rankedSpans(spans).map(span => span.id), ["b", "a", "c"]);
  assert.deepEqual(spans.map(span => span.id), ["a", "b", "c"]);
});

test("longest-span sorting retains nanosecond differences and original endpoints", () => {
  const spans = [
    { start: "2026-11-11T00:00:00.000000001", end: "2026-11-11T00:00:00.000000002", count: 1 },
    { start: "2026-11-11T00:00:00.000000001", end: "2026-11-11T00:00:00.000000003", count: 1 },
  ];
  const ranked = rankedSpans(spans, "duration");
  assert.equal(ranked[0], spans[1]);
  assert.equal(ranked[1], spans[0]);
  assert.equal(ranked[0].end, "2026-11-11T00:00:00.000000003");
  assert.deepEqual(rankedSpans(spans, "time"), spans);
});
