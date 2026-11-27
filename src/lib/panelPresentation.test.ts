import assert from "node:assert/strict";
import { test } from "node:test";
import { durationLabel, durationNanos, rankedSpans } from "./panelPresentation.ts";
import { shortTimelineLabel } from "./timelineRange.ts";
import type { TimeSpan } from "./types";
import { runSvelteTest } from "../../tests/ui/headlessSvelte.ts";

const shortSpan: TimeSpan = {
  start: "2026/11/14 00:00:00.000000001 UTC",
  end: "2026/11/14 00:01:00.000000002 UTC",
  start_utc: "2026-11-14T00:00:00.000000001Z",
  end_utc: "2026-11-14T00:01:00.000000002Z",
  duration_nanos: "60000000001",
  count: 60,
};
const longSpan: TimeSpan = {
  start: "2026/11/14 00:01:00.000000002 UTC",
  end: "14 Nov 2026 09:11:00.000000003 +0900",
  start_utc: "2026-11-14T00:01:00.000000002Z",
  end_utc: "2026-11-14T00:11:00.000000003Z",
  duration_nanos: "600000000001",
  count: 600,
};

test("duration display consumes exact engine nanoseconds", () => {
  assert.equal(durationLabel(durationNanos({ duration_nanos: "1" })), "1ns");
  assert.equal(durationLabel(durationNanos({ duration_nanos: "1000000001" })), "1.000000001s");
  assert.equal(durationLabel(durationNanos({ duration_nanos: "86400000000000" })), "1d");
  assert.equal(durationNanos({ duration_nanos: "18446744073709551615" }), 18_446_744_073_709_551_615n);
  assert.equal(durationLabel(0n), "0s");
});

test("span ranking is stable and never rewrites the source analysis", () => {
  const spans = [
    { ...shortSpan, count: 2, id: "a" },
    { ...shortSpan, count: 8, id: "b" },
    { ...shortSpan, count: 2, id: "c" },
  ];
  assert.deepEqual(rankedSpans(spans).map(span => span.id), ["b", "a", "c"]);
  assert.deepEqual(spans.map(span => span.id), ["a", "b", "c"]);
});

test("longest-span sorting retains exact nanosecond differences and original endpoints", () => {
  const spans = [
    {
      start: "2026.11.14t3:4:5.123456788utc", end: "2026/11/14 12:04:05,123456789 +0900",
      start_utc: "2026-11-14T03:04:05.123456788Z", end_utc: "2026-11-14T03:04:05.123456789Z",
      duration_nanos: "1", count: 1,
    },
    {
      start: "2026.11.14t3:4:5.123456788utc", end: "14 NOV 2026 03:04:05.123456790 UTC",
      start_utc: "2026-11-14T03:04:05.123456788Z", end_utc: "2026-11-14T03:04:05.123456790Z",
      duration_nanos: "2", count: 1,
    },
  ] satisfies TimeSpan[];
  const original = structuredClone(spans);
  const ranked = rankedSpans(spans, "duration");
  assert.equal(ranked[0], spans[1]);
  assert.equal(ranked[1], spans[0]);
  assert.deepEqual(spans, original);
  assert.deepEqual(rankedSpans(spans, "time"), spans);
});

test("flexible and long padded timestamp labels do not affect engine duration ranking", () => {
  const padding = "\t \u0085\u2002".repeat(50);
  const spans = [
    { ...shortSpan, start: `${padding}${shortSpan.start}${padding}`, end: `${padding}${shortSpan.end}${padding}` },
    longSpan,
  ];
  const original = structuredClone(spans);
  assert.equal(durationNanos(spans[0]), 60_000_000_001n);
  assert.equal(rankedSpans(spans, "duration")[0], spans[1]);
  assert.deepEqual(spans, original);
  assert.equal(shortTimelineLabel(spans[0].start), spans[0].start);
});

test("mounted time spans filter by engine UTC bounds while retaining raw labels", () => runSvelteTest(String.raw`
const rawStart='2026/11/14 12:04:05,123456788 +0900';
const rawEnd='14 NOV 2026 03:04:05.123456789 UTC';
const span={start:rawStart,end:rawEnd,
 start_utc:'2026-11-14T03:04:05.123456788Z',end_utc:'2026-11-14T03:04:05.123456789Z',
 duration_nanos:'1',count:2};
const calls=[];
globalThis.__workspace={filterByRange:(...args)=>calls.push(args)};
const List=await compileHeadless('TimeSpanList');
const list=await mountHeadless(List,{column:'Timestamp',spans:[span],kind:'session'});
try{
 await tick();
 const state=list.$capture_state();
 assert.equal(state.sorted[0].start,rawStart);
 assert.equal(state.sorted[0].end,rawEnd);
 assert.equal(state.shortTimelineLabel(state.sorted[0].start),rawStart);
 assert.equal(state.shortTimelineLabel(state.sorted[0].end),rawEnd);
 assert.equal(state.durationLabel(state.durationNanos(state.sorted[0])),'1ns');
 state.selectSpan(state.sorted[0]);
 assert.deepEqual(calls,[['Timestamp',span.start_utc,span.end_utc,'time-analysis']]);
 assert.equal(span.start,rawStart);assert.equal(span.end,rawEnd);
}finally{list.$destroy();}
`));
