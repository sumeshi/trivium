import type { HistogramBucket } from "./types";
import { formatTimelineNanos, parseTimelineNanos } from "./timelineRange.ts";
import type { TimelineRange } from "./timelineRange";

export const TIMELINE_MARKER_FLAGS = ["safe", "suspicious", "critical"] as const;
export type TimelineMarkerFlag = typeof TIMELINE_MARKER_FLAGS[number];

/** Presence indicators are separate from the proportional count bars. */
export function timelineFlagMarkers(bucket: HistogramBucket) {
  return TIMELINE_MARKER_FLAGS.flatMap((flag, row) =>
    bucket.flags[flag] > 0 ? [{ flag, count: bucket.flags[flag], row }] : []);
}

/** Start with an annotated bucket when possible, otherwise the busiest bucket. */
export function initialTimelineBucket(buckets: readonly HistogramBucket[]): number | null {
  const flagged = buckets.findIndex((bucket) => TIMELINE_MARKER_FLAGS.some((flag) => bucket.flags[flag] > 0));
  if (flagged >= 0) return flagged;
  if (buckets.length === 0) return null;
  return buckets.reduce((best, bucket, index) => bucket.count > buckets[best].count ? index : best, 0);
}

/** Aggregate at most the displayed buckets for an exact drag-selection readout. */
export function timelineBucketSelection(buckets: readonly HistogramBucket[], low: number, high: number): HistogramBucket | null {
  if (!Number.isInteger(low) || !Number.isInteger(high) || low < 0 || high < low || high >= buckets.length) return null;
  const selected = buckets.slice(low, high + 1);
  return {
    start: selected[0].start, end: selected.at(-1)!.end, end_inclusive: selected.at(-1)!.end_inclusive,
    count: selected.reduce((sum, bucket) => sum + bucket.count, 0),
    flags: {
      safe: selected.reduce((sum, bucket) => sum + bucket.flags.safe, 0),
      suspicious: selected.reduce((sum, bucket) => sum + bucket.flags.suspicious, 0),
      critical: selected.reduce((sum, bucket) => sum + bucket.flags.critical, 0),
      unflagged: selected.reduce((sum, bucket) => sum + bucket.flags.unflagged, 0),
    },
  };
}

/** Axis positions use layout numbers; all timestamp interpolation stays bigint. */
export function timelineAxisTicks(range: TimelineRange | null, width: number) {
  if (!range) return [];
  const first = parseTimelineNanos(range.min), last = parseTimelineNanos(range.max);
  if (first === null || last === null || last < first) return [];
  const desired = width >= 720 ? 5 : 3;
  // A sub-nanosecond duplicate label is impossible; narrow spans use fewer
  // unique ticks rather than repeating keys or inventing fractional precision.
  const count = last - first < BigInt(desired) ? Number(last - first) + 1 : desired;
  const subsecond = last - first < 1_000_000_000n;
  return Array.from({ length: count }, (_, index) => {
    const position = count === 1 ? .5 : index / (count - 1);
    const value = formatTimelineNanos(first + (count === 1 ? 0n : (last - first) * BigInt(index) / BigInt(count - 1)));
    const fraction = value.split(".")[1];
    return {
      value, position,
      primary: value.slice(11, 19),
      secondary: subsecond ? `.${fraction ?? "0"}` : value.slice(0, 10),
      anchor: index === 0 && count > 1 ? "start" : index === count - 1 && count > 1 ? "end" : "middle",
    };
  });
}
