import { parseTimelineNanos, shortTimelineLabel } from "./timelineRange.ts";

/** Display-only shortening: callers retain the original timestamp for filters. */
export const compactTimestamp = shortTimelineLabel;

/** Count-based ranking also ranks gaps, whose count is elapsed whole seconds. */
export function rankedSpans<T extends { count: number; start?: string; end?: string }>(
  spans: readonly T[], mode: "count" | "duration" | "time" = "count"
): T[] {
  if (mode === "time") return [...spans];
  if (mode === "count") return [...spans].sort((left, right) => right.count - left.count);
  return spans.map(span => ({ span, duration: durationNanos(span.start ?? "", span.end ?? "") ?? -1n }))
    .sort((left, right) => left.duration === right.duration ? 0 : left.duration > right.duration ? -1 : 1)
    .map(item => item.span);
}

/** Derive a duration for display without rounding original nanosecond text. */
export function durationNanos(start: string, end: string): bigint | null {
  const first = parseTimelineNanos(start);
  const last = parseTimelineNanos(end);
  return first === null || last === null || last < first ? null : last - first;
}

export function durationLabel(nanos: bigint | null): string {
  if (nanos === null) return "—";
  if (nanos === 0n) return "0s";
  if (nanos < 1_000n) return `${nanos}ns`;
  if (nanos < 1_000_000n) return `${Number(nanos) / 1_000}µs`;
  if (nanos < 1_000_000_000n) return `${Number(nanos) / 1_000_000}ms`;
  const seconds = nanos / 1_000_000_000n;
  if (seconds < 60n) {
    const fraction = (nanos % 1_000_000_000n).toString().padStart(9, "0").replace(/0+$/, "");
    return `${seconds}${fraction ? `.${fraction}` : ""}s`;
  }
  const days = seconds / 86_400n;
  const hours = seconds % 86_400n / 3_600n;
  const minutes = seconds % 3_600n / 60n;
  const remaining = seconds % 60n;
  return [days ? `${days}d` : "", hours ? `${hours}h` : "", minutes ? `${minutes}m` : "", remaining ? `${remaining}s` : ""].filter(Boolean).join(" ");
}
