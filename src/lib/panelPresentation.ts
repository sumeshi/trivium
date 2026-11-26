import { shortTimelineLabel } from "./timelineRange.ts";
import type { TimeSpan } from "./types";

/** Display-only shortening of source labels; tooltips retain original timestamps. */
export const compactTimestamp = shortTimelineLabel;

/** Count-based ranking also ranks gaps, whose count is elapsed whole seconds. */
export function rankedSpans<T extends { count: number; duration_nanos: string }>(
  spans: readonly T[], mode: "count" | "duration" | "time" = "count"
): T[] {
  if (mode === "time") return [...spans];
  if (mode === "count") return [...spans].sort((left, right) => right.count - left.count);
  return spans.map(span => ({ span, duration: durationNanos(span) }))
    .sort((left, right) => left.duration === right.duration ? 0 : left.duration > right.duration ? -1 : 1)
    .map(item => item.span);
}

/** Consume the engine's exact duration without interpreting source timestamps. */
export function durationNanos(span: Pick<TimeSpan, "duration_nanos">): bigint {
  return BigInt(span.duration_nanos);
}

export function durationLabel(nanos: bigint): string {
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
