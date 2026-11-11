export interface TimelineRange { min: string; max: string }

/** Parse timestamp fractions before Date handles the whole-second calendar part. */
export function parseTimelineNanos(text: string): bigint | null {
  const match = text.trim().match(/^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?)?(Z|[+-]\d{2}:?\d{2})?$/);
  if (!match) return null;
  const [, date, time = "00:00:00", fraction = "", zone = "Z"] = match;
  const [hour, minute, second] = time.split(":").map(Number);
  if (hour > 23 || minute > 59 || second > 59) return null;
  const [year, month, day] = date.split("-").map(Number);
  const calendar = new Date(0);
  calendar.setUTCFullYear(year, month, 0);
  if (month < 1 || month > 12 || day < 1 || day > calendar.getUTCDate()) return null;
  const milliseconds = Date.parse(`${date}T${time}${zone}`);
  if (!Number.isFinite(milliseconds)) return null;
  return BigInt(milliseconds) * 1_000_000n + BigInt(fraction.padEnd(9, "0") || "0");
}

export function formatTimelineNanos(nanos: bigint): string {
  const seconds = nanos >= 0n ? nanos / 1_000_000_000n : (nanos - 999_999_999n) / 1_000_000_000n;
  const fraction = (nanos - seconds * 1_000_000_000n).toString().padStart(9, "0").replace(/0+$/, "");
  const calendar = new Date(Number(seconds * 1_000n));
  const whole = calendar.toISOString().slice(0, 19);
  return fraction ? `${whole}.${fraction}` : whole;
}

/** Display only; original timestamp strings remain the query and tooltip values. */
export function shortTimelineLabel(text: string): string {
  const match = text.match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$/);
  return match ? `${match[1]} ${match[2]}${match[3] ? "…" : ""}${match[4] ? ` ${match[4]}` : ""}` : text;
}

function numericRange(range: TimelineRange): [bigint, bigint] | null {
  const min = parseTimelineNanos(range.min), max = parseTimelineNanos(range.max);
  return min !== null && max !== null && max >= min ? [min, max] : null;
}

function boundedRange(start: bigint, width: bigint, full: [bigint, bigint]): TimelineRange {
  const [min, max] = full;
  width = width > max - min ? max - min : width;
  start = start < min ? min : start;
  start = start + width > max ? max - width : start;
  return { min: formatTimelineNanos(start), max: formatTimelineNanos(start + width) };
}

export function zoomTimelineRange(current: TimelineRange, full: TimelineRange, factor: number): TimelineRange | null {
  const window = numericRange(current), limits = numericRange(full);
  if (!window || !limits || !Number.isFinite(factor) || factor <= 0) return null;
  const span = window[1] - window[0];
  let width = span * BigInt(Math.round(factor * 1_000)) / 1_000n;
  if (width < 1n) width = 1n;
  if (width === span && factor > 1) width += 1n;
  return boundedRange(window[0] + (span - width) / 2n, width, limits);
}

export function panTimelineRange(current: TimelineRange, full: TimelineRange, direction: -1 | 1): TimelineRange | null {
  const window = numericRange(current), limits = numericRange(full);
  if (!window || !limits) return null;
  const width = window[1] - window[0];
  const shift = width / 2n || 1n;
  return boundedRange(window[0] + BigInt(direction) * shift, width, limits);
}
