/** Source timestamp parsing is presentation-only; evidence text is never rewritten. */
const MAX_INPUT_BYTES = 160;
const MIN_NANOS = -(1n << 63n);
const MAX_NANOS = (1n << 63n) - 1n;
const utf8 = new TextEncoder();
const white = String.raw`\p{White_Space}`;
const zone = String.raw`([Zz]|[Uu][Tt][Cc]|[Gg][Mm][Tt]|[+-]\d{2}:?\d{2})`;
const yearFirst = new RegExp(String.raw`^(\d{4})([-/.])(\d{1,2})\2(\d{1,2})(?:(?:[Tt]|${white}+)(\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:[.,](\d{1,9}))?)?(?:${white}*${zone})?)?$`, 'u');
const mailDate = new RegExp(String.raw`^(?:([A-Za-z]{3}),${white}+)?(\d{1,2})${white}+([A-Za-z]{3})${white}+(\d{4})${white}+(\d{1,2}):(\d{1,2}):(\d{1,2})(?:[.,](\d{1,9}))?${white}+${zone}$`, 'u');
const months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
const weekdays = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

interface Parts {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
  fraction: string;
  zone?: string;
  weekday?: string;
}

function offsetSeconds(value: string | undefined): number | null {
  if (!value || value.toUpperCase() === 'Z' || value.toUpperCase() === 'UTC' || value.toUpperCase() === 'GMT') return 0;
  const match = /^([+-])(\d{2}):?(\d{2})$/.exec(value);
  if (!match) return null;
  const hours = Number(match[2]), minutes = Number(match[3]);
  if (hours > 23 || minutes > 59) return null;
  return (match[1] === '-' ? -1 : 1) * (hours * 3_600 + minutes * 60);
}

function instant(parts: Parts): bigint | null {
  const { year, month, day, hour, minute, second, fraction, weekday } = parts;
  if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59) return null;
  const calendar = new Date(0);
  calendar.setUTCFullYear(year, month - 1, day);
  if (calendar.getUTCFullYear() !== year || calendar.getUTCMonth() !== month - 1 || calendar.getUTCDate() !== day) return null;
  if (weekday && weekdays[calendar.getUTCDay()] !== weekday.toLowerCase()) return null;
  calendar.setUTCHours(hour, minute, second, 0);
  const offset = offsetSeconds(parts.zone);
  if (offset === null) return null;
  const nanos = BigInt(calendar.getTime()) * 1_000_000n
    + BigInt(fraction.padEnd(9, '0') || '0') - BigInt(offset) * 1_000_000_000n;
  return nanos >= MIN_NANOS && nanos <= MAX_NANOS ? nanos : null;
}

/** Match the engine's unambiguous formats without locale or epoch guessing. */
export function parseSourceTimestampNanos(input: string): bigint | null {
  if (input.length > MAX_INPUT_BYTES || (/[^\x00-\x7f]/.test(input) && utf8.encode(input).length > MAX_INPUT_BYTES)) return null;
  // Rust's White_Space trim does not remove a literal U+FEFF.
  const text = input.replace(/^\p{White_Space}+|\p{White_Space}+$/gu, '');
  const iso = yearFirst.exec(text);
  if (iso) return instant({
    year: Number(iso[1]), month: Number(iso[3]), day: Number(iso[4]),
    hour: Number(iso[5] ?? 0), minute: Number(iso[6] ?? 0), second: Number(iso[7] ?? 0),
    fraction: iso[8] ?? '', zone: iso[9],
  });
  const mail = mailDate.exec(text);
  if (!mail) return null;
  return instant({
    weekday: mail[1], day: Number(mail[2]), month: months.indexOf(mail[3].toLowerCase()) + 1,
    year: Number(mail[4]), hour: Number(mail[5]), minute: Number(mail[6]), second: Number(mail[7]),
    fraction: mail[8] ?? '', zone: mail[9],
  });
}
