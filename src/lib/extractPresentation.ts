/** Short display text never changes the raw value used by copy/filter/export. */
export function extractValuePreview(value: string, limit = 240): string {
  const text = value.replace(/\r\n|\r|\n/g, ' ↵ ');
  if (text.length <= limit) return text;
  const end = /[\uD800-\uDBFF]/.test(text.charAt(limit - 1)) ? limit - 1 : limit;
  return `${text.slice(0, end)}…`;
}

export function extractFileName(source: string, format: string): string {
  const name = source.split(/[/\\]/).at(-1) ?? source;
  const stem = name.replace(/\.[^.]+$/, '').replace(/[<>:"/\\|?*\x00-\x1f]/g, '_') || 'data';
  const safeFormat = format.replace(/[^a-z0-9_-]+/gi, '-').replace(/^-+|-+$/g, '') || 'values';
  return `trivium-extract-${safeFormat}-${stem}.csv`;
}
