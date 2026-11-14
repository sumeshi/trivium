/** A literal annotation tag; the original search grammar has no backslash escapes. */
export function tagSearch(tag: string): string {
  if (/["\\%\x00-\x1f\x7f]/.test(tag) || tag.trim() !== tag) {
    return `@tag:${encodeURIComponent(tag)}`;
  }
  return `@tag:"${tag}"`;
}
