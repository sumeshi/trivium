/** Match engine Unicode whitespace trimming after the UI's existing trim. */
export function normalizeSearchText(search: string): string {
  return search.trim().replace(/^\p{White_Space}+|\p{White_Space}+$/gu, "");
}

/** An empty text query does not inspect columns; retain the picker state elsewhere. */
export function effectiveSearchColumns(search: string, columns: readonly string[]): string[] {
  return normalizeSearchText(search).length === 0 ? [] : [...columns];
}

export function effectiveSearchScopeKey(search: string, columns: readonly string[]): string {
  return JSON.stringify(effectiveSearchColumns(search, columns));
}
