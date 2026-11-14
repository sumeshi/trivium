export const SEARCH_EXAMPLES = [
  ['error timeout', 'Both terms (AND)'],
  ['error | warning', 'Either term (OR)'],
  ['-noise', 'Exclude rows containing noise'],
  ['"failed login"', 'Keep the phrase together'],
  ['column:value', 'Search one named column'],
  ['column:"failed login"', 'A phrase in one column'],
  ['@tag:review', 'Rows with the tag review'],
  ['@tag:*', 'Rows with any tag'],
] as const;

export const SEARCH_SYNTAX_HELP = [
  'Case-insensitive text search.',
  ...SEARCH_EXAMPLES.map(([query, meaning]) => `${query} — ${meaning}`),
  'Quote literal colons: "https://example.test".',
  'Use Filter → Rules for grouped AND/OR, comparisons, dates or regular expressions.',
].join('\n');
