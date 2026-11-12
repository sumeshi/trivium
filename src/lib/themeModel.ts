export const DEFAULT_THEME = "light" as const;
export const THEME_PREFERENCE_VERSION = 2 as const;

export const PASTEL_THEME_OPTIONS = [
  { value: "yukari", label: "Yukari" },
  { value: "sakura", label: "Sakura" },
  { value: "sora", label: "Sora" },
  { value: "kohaku", label: "Kohaku" },
  { value: "hisui", label: "Hisui" },
  { value: "fuji", label: "Fuji" },
  { value: "wakaba", label: "Wakaba" },
  { value: "momiji", label: "Momiji" },
  { value: "toki", label: "Toki" },
  { value: "nagi", label: "Nagi" },
] as const;

export const THEME_OPTIONS = [
  { value: "light", label: "Light" },
  { value: "dark", label: "Dark" },
  ...PASTEL_THEME_OPTIONS,
  { value: "paper", label: "Paper" },
  { value: "midnight", label: "Midnight" },
  { value: "mauve", label: "Mauve" },
  { value: "trivium", label: "Trivium" },
] as const;

export type Theme = (typeof THEME_OPTIONS)[number]["value"];
export interface ThemePreference {
  version: typeof THEME_PREFERENCE_VERSION;
  theme: Theme;
}

/** Parse a current UI choice. Saved preferences use their versioned parser. */
export function parseTheme(value: unknown): Theme | null {
  return THEME_OPTIONS.some((option) => option.value === value) ? value as Theme : null;
}

function legacyTheme(value: unknown): Theme | null {
  const parsed = parseTheme(value);
  // The previous Light was the purple pastel now named Yukari.
  return parsed === "light" ? "yukari" : parsed;
}

/** Accept the old plain localStorage name and {theme} file without losing its palette. */
export function parseSavedTheme(value: unknown): Theme | null {
  let record = value;
  if (typeof record === "string") {
    const name = legacyTheme(record);
    if (name) return name;
    try { record = JSON.parse(record); } catch { return null; }
  }
  if (typeof record === "string") return legacyTheme(record);
  if (!record || typeof record !== "object" || Array.isArray(record)) return null;
  const saved = record as { version?: unknown; theme?: unknown };
  if (saved.version === undefined || saved.version === 1) return legacyTheme(saved.theme);
  return saved.version === THEME_PREFERENCE_VERSION ? parseTheme(saved.theme) : null;
}

export function createThemePreference(value: Theme): ThemePreference {
  return { version: THEME_PREFERENCE_VERSION, theme: value };
}

export function isLightTheme(value: Theme): boolean {
  return value === "light" || value === "paper" ||
    PASTEL_THEME_OPTIONS.some((option) => option.value === value);
}
