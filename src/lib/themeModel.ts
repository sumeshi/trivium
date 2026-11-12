export const DEFAULT_THEME = "light" as const;
export const THEME_PREFERENCE_VERSION = 2 as const;

export const THEME_OPTIONS = [
  { value: "trivium", label: "Trivium" },
  { value: "light", label: "Light" },
  { value: "dark", label: "Dark" },
  { value: "toki", label: "Toki" },
  { value: "sakura", label: "Sakura" },
  { value: "kohaku", label: "Kohaku" },
  { value: "hisui", label: "Hisui" },
  { value: "nagi", label: "Nagi" },
  { value: "sora", label: "Sora" },
  { value: "yukari", label: "Yukari" },
  { value: "sumire", label: "Sumire" },
  { value: "yozora", label: "Yozora" },
  { value: "shion", label: "Shion" },
  { value: "kasumi", label: "Kasumi" },
] as const;

export type Theme = (typeof THEME_OPTIONS)[number]["value"];
const PASTEL_THEME_IDS: ReadonlySet<Theme> = new Set([
  "yukari", "sakura", "sora", "kohaku", "hisui", "toki", "nagi",
]);
export const PASTEL_THEME_OPTIONS = THEME_OPTIONS.filter(({ value }) => PASTEL_THEME_IDS.has(value));

export interface ThemePreference {
  version: typeof THEME_PREFERENCE_VERSION;
  theme: Theme;
}

/** Only names currently offered by the application are accepted. */
export function parseTheme(value: unknown): Theme | null {
  return THEME_OPTIONS.some((option) => option.value === value) ? value as Theme : null;
}

/** Persisted settings use the current versioned record in both storage paths. */
export function parseSavedTheme(value: unknown): Theme | null {
  let record = value;
  if (typeof record === "string") {
    try { record = JSON.parse(record); } catch { return null; }
  }
  if (!record || typeof record !== "object" || Array.isArray(record)) return null;
  const saved = record as { version?: unknown; theme?: unknown };
  return saved.version === THEME_PREFERENCE_VERSION ? parseTheme(saved.theme) : null;
}

export function createThemePreference(value: Theme): ThemePreference {
  return { version: THEME_PREFERENCE_VERSION, theme: value };
}

export function isLightTheme(value: Theme): boolean {
  return value === "light" || value === "sumire" ||
    PASTEL_THEME_IDS.has(value);
}
