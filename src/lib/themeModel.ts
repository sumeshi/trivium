export const DEFAULT_THEME = "trivium" as const;

export const THEME_OPTIONS = [
  { value: "trivium", label: "Trivium" },
  { value: "light", label: "Light" },
  { value: "dark", label: "Dark" },
  { value: "paper", label: "Paper" },
  { value: "midnight", label: "Midnight" },
  { value: "mauve", label: "Mauve" },
] as const;

export type Theme = (typeof THEME_OPTIONS)[number]["value"];

/** Accept saved theme names without discarding existing Light/Dark settings. */
export function parseTheme(value: unknown): Theme | null {
  return THEME_OPTIONS.some((option) => option.value === value) ? value as Theme : null;
}

export function isLightTheme(value: Theme): boolean {
  return value === "light" || value === "paper";
}
