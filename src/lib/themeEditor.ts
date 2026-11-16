import { exportTheme, normalizeThemeHex, parseUserTheme } from "./themeModel.ts";
import type { Theme, ThemeColor, UserTheme } from "./themeModel";

export const BASIC_THEME_FIELDS: ReadonlyArray<{ key: ThemeColor; label: string }> = [
  { key: "bg", label: "App background" }, { key: "surface", label: "Table background" },
  { key: "fg", label: "Text" }, { key: "muted", label: "Muted text" },
  { key: "accent", label: "Accent" }, { key: "border", label: "Border" },
];
export const ADVANCED_THEME_FIELDS: ReadonlyArray<{ key: ThemeColor; label: string }> = [
  { key: "surface-weak", label: "Headers" }, { key: "surface-soft", label: "Alternate rows" },
  { key: "surface-strong", label: "Strong surface" }, { key: "border-strong", label: "Strong border" },
  { key: "accent-contrast", label: "Accent text" }, { key: "accent-border", label: "Focus border" },
];

/** Resolve inherited colors once; the draft never changes its base profile. */
export function createThemeDraft(source: Theme, newId: string): UserTheme {
  const definition = exportTheme(source, newId);
  const inherited = exportTheme(definition.base, newId);
  return { ...definition, colors: { ...inherited.colors, ...definition.colors } };
}

export function validThemeDraft(draft: UserTheme): UserTheme | null {
  try { return parseUserTheme(draft); } catch { return null; }
}

/** Preview uses the last valid value while a hex field is being typed. */
export function withPreviewColor(definition: UserTheme, key: ThemeColor, input: string): UserTheme {
  const color = normalizeThemeHex(input);
  return color ? { ...definition, colors: { ...definition.colors, [key]: color } } : definition;
}

export function themeContrastWarning(definition: UserTheme): string {
  const luminance = (value: string) => {
    const color = normalizeThemeHex(value);
    if (!color) return null;
    const channel = (at: number) => {
      const n = parseInt(color.slice(at, at + 2), 16) / 255;
      return n <= .04045 ? n / 12.92 : ((n + .055) / 1.055) ** 2.4;
    };
    return .2126 * channel(1) + .7152 * channel(3) + .0722 * channel(5);
  };
  for (const [key, backgroundKey, label, backgroundLabel] of [
    ["fg", "surface", "Text", "the table background"],
    ["muted", "surface", "Muted text", "the table background"],
    ["accent-contrast", "accent", "Accent text", "its background"],
  ] as const) {
    const foreground = luminance(definition.colors[key] ?? ""), background = luminance(definition.colors[backgroundKey] ?? "");
    if (foreground !== null && background !== null
      && (Math.max(background, foreground) + .05) / (Math.min(background, foreground) + .05) < 4.5) {
      return `${label} has low contrast against ${backgroundLabel}.`;
    }
  }
  return "";
}
