import { BUILTIN_THEMES } from "./themeCatalog.generated.ts";

export const DEFAULT_THEME = "light" as const;
export const THEME_PREFERENCE_VERSION = 2 as const;
export type BuiltinTheme = (typeof BUILTIN_THEMES)[number]["id"];
export type Theme = BuiltinTheme | `user:${string}`;
export const THEME_OPTIONS = BUILTIN_THEMES.map(({ id, name }) => ({ value: id, label: name }));
export const PASTEL_THEME_OPTIONS = BUILTIN_THEMES.filter(definition => definition.pastel)
  .map(({ id, name }) => ({ value: id, label: name }));

// Opaque palette values keep fixed cells opaque. Geometry, fonts, semantic flag
// colours, URLs and CSS expressions are deliberately outside a user theme.
export const EDITABLE_THEME_COLORS = ["bg", "fg", "muted", "surface", "surface-weak", "surface-soft",
  "surface-strong", "border", "border-strong", "accent", "accent-contrast", "accent-border"] as const;
export type ThemeColor = (typeof EDITABLE_THEME_COLORS)[number];
export interface UserTheme {
  type: "trivium-theme";
  version: 1;
  id: string;
  name: string;
  base: BuiltinTheme;
  colors: Partial<Record<ThemeColor, string>>;
}
export function normalizeThemeHex(value: string): string | null {
  if (!/^#(?:[\da-f]{3}|[\da-f]{6})$/i.test(value)) return null;
  const hex = value.slice(1).toLowerCase();
  return "#" + (hex.length === 3 ? [...hex].map(character => character + character).join("") : hex);
}
const userThemes = new Map<string, UserTheme>();
const own = (value: object, key: string) => Object.prototype.hasOwnProperty.call(value, key);

export function parseBuiltinTheme(value: unknown): BuiltinTheme | null {
  return BUILTIN_THEMES.some(definition => definition.id === value) ? value as BuiltinTheme : null;
}
export function parseTheme(value: unknown): Theme | null {
  if (parseBuiltinTheme(value)) return value as BuiltinTheme;
  return typeof value === "string" && value.startsWith("user:") && userThemes.has(value.slice(5)) ? value as Theme : null;
}
export function parseUserTheme(value: unknown): UserTheme {
  if (typeof value === "string") {
    if (value.length > 65_536) throw new Error("Theme JSON must be 64 KiB or smaller.");
    try { value = JSON.parse(value); } catch { throw new Error("Invalid theme JSON."); }
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Expected a Trivium theme.");
  const record = value as Record<string, unknown>;
  if (Object.keys(record).some(key => !["type", "version", "id", "name", "base", "colors"].includes(key))
    || record.type !== "trivium-theme" || record.version !== 1) throw new Error("Expected a version 1 Trivium theme.");
  if (typeof record.id !== "string" || !/^[a-z0-9][a-z0-9_-]{0,63}$/.test(record.id)
    || /^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])$/.test(record.id)
    || ["__proto__", "constructor", "prototype"].includes(record.id)) throw new Error("Theme ID must use lowercase letters, digits, dashes or underscores and a regular filename.");
  if (typeof record.name !== "string" || !record.name.trim() || record.name.length > 64) throw new Error("Theme name must contain 1–64 characters.");
  const base = parseBuiltinTheme(record.base);
  if (!base) throw new Error("Choose an existing built-in theme as the base.");
  if (!record.colors || typeof record.colors !== "object" || Array.isArray(record.colors)) throw new Error("Theme colors must be an object.");
  const colors: UserTheme["colors"] = {};
  for (const [key, color] of Object.entries(record.colors)) {
    if (!EDITABLE_THEME_COLORS.includes(key as ThemeColor)) throw new Error(`Unsupported theme color: ${key}`);
    if (typeof color !== "string" || !/^#(?:[\da-f]{3}|[\da-f]{6})$/i.test(color)) throw new Error(`Use an opaque hex color for ${key}.`);
    colors[key as ThemeColor] = color.toLowerCase();
  }
  return { type: "trivium-theme", version: 1, id: record.id, name: record.name.trim(), base, colors };
}
export function registerUserTheme(definition: UserTheme): Theme {
  const validated = parseUserTheme(definition);
  userThemes.set(validated.id, validated);
  return `user:${validated.id}`;
}
export function getUserTheme(value: Theme): UserTheme | null {
  return value.startsWith("user:") ? userThemes.get(value.slice(5)) ?? null : null;
}
export function themeBase(value: Theme): BuiltinTheme {
  return getUserTheme(value)?.base ?? parseBuiltinTheme(value) ?? DEFAULT_THEME;
}
export function themeTokens(value: Theme): Record<string, string> {
  const definition = BUILTIN_THEMES.find(definition => definition.id === themeBase(value))!;
  return { ...definition.tokens, ...getUserTheme(value)?.colors };
}
export function exportTheme(value: Theme, id: string): UserTheme {
  const existing = getUserTheme(value);
  if (existing) return parseUserTheme(existing);
  const definition = BUILTIN_THEMES.find(definition => definition.id === value)!;
  const tokens = themeTokens(value), colors: UserTheme["colors"] = {};
  for (const key of EDITABLE_THEME_COLORS) {
    let color = tokens[key];
    const visited = new Set<string>();
    while (/^var\(--([\w-]+)\)$/.test(color) && !visited.has(color)) {
      visited.add(color); color = tokens[/^var\(--([\w-]+)\)$/.exec(color)![1]];
    }
    if (/^#[\da-f]{3}(?:[\da-f]{3})?$/i.test(color)) colors[key] = color;
  }
  return parseUserTheme({ type: "trivium-theme", version: 1, id, name: `${definition.name} copy`, base: definition.id, colors });
}

export interface ThemePreference { version: typeof THEME_PREFERENCE_VERSION; theme: Theme; }
export function parseSavedTheme(value: unknown): Theme | null {
  let record = value;
  if (typeof record === "string") { try { record = JSON.parse(record); } catch { return null; } }
  if (!record || typeof record !== "object" || Array.isArray(record)) return null;
  return own(record, "version") && (record as ThemePreference).version === THEME_PREFERENCE_VERSION
    ? parseTheme((record as ThemePreference).theme) : null;
}
export function createThemePreference(value: Theme): ThemePreference { return { version: THEME_PREFERENCE_VERSION, theme: value }; }
export function isLightTheme(value: Theme): boolean {
  return BUILTIN_THEMES.find(definition => definition.id === themeBase(value))?.mode === "light";
}
