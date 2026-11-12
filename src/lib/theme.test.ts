import assert from "node:assert/strict";
import { test } from "node:test";
import { isLightTheme, parseTheme } from "./themeModel.ts";
import { displayRowColor, resolveConditionColor } from "./skin.ts";

test("existing and new stored theme names are validated without coercion", () => {
  for (const saved of ["light", "dark", "paper", "midnight"]) {
    assert.equal(parseTheme(saved), saved);
  }
  for (const saved of [null, undefined, "", "auto", "unknown", {}, 1]) {
    assert.equal(parseTheme(saved), null);
  }
});

test("extra themes use the matching light/dark condition palette", () => {
  assert.ok(isLightTheme("paper"));
  assert.ok(!isLightTheme("midnight"));
  for (const name of ["red", "orange", "amber", "green", "teal", "blue", "purple", "pink"]) {
    assert.equal(resolveConditionColor(name, "paper"), resolveConditionColor(name, "light"));
    assert.equal(resolveConditionColor(name, "midnight"), resolveConditionColor(name, "dark"));
  }
  assert.equal(resolveConditionColor("unknown", "paper"), null);
});

test("existing saved row colours follow the selected theme without rewriting storage", () => {
  for (const current of ["paper", "midnight"] as const) {
    assert.equal(displayRowColor("#fbe4e4", current), "var(--rule-red)");
    assert.equal(displayRowColor("#4a2b2f", current), "var(--rule-red)");
    assert.equal(displayRowColor("#123456", current), "#123456");
  }
});

// Measure the actual CSS tokens instead of a second palette kept only in tests.
// Opaque row surfaces also prevent scrolling content from bleeding through a
// pinned cell when the table uses these theme backgrounds.
import { readFileSync } from "node:fs";
import { createThemePreference, DEFAULT_THEME, PASTEL_THEME_OPTIONS, parseSavedTheme, THEME_OPTIONS } from "./themeModel.ts";

const stylesheet = readFileSync(new URL("../app.css", import.meta.url), "utf8");
function cssTokens(themeName: string): Map<string, string> {
  const values = new Map<string, string>();
  for (const block of stylesheet.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (!block[1].includes(":root") && !block[1].includes(`[data-theme="${themeName}"]`)) continue;
    for (const field of block[2].matchAll(/--([\w-]+)\s*:\s*([^;]+);/g)) {
      values.set(field[1], field[2].trim());
    }
  }
  return values;
}
function colour(tokens: Map<string, string>, name: string, seen = new Set<string>()): string {
  assert.ok(!seen.has(name), `CSS token cycle: ${name}`);
  const visited = new Set([...seen, name]);
  const value = tokens.get(name) ?? "";
  const reference = /^var\(--([\w-]+)\)$/.exec(value);
  if (reference) return colour(tokens, reference[1], visited);
  const mix = /^color-mix\(in srgb, var\(--([\w-]+)\) ([\d.]+)%, var\(--([\w-]+)\)\)$/.exec(value);
  if (!mix) return value;
  const first = colour(tokens, mix[1], visited), second = colour(tokens, mix[3], visited);
  const weight = Number(mix[2]) / 100;
  return "#" + [1, 3, 5].map((offset) => Math.round(
    parseInt(first.slice(offset, offset + 2), 16) * weight +
    parseInt(second.slice(offset, offset + 2), 16) * (1 - weight),
  ).toString(16).padStart(2, "0")).join("");
}

function luminance(hex: string): number {
  assert.match(hex, /^#[\da-f]{6}$/i, `Expected an opaque colour: ${hex}`);
  const channel = (offset: number) => {
    const n = parseInt(hex.slice(offset, offset + 2), 16) / 255;
    return n <= .04045 ? n / 12.92 : ((n + .055) / 1.055) ** 2.4;
  };
  return .2126 * channel(1) + .7152 * channel(3) + .0722 * channel(5);
}
function contrast(a: string, b: string): number {
  const values = [luminance(a), luminance(b)].sort((x, y) => x - y);
  return (values[1] + .05) / (values[0] + .05);
}
function readable(tokens: Map<string, string>, foreground: string, background: string, context: string, minimum = 4.5) {
  const ratio = contrast(colour(tokens, foreground), colour(tokens, background));
  assert.ok(ratio >= minimum, `${context}: ${foreground}/${background} contrast ${ratio.toFixed(2)} < ${minimum}`);
}

test("neutral Light is the default and ten distinct pastel choices include the renamed Yukari", () => {
  assert.equal(DEFAULT_THEME, "light");
  assert.equal(PASTEL_THEME_OPTIONS.length, 10);
  for (const { value } of THEME_OPTIONS) assert.equal(parseTheme(value), value);
  assert.equal(parseTheme(null) ?? DEFAULT_THEME, "light");
  assert.equal(parseTheme("removed-theme") ?? DEFAULT_THEME, "light");
  for (const { value } of PASTEL_THEME_OPTIONS) assert.equal(isLightTheme(value), true);
  assert.equal(isLightTheme("mauve"), false);
  const palettes = new Set(PASTEL_THEME_OPTIONS.map(({ value }) => {
    const tokens = cssTokens(value);
    return colour(tokens, "bg") + colour(tokens, "accent");
  }));
  assert.equal(palettes.size, 10);
  const yukari = cssTokens("yukari");
  assert.equal(colour(yukari, "bg"), "#f2f5fa");
  assert.equal(colour(yukari, "accent"), "#5948a5");
  assert.notEqual(colour(cssTokens("light"), "bg"), colour(yukari, "bg"));
  assert.notEqual(colour(cssTokens("light"), "accent"), colour(yukari, "accent"));
  assert.equal(colour(cssTokens("dark"), "surface"), "#171c24");
});

test("old Light preferences migrate to Yukari while newly saved Light remains neutral", () => {
  for (const old of ["light", '{"theme":"light"}', { theme: "light" }, { version: 1, theme: "light" }]) {
    assert.equal(parseSavedTheme(old), "yukari");
  }
  for (const id of ["dark", "paper", "midnight", "mauve", "trivium"] as const) {
    assert.equal(parseSavedTheme(id), id);
    assert.equal(parseSavedTheme({ theme: id }), id);
  }
  for (const { value } of THEME_OPTIONS) {
    const saved = createThemePreference(value);
    assert.equal(saved.version, 2);
    assert.equal(parseSavedTheme(saved), value);
    assert.equal(parseSavedTheme(JSON.stringify(saved)), value);
  }
  for (const invalid of [null, [], {}, "bad json", { version: 3, theme: "light" },
    { version: "2", theme: "light" }, { version: 2, theme: "unknown" }]) {
    assert.equal(parseSavedTheme(invalid), null);
  }
});

test("all themes keep ordinary, muted and input text readable on every opaque surface", () => {
  for (const { value } of THEME_OPTIONS) {
    const tokens = cssTokens(value);
    for (const surface of ["bg", "surface", "surface-weak", "surface-soft", "surface-strong"]) {
      readable(tokens, "fg", surface, value);
      readable(tokens, "muted", surface, value);
      readable(tokens, "accent", surface, value);
    }
    readable(tokens, "accent-contrast", "accent", `${value} primary control`);
    readable(tokens, "accent-border", "surface", `${value} focus indicator`, 3);
    readable(tokens, "border-strong", "surface", `${value} checkbox boundary`, 3);
    readable(tokens, "bookmark", "bookmark-soft", `${value} bookmark`);
    readable(tokens, "fg", "bookmark-soft", `${value} bookmarked row`);
    readable(tokens, "muted", "bookmark-soft", `${value} bookmarked row hint`);
    assert.ok(!tokens.get("accent-soft")?.includes("transparent"));
  }
});

test("flag meanings retain consistent colours and readable selected symbols in every theme", () => {
  const semantic = ["positive", "warning", "danger"];
  const dark = cssTokens("trivium"), light = cssTokens("light");
  const backgrounds = ["surface", "surface-weak", "surface-soft", "surface-strong",
    "rule-red", "rule-orange", "rule-amber", "rule-green", "rule-teal", "rule-blue", "rule-purple", "rule-pink", "bookmark-soft"];
  for (const { value } of THEME_OPTIONS) {
    const tokens = cssTokens(value), family = isLightTheme(value) ? light : dark;
    for (const flag of semantic) {
      assert.equal(colour(tokens, flag), colour(family, flag), `${value} must preserve the ${flag} meaning`);
      const key = flag === "positive" ? "safe" : flag === "warning" ? "suspicious" : "critical";
      readable(tokens, `flag-${key}-selected-fg`, `flag-${key}-selected-bg`, `${value} soft selected flag`);
      for (const background of backgrounds) readable(tokens, flag, background, `${value} flag on row`);
    }
  }
});
