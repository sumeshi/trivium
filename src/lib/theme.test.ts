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
import { DEFAULT_THEME, THEME_OPTIONS } from "./themeModel.ts";

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
function colour(tokens: Map<string, string>, name: string): string {
  let value = tokens.get(name) ?? "";
  const visited = new Set<string>();
  for (;;) {
    const reference = /^var\(--([\w-]+)\)$/.exec(value);
    if (!reference) return value;
    assert.ok(!visited.has(reference[1]), `CSS token cycle: ${name}`);
    visited.add(reference[1]);
    value = tokens.get(reference[1]) ?? "";
  }
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

test("first-run preference is the site-inspired Trivium theme and saved choices remain valid", () => {
  assert.equal(DEFAULT_THEME, "trivium");
  for (const { value } of THEME_OPTIONS) assert.equal(parseTheme(value), value);
  for (const saved of ["light", "dark", "paper", "midnight"]) {
    assert.equal(parseTheme(saved) ?? DEFAULT_THEME, saved);
  }
  assert.equal(parseTheme(null) ?? DEFAULT_THEME, "trivium");
  assert.equal(parseTheme("removed-theme") ?? DEFAULT_THEME, "trivium");
  assert.equal(isLightTheme("mauve"), false);
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
    assert.ok(!tokens.get("accent-soft")?.includes("transparent"));
  }
});

test("flag meanings retain consistent colours and readable selected symbols in every theme", () => {
  const semantic = ["positive", "warning", "danger"];
  const dark = cssTokens("trivium"), light = cssTokens("light");
  const backgrounds = ["surface", "surface-weak", "surface-soft", "surface-strong",
    "rule-red", "rule-orange", "rule-amber", "rule-green", "rule-teal", "rule-blue", "rule-purple", "rule-pink"];
  for (const { value } of THEME_OPTIONS) {
    const tokens = cssTokens(value), family = isLightTheme(value) ? light : dark;
    for (const flag of semantic) {
      assert.equal(colour(tokens, flag), colour(family, flag), `${value} must preserve the ${flag} meaning`);
      readable(tokens, "flag-selected-text", flag, `${value} selected flag`);
      for (const background of backgrounds) readable(tokens, flag, background, `${value} flag on row`);
    }
  }
});
