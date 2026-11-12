import assert from "node:assert/strict";
import { test } from "node:test";
import { isLightTheme, parseTheme } from "./themeModel.ts";
import { displayRowColor, resolveConditionColor } from "./skin.ts";

test("existing and new stored theme names are validated without coercion", () => {
  for (const saved of ["light", "dark", "yukari", "yozora"]) {
    assert.equal(parseTheme(saved), saved);
  }
  for (const saved of [null, undefined, "", "auto", "unknown", {}, 1]) {
    assert.equal(parseTheme(saved), null);
  }
});

test("extra themes use the matching light/dark condition palette", () => {
  assert.ok(isLightTheme("yukari"));
  assert.ok(!isLightTheme("yozora"));
  for (const name of ["red", "orange", "amber", "green", "teal", "blue", "purple", "pink"]) {
    assert.equal(resolveConditionColor(name, "yukari"), resolveConditionColor(name, "light"));
    assert.equal(resolveConditionColor(name, "yozora"), resolveConditionColor(name, "dark"));
  }
  assert.equal(resolveConditionColor("unknown", "yukari"), null);
});

test("existing saved row colours follow the selected theme without rewriting storage", () => {
  for (const current of ["sakura", "yozora"] as const) {
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

test("neutral Light and the retained palettes keep their current names and distinct accents", () => {
  assert.equal(DEFAULT_THEME, "light");
  assert.deepEqual(PASTEL_THEME_OPTIONS.map(({ value }) => value), ["yukari", "sakura", "sora", "kohaku", "hisui", "toki", "nagi"]);
  for (const { value } of THEME_OPTIONS) assert.equal(parseTheme(value), value);
  assert.equal(parseTheme(null) ?? DEFAULT_THEME, "light");
  assert.equal(parseTheme("removed-theme") ?? DEFAULT_THEME, "light");
  for (const { value } of PASTEL_THEME_OPTIONS) assert.equal(isLightTheme(value), true);
  assert.equal(isLightTheme("tsukimi"), false);
  const palettes = new Set(PASTEL_THEME_OPTIONS.map(({ value }) => {
    const tokens = cssTokens(value);
    return colour(tokens, "bg") + colour(tokens, "accent");
  }));
  assert.equal(palettes.size, 7);
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
  for (const id of ["dark", "yozora", "kasumi", "tsukimi", "trivium"] as const) {
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


test("retired choices map consistently without turning Paper into Yukari", () => {
  const aliases = { fuji: "yukari", wakaba: "hisui", momiji: "kohaku", paper: "light",
    sumi: "dark", midnight: "yozora", mauve: "tsukimi", tasogare: "kasumi" } as const;
  for (const [retired, replacement] of Object.entries(aliases)) {
    assert.equal(THEME_OPTIONS.some(({ value }) => value === retired), false);
    assert.equal(parseTheme(retired), replacement);
    for (const saved of [retired, { theme: retired }, { version: 1, theme: retired }, { version: 2, theme: retired }]) {
      assert.equal(parseSavedTheme(saved), replacement);
      assert.equal(parseSavedTheme(JSON.stringify(saved)), replacement);
    }
  }
  assert.equal(parseSavedTheme("light"), "yukari");
  assert.equal(parseSavedTheme({ version: 2, theme: "light" }), "light");
  assert.equal(parseTheme("__proto__"), null);
});

test("colour stays on pastel headers and accents while main backgrounds remain white", () => {
  for (const name of ["sakura", "sora", "kohaku", "hisui", "toki", "nagi"]) {
    const tokens = cssTokens(name);
    assert.equal(colour(tokens, "bg"), "#ffffff", name);
    assert.equal(colour(tokens, "surface"), "#ffffff", name);
    assert.notEqual(colour(tokens, "surface-weak"), "#ffffff", `${name} header needs its pastel colour`);
    assert.notEqual(colour(tokens, "surface-strong"), "#ffffff", `${name} strong surface needs its pastel colour`);
  }
  const yellow = colour(cssTokens("kohaku"), "surface-strong");
  assert.equal(parseInt(yellow.slice(1, 3), 16), 255);
  const green = parseInt(yellow.slice(3, 5), 16), blue = parseInt(yellow.slice(5, 7), 16);
  assert.ok(green >= 210 && green < 240 && blue >= 140 && blue < 195, "Kohaku uses warm amber rather than lemon yellow");
  assert.ok(luminance(yellow) > .7, "Warm amber still stays bright on the white base");
});

test("new night palettes are dark and bookmarks remain a subtle surface tint", () => {
  for (const name of ["yozora", "kasumi", "tsukimi"] as const) {
    assert.equal(parseTheme(name), name);
    assert.equal(isLightTheme(name), false);
    assert.ok(luminance(colour(cssTokens(name), "bg")) < .03);
  }
  for (const { value } of THEME_OPTIONS) {
    const tokens = cssTokens(value);
    const tinted = colour(tokens, "bookmark-soft");
    const base = colour(tokens, "surface");
    const stronger = value === "light" || value === "dark";
    assert.equal(tokens.get("bookmark-row-strength"), stronger ? "10%" : "5%");
    assert.ok(contrast(tinted, base) < (stronger ? 1.4 : 1.1), `${value} bookmark tint must remain moderate`);
  }
});


test("Light and Dark selections stand out while the approved pastel palettes stay unchanged", () => {
  for (const name of ["light", "dark"] as const) {
    const tokens = cssTokens(name);
    for (const flag of ["safe", "suspicious", "critical"]) {
      const fill = colour(tokens, `flag-${flag}-selected-bg`);
      assert.ok(contrast(fill, colour(tokens, "surface")) > 1.25, `${name} ${flag} selection must be visible`);
      assert.match(colour(tokens, `flag-${flag}-selected-border`), /^#[0-9a-f]{6}$/i);
    }
  }
  const approved = { yukari: "#5948a5", sakura: "#923c6a", sora: "#346792", hisui: "#2a694d", toki: "#96424d", nagi: "#2e696d" };
  for (const [name, accent] of Object.entries(approved)) {
    const tokens = cssTokens(name);
    assert.equal(colour(tokens, "accent"), accent);
    assert.equal(tokens.get("flag-safe-selected-bg"), "var(--positive-soft)");
    assert.equal(tokens.get("bookmark-row-strength"), "5%");
  }
});

test("Trivium follows the near-black blue reference and Tsukimi inherits the purple palette", () => {
  const trivium = cssTokens("trivium"), tsukimi = cssTokens("tsukimi");
  assert.equal(colour(trivium, "bg"), "#050716");
  assert.equal(colour(trivium, "surface"), "#11111b");
  assert.equal(colour(trivium, "accent"), "#879de9");
  assert.equal(colour(tsukimi, "accent"), "#cbb8ee");
  assert.equal(parseSavedTheme("mauve"), "tsukimi");
  assert.equal(parseSavedTheme("tsukimi"), "tsukimi");
  assert.equal(parseSavedTheme({ version: 2, theme: "tsukimi" }), "tsukimi");
});
