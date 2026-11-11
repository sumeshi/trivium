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
