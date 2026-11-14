import assert from "node:assert/strict";
import { test } from "node:test";
import { presetFileName } from "./presetFileName.ts";

test("preset filenames identify Trivium and the source rather than a generic preset", () => {
  assert.equal(presetFileName("result.csv"), "trivium-preset-result.json");
  assert.equal(presetFileName("event.logs.jsonl"), "trivium-preset-event.logs.json");
  assert.equal(presetFileName("C:\\evidence\\調査結果.json"), "trivium-preset-調査結果.json");
  assert.equal(presetFileName("/evidence/events.ndjson"), "trivium-preset-events.json");
});

test("preset filenames are usable with Windows restrictions and empty names", () => {
  assert.equal(presetFileName('invalid:<name>?.csv'), "trivium-preset-invalid__name__.json");
  assert.equal(presetFileName("..csv"), "trivium-preset-investigation.json");
  assert.equal(presetFileName(""), "trivium-preset-investigation.json");
});
