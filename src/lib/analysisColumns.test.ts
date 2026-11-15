import assert from "node:assert/strict";
import { test } from "node:test";
import { datetimeAnalysisColumns, initialDatetimeColumn } from "./analysisColumns.ts";

test("mixed date columns remain available for explicit datetime analysis", () => {
  const columns = ["Host", "MixedTime", "KnownTime"];
  const kinds = { Host: "string", MixedTime: "string", KnownTime: "datetime" };
  assert.deepEqual(datetimeAnalysisColumns(columns, kinds), {
    inferred: ["KnownTime"], other: ["Host", "MixedTime"],
  });
  assert.equal(initialDatetimeColumn(columns, kinds), "KnownTime");
  assert.deepEqual(columns, ["Host", "MixedTime", "KnownTime"]);
});

test("absence of inferred dates requires an explicit choice instead of analyzing arbitrary text", () => {
  assert.equal(initialDatetimeColumn(["Host", "MixedTime"], { MixedTime: "string" }), "");
  assert.deepEqual(datetimeAnalysisColumns(["MixedTime"]), { inferred: [], other: ["MixedTime"] });
  assert.deepEqual(datetimeAnalysisColumns([]), { inferred: [], other: [] });
});
