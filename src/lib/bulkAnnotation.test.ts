import assert from "node:assert/strict";
import { test } from "node:test";

import {
  buildBulkAnnotateArgs,
  buildFlagMemoPatch,
} from "./bulkAnnotation.ts";

const filters = {
  search: "alpha",
  flag: "priority" as const,
  bookmarkedOnly: true,
  searchColumns: ["c000002"],
  condition: null,
};

test("an empty memo leaves the existing memo untouched", () => {
  const patch = buildFlagMemoPatch({
    flag: "critical",
    memo: "",
    clearMemo: false,
  });
  assert.equal(patch.flag, "critical");
  assert.equal("memo" in patch, false);
  assert.equal(patch.memo, undefined);
});

test("whitespace is still an omitted memo, not a delete", () => {
  const patch = buildFlagMemoPatch({
    flag: "safe",
    memo: "   ",
    clearMemo: false,
  });
  assert.equal("memo" in patch, false);
});

test("a non-empty memo is sent as text and clear is explicit", () => {
  const text = buildFlagMemoPatch({
    flag: "suspicious",
    memo: "reviewed",
    clearMemo: false,
  });
  assert.equal(text.memo, "reviewed");

  const cleared = buildFlagMemoPatch({
    flag: "suspicious",
    memo: "ignored",
    clearMemo: true,
  });
  assert.equal("memo" in cleared, true);
  assert.equal(cleared.memo, null);
});

test("the bulk payload omits memo when the UI field is empty", () => {
  const args = buildBulkAnnotateArgs({
    projectId: "p1",
    target: "filtered",
    selectedRowIds: [],
    filters,
    sortKey: "c000001",
    sortDirection: "desc",
    previewToken: "token",
    patch: buildFlagMemoPatch({
      flag: "safe",
      memo: "",
      clearMemo: false,
    }),
  });
  assert.equal("memo" in args, false);
  assert.equal(args.memo, undefined);
  assert.equal(args.flag, "safe");
  assert.deepEqual(args.searchColumns, ["c000002"]);
  assert.equal(args.bookmarkedOnly, true);
});

test("selected bulk args include only the frozen row ids", () => {
  const args = buildBulkAnnotateArgs({
    projectId: "p1",
    target: "selection",
    selectedRowIds: [4, 2],
    filters,
    sortKey: null,
    sortDirection: "asc",
    previewToken: null,
    patch: { flag: "critical" },
  });
  assert.deepEqual(args.rowIndices, [4, 2]);
  assert.equal("previewToken" in args, false);
});
