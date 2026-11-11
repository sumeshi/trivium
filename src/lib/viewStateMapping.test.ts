import assert from "node:assert/strict";
import { test } from "node:test";

import {
  savedViewFromUnknown,
  savedViewToViewState,
  viewStateToSavedView,
  viewStateWarnings,
} from "./viewStateMapping.ts";
import type { EditorGroup } from "./stores/condition.ts";
import type { SavedView } from "./viewState.ts";

const condition: EditorGroup = {
  type: "group",
  id: "root",
  kind: "and",
  collapsed: false,
  children: [
    {
      type: "condition",
      id: "leaf-1",
      column: "c000001",
      op: "eq",
      value: "WS01",
      negated: false,
    },
  ],
};

const columns = ["c000001", "c000002", "c000003"];

function saved(overrides: Partial<SavedView> = {}): SavedView {
  return {
    condition,
    search: "powershell",
    searchColumns: ["c000002"],
    flagFilter: "suspicious",
    bookmarkedOnly: true,
    hiddenColumns: ["c000003"],
    sortKey: "c000001",
    sortDirection: "desc",
    columnWidths: { c000001: 176.5, c000002: 320 },
    offset: 12,
    logicalRow: 12,
    ...overrides,
  };
}

test("save and restore keeps search, search columns, flag filter, widths and row", () => {
  const state = savedViewToViewState(saved());
  const restored = viewStateToSavedView(state, columns);

  assert.deepEqual(restored, saved());
});

test("unknown saved fields are reported instead of silently reset", () => {
  const state = savedViewToViewState(
    saved({
      searchColumns: ["c999999"],
      flagFilter: "future" as never,
      sortKey: "c999999",
      hiddenColumns: ["c999999"],
    })
  );
  const warnings = viewStateWarnings(state, columns);
  assert.deepEqual(warnings, [
    "search columns c999999",
    "flag filter",
    "sort column c999999",
    "hidden columns c999999",
  ]);
});

test("a legacy save without the new fields restores safe defaults", () => {
  const parsed = savedViewFromUnknown(
    {
      condition,
      hiddenColumns: ["c000002"],
      sortKey: "c000001",
      sortDirection: "desc",
      offset: 21,
    },
    columns
  );

  assert.ok(parsed);
  assert.equal(parsed.search, "");
  assert.deepEqual(parsed.searchColumns, []);
  assert.equal(parsed.flagFilter, "all");
  assert.equal(parsed.bookmarkedOnly, false);
  assert.deepEqual(parsed.hiddenColumns, ["c000002"]);
  assert.deepEqual(parsed.columnWidths, {});
  // `offset` used to mean the result-relative row at the top of the viewport.
  assert.equal(parsed.logicalRow, 21);
});

test("unknown columns and invalid widths are dropped instead of used", () => {
  const restored = viewStateToSavedView(
    {
      ...savedViewToViewState(saved()),
      searchColumns: ["c000002", "removed-column"],
      hiddenColumns: ["c000003", "removed-column"],
      sortKey: "removed-column",
      columnWidths: {
        c000001: 200,
        c000002: Number.NaN,
        c000003: -10,
        "removed-column": 300,
      },
    },
    columns
  );

  assert.ok(restored);
  assert.deepEqual(restored.searchColumns, ["c000002"]);
  assert.deepEqual(restored.hiddenColumns, ["c000003"]);
  assert.equal(restored.sortKey, null);
  assert.deepEqual(restored.columnWidths, { c000001: 200 });
});

test("an unreadable condition tree is reported instead of silently emptied", () => {
  assert.equal(
    savedViewFromUnknown({ condition: { type: "group" }, search: "x" }, columns),
    null
  );
});

test("flag filter falls back to all when the stored value is unknown", () => {
  const restored = viewStateToSavedView(
    {
      ...savedViewToViewState(saved()),
      flagFilter: "removed-value" as never,
    },
    columns
  );
  assert.ok(restored);
  assert.equal(restored.flagFilter, "all");
});
