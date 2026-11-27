import assert from "node:assert/strict";
import { test } from "node:test";
import { get } from "svelte/store";

import {
  savedViewFromUnknown,
  viewStateToSavedView,
  viewStateWarnings,
} from "./viewStateMapping.ts";
import type { EditorGroup } from "./stores/condition.ts";
import { createConditionState, presetCondition } from "./stores/condition.ts";
import type { ViewState } from "./types.ts";

const condition = {
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
} satisfies EditorGroup;

const columns = ["c000001", "c000002", "c000003"];

function stored(overrides: Partial<ViewState> = {}): ViewState {
  return {
    version: 1,
    condition,
    search: "powershell",
    searchColumns: ["c000002"],
    flagFilter: "suspicious",
    bookmarkedOnly: true,
    hiddenColumns: ["c000003"],
    sortKey: "c000001",
    sortDirection: "desc",
    columnWidths: { c000001: 176.5, c000002: 320 },
    logicalRow: 12,
    ...overrides,
  };
}

test("current view DTO restores search, search columns, flag filter, widths and row", () => {
  const state = stored();
  const restored = viewStateToSavedView(state, columns);

  assert.deepEqual({ version: 1, ...restored }, state);
});

test('note width restores separately from source column widths', () => {
  const wire = stored({ noteColumnWidth: 154 });
  assert.deepEqual({ version: 1, ...viewStateToSavedView(wire, columns) }, wire);
  assert.deepEqual(wire.columnWidths, { c000001: 176.5, c000002: 320 });
  for (const invalid of [null, -10, 0, '154', Infinity]) {
    assert.equal(savedViewFromUnknown({ ...wire, noteColumnWidth: invalid }, columns), null);
  }
  assert.equal(viewStateToSavedView(stored(), columns)?.noteColumnWidth, undefined);
});

test("preset literal arrays survive engine view JSON and saved-view restoration", () => {
  for (const values of [[], [""], ["alice,bob", " carol ", ""]]) {
    const literal = { column: columns[0], op: "in" as const, values, negated: true };
    const tree = presetCondition(literal)!;
    const wire = JSON.parse(JSON.stringify(stored({ condition: tree as unknown as Record<string, unknown> })));
    const restored = viewStateToSavedView(wire, columns);
    assert.ok(restored);
    const state = createConditionState();
    state.conditionTree.set(restored.condition);
    assert.deepEqual(get(state.conditionPayload), literal);
  }
});

test("malformed saved literal arrays are rejected without silently changing the rule", () => {
  for (const sourceValues of [null, "a,b", ["a", null], [1], {}]) {
    const malformed = { ...condition, children: [{ ...condition.children[0], sourceValues }] };
    assert.equal(savedViewFromUnknown(stored({ condition: malformed }), columns), null);
  }
});

test('display order and ISO settings restore without changing query scope', () => {
  const state = stored({ columnOrder: ['c000003', 'c000001', 'c000002'], columnFormats: { c000002: 'iso' } });
  assert.deepEqual({ version: 1, ...viewStateToSavedView(state, columns) }, state);
  assert.deepEqual(state.searchColumns, ['c000002']);
  assert.equal(state.sortKey, 'c000001');
  const stale = { ...state, columnOrder: ['gone', 'c000002', 'c000002'], columnFormats: { gone: 'iso', c000002: 'iso' } };
  assert.deepEqual(viewStateToSavedView(stale, columns)?.columnOrder, ['c000002']);
  assert.deepEqual(viewStateToSavedView(stale, columns)?.columnFormats, { c000002: 'iso' });
  assert.deepEqual(viewStateWarnings(stale, columns), ['column order', 'column formats']);
  assert.equal(viewStateToSavedView({ ...state, columnFormats: { c000001: 'future' } }, columns), null);
});

test("unknown saved fields are reported instead of silently reset", () => {
  const state = stored({
    searchColumns: ["c999999"],
    flagFilter: "future" as never,
    sortKey: "c999999",
    hiddenColumns: ["c999999"],
  });
  const warnings = viewStateWarnings(state, columns);
  assert.deepEqual(warnings, [
    "search columns c999999",
    "flag filter",
    "sort column c999999",
    "hidden columns c999999",
  ]);
});

test("unsupported sorting and widths are reported before their defaults can replace saved settings", () => {
  const state = { ...stored(), sortDirection: "natural" as never,
    columnWidths: { c000001: -10, removed: 200 }, noteColumnWidth: -10 };
  assert.deepEqual(viewStateWarnings(state, columns), ['sort direction', 'column widths', 'note column width']);
  assert.deepEqual(viewStateWarnings(stored(), columns), []);
});

test("only the current complete persisted schema is accepted", () => {
  const state = stored();
  for (const version of [undefined, null, -1, 0, 2, '1']) {
    assert.equal(savedViewFromUnknown({ ...state, version }, columns), null);
  }
  for (const patch of [{ offset: 21 }, { logicalRow: undefined }, { logicalRow: -1 },
    { search: 1 }, { searchColumns: 'c000001' }, { bookmarkedOnly: 'true' }]) {
    assert.equal(savedViewFromUnknown({ ...state, ...patch }, columns), null);
  }
});

test("unknown column references are excluded and reported without changing valid current fields", () => {
  const restored = viewStateToSavedView(
    {
      ...stored(),
      searchColumns: ["c000002", "removed-column"],
      hiddenColumns: ["c000003", "removed-column"],
      sortKey: "removed-column",
      columnWidths: {
        c000001: 200,
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

test("an unsupported flag filter is rejected without widening the saved result", () => {
  const restored = viewStateToSavedView(
    {
      ...stored(),
      flagFilter: "removed-value" as never,
    },
    columns
  );
  assert.equal(restored, null);
});

test("canonical composite flags restore from the current DTO", () => {
  for (const flagFilter of ["safe|critical|none", "safe|suspicious|critical|none", "none", "all"] as const) {
    const state = stored({ flagFilter });
    assert.equal(viewStateToSavedView(state, columns)?.flagFilter, flagFilter);
    assert.deepEqual(viewStateWarnings(state, columns), []);
  }
  const malformed = { ...stored(), flagFilter: "safe|future" as const };
  assert.equal(viewStateToSavedView(malformed, columns), null);
  assert.deepEqual(viewStateWarnings(malformed, columns), ["flag filter"]);
});
