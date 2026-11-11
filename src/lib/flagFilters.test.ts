import assert from "node:assert/strict";
import { test } from "node:test";
import {
  FLAG_FILTER_VALUES, encodeFlagFilter, isFlagFilterValue, parseFlagFilter, toggleFlagFilter,
} from "./flagFilters.ts";
import { buildBulkAnnotateArgs } from "./bulkAnnotation.ts";
import { buildExportArgs } from "./exportFlow.ts";

test("all sixteen flag selections round-trip without special combination options", () => {
  for (let mask = 0; mask < 16; mask += 1) {
    const selected = FLAG_FILTER_VALUES.filter((_, index) => (mask & (1 << index)) !== 0);
    const encoded = encodeFlagFilter([...selected].reverse());
    assert.deepEqual(parseFlagFilter(encoded), selected);
    assert.ok(isFlagFilterValue(encoded));
    assert.equal(encoded, selected.length ? selected.join("|") : "all");
  }
});

test("checkbox changes allow any combination and clearing the last one restores all", () => {
  let current = toggleFlagFilter("all", "critical");
  current = toggleFlagFilter(current, "safe");
  current = toggleFlagFilter(current, "none");
  assert.equal(current, "safe|critical|none");
  current = toggleFlagFilter(current, "critical");
  current = toggleFlagFilter(current, "none");
  assert.equal(current, "safe");
  assert.equal(toggleFlagFilter(current, "safe"), "all");
});

test("legacy values retain their meaning and malformed saved combinations are rejected", () => {
  assert.deepEqual(parseFlagFilter("priority"), ["suspicious", "critical"]);
  assert.deepEqual(parseFlagFilter("none"), ["none"]);
  assert.deepEqual(parseFlagFilter(""), []);
  assert.deepEqual(parseFlagFilter("none|safe|safe"), ["safe", "none"]);
  assert.deepEqual(parseFlagFilter(" safe | none "), ["safe", "none"]);
  assert.equal(toggleFlagFilter("priority", "safe"), "safe|suspicious|critical");
  for (const unknown of ["future", "safe|future", "safe|", "safe|all", "safe|priority", null, []]) {
    assert.equal(parseFlagFilter(unknown), null);
    assert.equal(isFlagFilterValue(unknown), false);
  }
});

test("export and bulk keep the same composite flag query unchanged", () => {
  const flag = "safe|critical|none" as const;
  const filters = { search: "needle", searchColumns: ["hidden"], flag,
    bookmarkedOnly: true, condition: null };
  const bulk = buildBulkAnnotateArgs({ projectId: "project", target: "filtered", selectedRowIds: [],
    filters, sortKey: "rank", sortDirection: "desc", previewToken: null, patch: { memo: "checked" } });
  const exported = buildExportArgs("project", { search: filters.search,
    searchColumns: filters.searchColumns, flagFilter: flag, bookmarkedOnly: true, condition: null,
    sortKey: "rank", sortDirection: "desc", rowIndices: [], totalRows: 40, totalFilteredRows: 8,
    selectedCount: 0, visibleColumns: ["rank"] },
  { scope: "filtered", useVisibleColumns: true, includeAnnotations: true }, "result.csv");
  assert.equal(bulk.flagFilter, flag);
  assert.equal(exported.flagFilter, flag);
  assert.deepEqual(bulk.searchColumns, exported.searchColumns);
  assert.equal(bulk.search, exported.search);
  assert.equal(bulk.sortKey, exported.sortKey);
  assert.equal(bulk.sortDirection, exported.sortDirection);
});
