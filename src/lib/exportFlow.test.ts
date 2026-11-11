import assert from "node:assert/strict";
import { test } from "node:test";

import { buildExportArgs, exportFileName } from "./exportFlow.ts";
import type { ExportSnapshot } from "./components/project_view/state.ts";

const snapshot: ExportSnapshot = {
  search: "alpha",
  searchColumns: ["c000002"],
  condition: null,
  flagFilter: "priority",
  bookmarkedOnly: true,
  sortKey: "c000001",
  sortDirection: "desc",
  rowIndices: [4, 2],
  totalRows: 10,
  totalFilteredRows: 3,
  selectedCount: 2,
  visibleColumns: ["c000001", "c000002"],
};

test("filtered export sends the query scope but no selection ids", () => {
  const args = buildExportArgs(
    "p1",
    snapshot,
    { scope: "filtered", useVisibleColumns: false, includeAnnotations: true },
    "/tmp/out.csv"
  );
  assert.equal(args.scope, "filtered");
  assert.deepEqual(args.searchColumns, ["c000002"]);
  assert.equal(args.flagFilter, "priority");
  assert.equal(args.bookmarkedOnly, true);
  assert.equal(args.rowIndices, undefined);
  assert.equal(args.columns, undefined);
});

test("selected export sends the frozen row ids and optional output columns", () => {
  const args = buildExportArgs(
    "p1",
    snapshot,
    { scope: "selected", useVisibleColumns: true, includeAnnotations: false },
    "/tmp/out.csv"
  );
  assert.deepEqual(args.rowIndices, [4, 2]);
  assert.deepEqual(args.columns, ["c000001", "c000002"]);
  assert.equal(args.includeAnnotations, false);
});

test("the default export name is deterministic and project-based", () => {
  assert.equal(
    exportFileName("logs.jsonl", new Date(2026, 10, 11, 9, 5, 7)),
    "20261111-090507_trivium_logs.csv"
  );
});
