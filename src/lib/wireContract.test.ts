import assert from "node:assert/strict";
import test from "node:test";
import type * as Wire from "./generated/engine";
import type { AnnotationPatch, QueryProjectRowsArgs } from "./backend";
import type { QuerySpec } from "./types";

// Compile-time checks ensure UI normalization still fits actual Rust inputs.
const query: QuerySpec = { search: "x", searchColumns: [], condition: null,
  flagFilter: "safe|none", bookmarkedOnly: false, sortKey: null, sortDirection: "asc" };
const wireQuery: Wire.QuerySpec = query;
const uiRequest: QueryProjectRowsArgs = { ...query, projectId: "case", requestId: "owner" };
const wireRequest: Wire.QueryRowsPayload = { ...uiRequest, offset: null, limit: null };
const patch: AnnotationPatch = { memo: null, tags: [], bookmarked: false };
const wirePatch: Partial<Wire.AnnotationPatch> = patch;

test("UI query and patch retain wire field names and explicit clearing", () => {
  assert.equal(wireQuery.flagFilter, "safe|none");
  assert.equal(wireRequest.projectId, "case");
  assert.deepEqual(JSON.parse(JSON.stringify(wirePatch)), { memo: null, tags: [], bookmarked: false });
});
