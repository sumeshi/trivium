import assert from "node:assert/strict";
import { test } from "node:test";
import { applyAnnotationEntry, normalizeRow } from "./components/project_view/state.ts";
import type { FlagEntry, ProjectRow } from "./types.ts";

const notes = [
  "Sender: <alice@example.org>",
  "<alice@example.org>",
  '<CommandLine>whoami & "hostname"</CommandLine>',
  '<img src=x onerror="window.__memoUnsafe=true"><script>window.__memoUnsafe=true</script>',
  "  first line\r\n\tsecond line, \"quoted\"\n \t",
  "\u0000\u0008\u000b\u001f",
  " \t\n ",
];

const source: ProjectRow = { color: null,
  row_index: 9, cells: { c1: { truncated: false, length: Array.from("source is unchanged").length,  kind: "string", text: "source is unchanged" } },
  flag: "suspicious", memo: null, tags: ["triage"], bookmarked: true,
};

test("loaded notes retain XML, addresses, markup, controls and whitespace as text", () => {
  for (const memo of notes) {
    const loaded = normalizeRow(JSON.parse(JSON.stringify({ ...source, memo })));
    assert.equal(loaded.memo, memo);
    assert.equal(loaded.displayCache.c1, "source is unchanged");
  }
  assert.equal(normalizeRow(source).memo, "");
});

test("annotation responses update notes losslessly without changing source cells", () => {
  const row = normalizeRow(source);
  for (const memo of notes) {
    const entry: FlagEntry = { flag: "critical", memo, tags: [], bookmarked: false };
    const updated = applyAnnotationEntry(row, JSON.parse(JSON.stringify(entry)));
    assert.equal(updated.memo, memo);
    assert.equal(updated.displayCache, row.displayCache);
    assert.equal(updated.cells, row.cells);
    assert.equal(updated.previewCache, row.previewCache);
    assert.equal(applyAnnotationEntry(updated, entry).memo, memo, "read/edit responses must not strip again");
  }
  const cleared = applyAnnotationEntry(row, null);
  assert.equal(cleared.memo, "");
});

test("notes never replace a source column whose id is memo", () => {
  const row = normalizeRow({ ...source, cells: { memo: { truncated: false, length: Array.from("source field").length,  kind: "string", text: "source field" } } });
  const updated = applyAnnotationEntry(row, { flag: "safe", memo: "investigation note", tags: [], bookmarked: false });
  assert.equal(updated.memo, "investigation note");
  assert.equal(updated.displayCache.memo, "source field");
  assert.equal(updated.previewCache.memo, "source field");
  assert.equal(updated.displayCache, row.displayCache);
});
