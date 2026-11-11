import assert from "node:assert/strict";
import { test } from "node:test";

import {
  buildCellCaches,
  copyText,
  displayText,
} from "./ipcValues.ts";
import type { Cell, ProjectRow } from "./types";

/** Reads a cell exactly as it arrives from the Tauri IPC JSON. */
function fromIpc(json: string): Cell {
  return JSON.parse(json) as Cell;
}

function rowFromIpc(cells: Record<string, unknown>): ProjectRow {
  return JSON.parse(
    JSON.stringify({
      row_index: 0,

      cells,
      flag: "",
      tags: [],
      bookmarked: false,
    })
  ) as ProjectRow;
}

test("long integers stay exact decimal strings", () => {
  const big = fromIpc('{"text":"9007199254740993","kind":"int"}');
  const u64 = fromIpc('{"text":"18446744073709551615","kind":"int"}');

  assert.equal(displayText(big), "9007199254740993");
  assert.equal(copyText(big), "9007199254740993");
  assert.equal(displayText(u64), "18446744073709551615");
  assert.equal(copyText(u64), "18446744073709551615");

  // This is the value a Number conversion would produce; the cell never goes
  // through that path.
  assert.notEqual(copyText(big), String(Number(big.text)));
  assert.equal(String(Number(big.text)), "9007199254740992");
});

test("null, missing, and empty string are distinct", () => {
  const nullCell = fromIpc('{"text":null,"kind":"null"}');
  const missing = fromIpc('{"text":null,"kind":"missing"}');
  const empty = fromIpc('{"text":"","kind":"string"}');

  assert.equal(displayText(nullCell), "null");
  assert.equal(copyText(nullCell), "null");

  assert.equal(displayText(missing), "—");
  assert.equal(copyText(missing), "");

  assert.equal(displayText(empty), "");
  assert.equal(copyText(empty), "");
});

test("bool cells keep their source spelling", () => {
  const truthy = fromIpc('{"text":"true","kind":"string"}');
  const falsy = fromIpc('{"text":"false","kind":"string"}');

  assert.equal(displayText(truthy), "true");
  assert.equal(copyText(falsy), "false");
});

test("table, detail, copy, and filter share the same exact string", () => {
  const row = rowFromIpc({
    c000001: { text: "9007199254740993", kind: "int" },
    c000002: { text: null, kind: "null" },
    c000003: { text: null, kind: "missing" },
    c000004: { text: "", kind: "string" },
  });
  const { displayCache, previewCache } = buildCellCaches(row.cells);

  // Table and detail both read displayCache.
  assert.equal(displayCache.c000001, "9007199254740993");
  // The preview cache holds the same text for an untruncated cell.
  assert.equal(previewCache.c000001, "9007199254740993");
  assert.equal(previewCache.c000001, displayCache.c000001);

  assert.equal(displayCache.c000002, "null");
  assert.equal(previewCache.c000002, "null");
  assert.equal(displayCache.c000003, "—");
  assert.equal(previewCache.c000003, "");
  assert.equal(displayCache.c000004, "");
  assert.equal(previewCache.c000004, "");
});

test("a truncated cell is marked as a preview, not shown as the full value", () => {
  const preview = "x".repeat(256);
  const row = rowFromIpc({
    c000001: {
      text: preview,
      kind: "string",
      truncated: true,
      length: 300_000,
    },
  });
  const { displayCache, previewCache } = buildCellCaches(row.cells);

  // The table ends the preview with an ellipsis; the raw prefix stays
  // available as the dialog's initial hint.
  assert.equal(displayCache.c000001, `${preview}…`);
  assert.equal(previewCache.c000001, preview);
  assert.notEqual(previewCache.c000001, displayCache.c000001);
});
