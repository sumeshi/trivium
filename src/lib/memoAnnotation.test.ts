import assert from "node:assert/strict";
import { test } from "node:test";
import { clearMocks, mockIPC } from "@tauri-apps/api/mocks";
import { createBackend } from "./backend.ts";
import { buildMemoPatch } from "./memoAnnotation.ts";
import type { FlagEntry } from "./types.ts";

const baseline: FlagEntry = { flag: "safe", memo: "original", tags: ["manual"], bookmarked: true };

test("an unchanged note editor creates no write, including a missing note", () => {
  assert.deepEqual(buildMemoPatch(baseline, { memo: "original", tags: ["manual"], bookmarked: true }), {});
  assert.deepEqual(buildMemoPatch({ memo: null, tags: [], bookmarked: false }, { memo: "", tags: [], bookmarked: false }), {});
});

test("editing the note omits untouched tags and bookmark and keeps raw text exact", () => {
  const memo = " \n<CommandLine>whoami</CommandLine> <alice@example.org>\r\n ";
  assert.deepEqual(buildMemoPatch(baseline, { memo, tags: ["manual"], bookmarked: true }), { memo });
  assert.deepEqual(buildMemoPatch(baseline, { memo: "", tags: ["manual"], bookmarked: true }), { memo: null });
  assert.deepEqual(buildMemoPatch(baseline, { memo: " ", tags: ["manual"], bookmarked: true }), { memo: " " });
});

test("explicit tag and bookmark edits remain replacements, including clearing either", () => {
  const tags = ["new"];
  const patch = buildMemoPatch(baseline, { memo: "original", tags, bookmarked: false });
  tags.push("later edit");
  assert.deepEqual(patch, { tags: ["new"], bookmarked: false });
  assert.deepEqual(buildMemoPatch(baseline, { memo: "original", tags: [], bookmarked: true }), { tags: [] });
});

test("a note-only save from a stale open editor preserves earlier queued tags and bookmark changes", async () => {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { value: {}, configurable: true, writable: true });
  let release!: () => void;
  const held = new Promise<void>(done => { release = done; });
  const opening: FlagEntry = { flag: "safe", memo: "original", tags: [], bookmarked: false };
  let stored: FlagEntry = structuredClone(opening);
  let revision = 0;
  const order: string[] = [];
  mockIPC(async (command, args) => {
    const p = (args as { payload: Record<string, unknown> }).payload;
    if (command === "bulk_preview") { await held; return { count: 1, previewToken: "checked" }; }
    if (command === "bulk_update") {
      order.push("bulk"); stored = { ...stored, flag: "critical", tags: ["review"] }; return { updated: 1 };
    }
    assert.equal(command, "update_flag");
    if ("memo" in p) {
      order.push("note");
      assert.equal(Object.hasOwn(p, "tags"), false);
      assert.equal(Object.hasOwn(p, "bookmarked"), false);
      stored = { ...stored, memo: String(p.memo) };
    } else {
      order.push("bookmark"); stored = { ...stored, bookmarked: true };
    }
    return { entry: structuredClone(stored), revision: ++revision, flaggedRecords: 1 };
  });
  const api = createBackend();
  const apply = api.bulkApply({ projectId: "note-after-bulk", target: "all", flag: "critical", add_tags: ["review"] }, () => true);
  const bookmark = api.updateFlag({ projectId: "note-after-bulk", rowIndex: 0, bookmarked: true });
  const note = api.updateFlag({ projectId: "note-after-bulk", rowIndex: 0,
    ...buildMemoPatch(opening, { memo: "finding", tags: [], bookmarked: false }) });
  try {
    release(); await Promise.all([apply, bookmark, note]);
    assert.deepEqual(order, ["bulk", "bookmark", "note"]);
    assert.deepEqual(stored, { flag: "critical", memo: "finding", tags: ["review"], bookmarked: true });
  } finally {
    release(); await Promise.allSettled([apply, bookmark, note]); clearMocks();
    if (previous) Object.defineProperty(globalThis, "window", previous);
    else Reflect.deleteProperty(globalThis, "window");
  }
});
