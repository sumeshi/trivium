import assert from "node:assert/strict";
import { test } from "node:test";
import { appendGroupPath, groupDisplayValue, groupPathKey, groupRowsPage } from "./grouping.ts";
import type { GroupTreeNode } from "./types";
import type { EditorGroup } from "./stores/condition";

const node = (label: string, children: GroupTreeNode[] = [], value: unknown = label): GroupTreeNode =>
  ({ label, children, value, count: 1 });

test("group keys preserve null, empty, whitespace and embedded separators", () => {
  const paths = [[node("", [], null)], [node("")], [node(" ")],
    [node("a\u0001b")], [node("a"), node("b")]];
  assert.equal(new Set(paths.map(groupPathKey)).size, paths.length);
  assert.equal(groupDisplayValue(node("", [], null)), "(missing / null)");
  assert.equal(groupDisplayValue(node("")), "(empty string)");
  assert.equal(groupDisplayValue(node("  ")), '"  "');
  assert.equal(groupDisplayValue(node("NaN", [], null)), "NaN");
});

test("group pages include expanded ancestors only and clamp after collapse", () => {
  const children = Array.from({ length: 205 }, (_, index) => node(`child-${index}`));
  const root = node("parent", children);
  const open = new Set([groupPathKey([root])]);
  const page = groupRowsPage([root, node("other")], open, 1);
  assert.equal(page.rows.length, 100);
  assert.equal(page.total, 207);
  assert.equal(page.rows[0].node.label, "child-99");
  assert.deepEqual(page.rows[0].path.map((part) => part.label), ["parent", "child-99"]);
  const collapsed = groupRowsPage([root, node("other")], new Set(), 2);
  assert.equal(collapsed.page, 0);
  assert.equal(collapsed.pages, 1);
  assert.deepEqual(collapsed.rows.map((row) => row.node.label), ["parent", "other"]);
});

test("nested drilldown narrows an existing OR without losing exact values", () => {
  const root: EditorGroup = {
    type: "group", id: "root", kind: "or", collapsed: false,
    children: [{ type: "condition", id: "old", column: "x", op: "eq", value: "yes", negated: false }],
  };
  let id = 0;
  const next = appendGroupPath(root, ["parent", "child", "absent"],
    [node("a,b"), node("9007199254740993", [], 9007199254740992), node("", [], null)], () => `id-${++id}`);
  assert.equal(next.kind, "and");
  assert.equal(next.children[0].type === "group" && next.children[0].kind, "or");
  assert.deepEqual(next.children.slice(1).map((row) => row.type === "condition" ? [row.column, row.op, row.value] : []),
    [["parent", "eq", "a,b"], ["child", "eq", "9007199254740993"], ["absent", "ne", ""], ["absent", "regex", "\\A\\z"]]);
  assert.equal(root.children.length, 1);
});

test("empty group paths leave existing conditions untouched", () => {
  const root: EditorGroup = { type: "group", id: "root", kind: "and", collapsed: false, children: [] };
  assert.equal(appendGroupPath(root, [], []), root);
  const next = appendGroupPath(root, ["x"], [node("")]);
  assert.equal(next.children[0].type === "condition" && next.children[0].value, "");
  assert.equal(next.children[0].type === "condition" && next.children[0].op, "eq");
  const special = appendGroupPath(root, ["x"], [node("NaN", [], null)]);
  assert.equal(special.children.length, 1);
  assert.equal(special.children[0].type === "condition" && special.children[0].value, "NaN");
});

test("calendar drilldown adds an exact half-open range after excluding blank timestamps", () => {
  const root: EditorGroup = { type: "group", id: "root", kind: "and", collapsed: false, children: [] };
  const day = { ...node("2026-11-14"), range: { start: "2026-11-14T00:00:00Z", end: "2026-11-15T00:00:00Z" } };
  const next = appendGroupPath(root, ["host", "timestamp"], [node("host-a"), day]);
  assert.deepEqual(next.children.map((row) => row.type === "condition" ? [row.column, row.op, row.value] : []), [
    ["host", "eq", "host-a"], ["timestamp", "regex", "\\S"],
    ["timestamp", "ge", day.range.start], ["timestamp", "lt", day.range.end],
  ]);
  assert.equal(root.children.length, 0);
  assert.deepEqual(day.range, { start: "2026-11-14T00:00:00Z", end: "2026-11-15T00:00:00Z" });
});

test("time ranges narrow an existing OR group without replacing its filters", () => {
  const root: EditorGroup = { type: "group", id: "root", kind: "or", collapsed: false,
    children: [{ type: "condition", id: "keep", column: "message", op: "contains", value: "logon", negated: false }] };
  const hour = { ...node("2026-11-14T23:00Z"), range: { start: "2026-11-14T23:00:00Z", end: "2026-11-15T00:00:00Z" } };
  let id = 0;
  const next = appendGroupPath(root, ["timestamp"], [hour], () => `time-${++id}`);
  assert.equal(next.kind, "and");
  assert.equal(next.children[0].type === "group" && next.children[0].kind, "or");
  assert.deepEqual(next.children.slice(1).map((row) => row.type === "condition" ? row.op : "group"), ["regex", "ge", "lt"]);
  assert.equal(root.children.length, 1);
});
