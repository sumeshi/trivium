import assert from "node:assert/strict";
import { test } from "node:test";
import { appendGroupPath, groupDisplayValue, groupPathKey, groupRowsPage, selectedGroupGranularities, sortGroupTree } from "./grouping.ts";
import type { GroupTreeNode } from "./types";
import type { EditorGroup } from "./stores/condition";

const node = (label: string, children: GroupTreeNode[] = [], value: unknown = label): GroupTreeNode =>
  ({ label, children, value, count: 1 });

test("group ordering sorts siblings by name or count without flattening or changing source values", () => {
  const nodes = [
    { ...node("beta", [{ ...node("z"), count: 3 }, node("a")]), count: 4 },
    { ...node("alpha", [node("9007199254740993", [], 9007199254740992)]), count: 2 },
    { ...node("gamma"), count: 2 },
  ];
  const original = structuredClone(nodes);
  const labels = (by: "name" | "count", direction: "asc" | "desc") =>
    sortGroupTree(nodes, by, direction).map(part => part.label);
  assert.deepEqual(labels("name", "asc"), ["alpha", "beta", "gamma"]);
  assert.deepEqual(labels("name", "desc"), ["gamma", "beta", "alpha"]);
  assert.deepEqual(labels("count", "asc"), ["alpha", "gamma", "beta"]);
  assert.deepEqual(labels("count", "desc"), ["beta", "alpha", "gamma"]);
  const sorted = sortGroupTree(nodes, "count", "desc");
  assert.deepEqual(sorted[0].children.map(part => part.label), ["z", "a"]);
  const originalPath = [nodes[1], nodes[1].children[0]];
  const sortedPath = [sorted[1], sorted[1].children[0]];
  assert.equal(groupPathKey(sortedPath), groupPathKey(originalPath));
  const root: EditorGroup = { type: "group", id: "root", kind: "and", collapsed: false, children: [] };
  const filtered = appendGroupPath(root, ["parent", "large"], sortedPath);
  assert.equal(filtered.children[1].type === "condition" && filtered.children[1].value, "9007199254740993");
  assert.deepEqual(nodes, original);
});

test("sorting retains null, empty and whitespace groups and exact calendar ranges", () => {
  const day = { ...node("2026-11-14"), range: { start: "2026-11-14T00:00:00Z", end: "2026-11-15T00:00:00Z" } };
  const nodes = [node("", [], null), day, node(""), node(" ")];
  const sorted = sortGroupTree(nodes, "name", "asc");
  assert.deepEqual(sorted.map(groupDisplayValue), ["(empty string)", "(missing / null)", '" "', "2026-11-14"]);
  assert.equal(new Set(sorted.map(part => groupPathKey([part]))).size, 4);
  assert.deepEqual(sorted.at(-1)?.range, day.range);
  assert.deepEqual(sortGroupTree(nodes, "name", "desc").map(groupDisplayValue), sorted.map(groupDisplayValue).reverse());
});

test("sorting precedes pagination and preserves expanded paths across sibling reordering", () => {
  const root = node("parent", Array.from({ length: 205 }, (_, index) =>
    ({ ...node(`child-${String(index).padStart(3, "0")}`), count: index + 1 })));
  const expanded = new Set([groupPathKey([root])]);
  const sorted = sortGroupTree([root], "count", "desc");
  const first = groupRowsPage(sorted, expanded, 0);
  const second = groupRowsPage(sorted, expanded, 1);
  const last = groupRowsPage(sorted, expanded, 2);
  assert.equal(first.rows[1].node.label, "child-204");
  assert.equal(second.rows[0].node.label, "child-105");
  assert.equal(last.rows.at(-1)?.node.label, "child-000");
  const rows = [first, second, last].flatMap(part => part.rows);
  assert.equal(rows.length, 206);
  assert.equal(new Set(rows.map(part => part.key)).size, 206);
  assert.deepEqual(second.rows[0].path.map(part => part.label), ["parent", "child-105"]);
});

test("legacy group columns use exact defaults and retain explicit time granularity choices", () => {
  const columns = ["__proto__", "constructor", "toString", "timestamp"];
  assert.deepEqual(selectedGroupGranularities(columns, {}), ["exact", "exact", "exact", "exact"]);
  const settings = JSON.parse('{"__proto__":"day","constructor":"hour","toString":"minute"}');
  assert.deepEqual(selectedGroupGranularities(columns, settings), ["day", "hour", "minute", "exact"]);
  assert.deepEqual(JSON.parse(JSON.stringify(selectedGroupGranularities(columns, settings))), ["day", "hour", "minute", "exact"]);
  assert.deepEqual(selectedGroupGranularities([...columns].reverse(), settings), ["exact", "minute", "hour", "day"]);
});

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
