import assert from "node:assert/strict";
import { test } from "node:test";

import { appendFilterCondition } from "./conditionFilters.ts";
import type { EditorGroup } from "./stores/condition.ts";

const group = (
  kind: "and" | "or",
  children: EditorGroup["children"] = []
): EditorGroup => ({
  type: "group",
  id: "root",
  kind,
  collapsed: false,
  children,
});

const idFactory = () => {
  let next = 0;
  return () => {
    next += 1;
    return `generated-${next}`;
  };
};

test("an AND root receives the condition and keeps its existing children", () => {
  const root = group("and");
  root.children.push({
    type: "condition",
    id: "existing",
    column: "message",
    op: "contains",
    value: "error",
    negated: false,
  });

  const next = appendFilterCondition(root, "path", "eq", "C:\\logs\\a.txt", idFactory());

  assert.equal(next.kind, "and");
  assert.equal(next.id, "root");
  assert.equal(next.children.length, 2);
  assert.deepEqual(next.children[0], root.children[0]);
  assert.deepEqual(next.children[1], {
    type: "condition",
    id: "generated-1",
    column: "path",
    op: "eq",
    value: "C:\\logs\\a.txt",
    exact: true,
    negated: false,
  });
  // The stored editor tree is not mutated in place.
  assert.equal(root.children.length, 1);
});

test("an OR root is wrapped in AND so the existing tree keeps its meaning", () => {
  const root = group("or");
  root.children.push({
    type: "condition",
    id: "existing",
    column: "status",
    op: "eq",
    value: "failed",
    exact: true,
    negated: false,
  });

  const next = appendFilterCondition(root, "user", "ne", "service:worker", idFactory());

  assert.equal(next.kind, "and");
  assert.equal(next.id, "root");
  assert.equal(next.children.length, 2);

  const wrapped = next.children[0];
  assert.equal(wrapped.type, "group");
  assert.equal(wrapped.type === "group" ? wrapped.kind : null, "or");
  assert.deepEqual(
    wrapped.type === "group" ? wrapped.children : [],
    root.children
  );

  assert.deepEqual(next.children[1], {
    type: "condition",
    id: "generated-1",
    column: "user",
    op: "ne",
    value: "service:worker",
    exact: true,
    negated: false,
  });
});

test("quotes, colons and backslashes stay in the value field", () => {
  const value = 'cmd: "C:\\Program Files\\tool.exe" --flag=\'a:b\'';
  const next = appendFilterCondition(group("and"), "command_line", "eq", value, idFactory());
  const leaf = next.children[0];

  assert.equal(leaf.type, "condition");
  if (leaf.type !== "condition") return;
  assert.equal(leaf.column, "command_line");
  assert.equal(leaf.value, value);
  assert.equal(leaf.op, "eq");
  assert.equal(leaf.exact, true);
  // The value is not escaped, split or joined with a column/operator string.
  assert.equal(leaf.value.includes(": "), true);
  assert.equal(leaf.value.includes("\\"), true);
  assert.equal(leaf.value.includes('"'), true);
});
