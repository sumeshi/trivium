import assert from "node:assert/strict";
import { test } from "node:test";

import {
  headerSelectionState,
  selectionAfterScopeChange,
  setRowsSelected,
  toggleRowId,
} from "./selection.ts";
import type { SelectionScope } from "./selection.ts";

const scope: SelectionScope = {
  projectId: "project-a",
  specSignature: "search::all::false::::asc::null",
};

test("toggle adds and removes only the addressed row id", () => {
  const first = toggleRowId(new Set(), 7);
  assert.deepEqual([...first], [7]);
  const second = toggleRowId(first, 9);
  assert.deepEqual([...second], [7, 9]);
  const third = toggleRowId(second, 7);
  assert.deepEqual([...third], [9]);
});

test("selecting visible rows preserves selections outside the viewport", () => {
  const selected = new Set([1, 4]);
  const next = setRowsSelected(selected, [4, 8, 9], true);
  assert.deepEqual([...next].sort((a, b) => a - b), [1, 4, 8, 9]);

  const cleared = setRowsSelected(next, [4, 8], false);
  assert.deepEqual([...cleared].sort((a, b) => a - b), [1, 9]);
});

test("scope changes clear selection while page moves keep it", () => {
  const selected = new Set([3, 12, 41]);

  const same = selectionAfterScopeChange(selected, scope, { ...scope });
  assert.deepEqual([...same].sort((a, b) => a - b), [3, 12, 41]);

  const otherSpec = selectionAfterScopeChange(selected, scope, {
    ...scope,
    specSignature: `${scope.specSignature}-sorted`,
  });
  assert.equal(otherSpec.size, 0);

  const otherProject = selectionAfterScopeChange(selected, scope, {
    projectId: "project-b",
    specSignature: scope.specSignature,
  });
  assert.equal(otherProject.size, 0);

  const firstScope = selectionAfterScopeChange(selected, null, scope);
  assert.equal(firstScope.size, 0);
});

test("header state distinguishes none, some, and all visible rows", () => {
  const visible = [2, 5, 6];
  assert.equal(headerSelectionState(visible, new Set()), "none");
  assert.equal(headerSelectionState(visible, new Set([2])), "some");
  assert.equal(headerSelectionState(visible, new Set([5, 6])), "some");
  assert.equal(headerSelectionState(visible, new Set([2, 5, 6])), "all");
  // Off-screen selections do not make the header look "all".
  assert.equal(headerSelectionState(visible, new Set([2, 5, 6, 99])), "all");
  assert.equal(headerSelectionState([], new Set([1])), "none");
});
