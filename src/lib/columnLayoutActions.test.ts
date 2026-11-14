import assert from "node:assert/strict";
import { test } from "node:test";
import { displayColumnOrder, moveDisplayColumn } from "./columnLayoutActions.ts";
import { frozenColumnLayout } from "./frozenColumns.ts";

test('restoring an order drops removed and duplicate IDs and appends new columns', () => {
  assert.deepEqual(displayColumnOrder(['a', 'b', 'c'], ['c', 'removed', 'c']), ['c', 'a', 'b']);
  assert.deepEqual(displayColumnOrder(['a', 'b'], []), ['a', 'b']);
});

test("column drops preserve every source ID, including hidden columns, on either side", () => {
  const source = ["record", "hidden", "time", "message"];
  assert.deepEqual(moveDisplayColumn(source, "message", "record"), ["message", "record", "hidden", "time"]);
  assert.deepEqual(moveDisplayColumn(source, "record", "time", true), ["hidden", "time", "record", "message"]);
  assert.deepEqual(source, ["record", "hidden", "time", "message"]);
  for (const column of source) for (const target of source) for (const after of [false, true]) {
    const moved = moveDisplayColumn(source, column, target, after);
    assert.equal(moved.length, source.length);
    assert.deepEqual(new Set(moved), new Set(source));
  }
});

test("invalid, self and already adjacent drops leave the existing layout untouched", () => {
  const order = ["a", "b", "c"];
  assert.equal(moveDisplayColumn(order, "absent", "b"), order);
  assert.equal(moveDisplayColumn(order, "b", "absent"), order);
  assert.equal(moveDisplayColumn(order, "b", "b"), order);
  assert.equal(moveDisplayColumn(order, "a", "b"), order);
  assert.equal(moveDisplayColumn(order, "b", "a", true), order);
});

test("frozen prefixes follow the new display order while hidden IDs retain the boundary", () => {
  const order = moveDisplayColumn(["record", "hidden", "time", "message"], "time", "record");
  const visible = order.filter((id) => id !== "hidden");
  const frozen = frozenColumnLayout(order, visible, [180, 100, 240], "record", 450);
  assert.deepEqual([...frozen.offsets], [["time", 450], ["record", 630]]);
  assert.equal(frozen.width, 730);
});
