import type {
  EditorCondition,
  EditorGroup,
} from "./stores/condition";

/** Operators exposed by the cell context menu. */
export type FilterConditionOp = "eq" | "ne";

const defaultId = () => crypto.randomUUID();

/**
 * Adds one exact-match condition to the existing editor tree.
 *
 * The value is stored as its own field. It is never concatenated into a search
 * string, so quotes, colons and backslashes stay part of the value. An AND root
 * receives a new child. An OR root is wrapped in a new AND group so the
 * existing tree keeps its meaning and the new condition narrows the result.
 */
export function appendFilterCondition(
  root: EditorGroup,
  column: string,
  op: FilterConditionOp,
  value: string,
  createId: () => string = defaultId
): EditorGroup {
  const condition: EditorCondition = {
    type: "condition",
    id: createId(),
    column,
    op,
    value,
    exact: true,
    negated: false,
  };

  if (root.kind === "and") {
    return { ...root, children: [...root.children, condition] };
  }

  return {
    type: "group",
    id: root.id,
    kind: "and",
    collapsed: false,
    children: [{ ...root, id: createId() }, condition],
  };
}
