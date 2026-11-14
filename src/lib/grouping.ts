import type { GroupTreeNode } from "./types";
import type { EditorCondition, EditorGroup } from "./stores/condition";
import type * as Wire from "./generated/engine";

export const MAX_GROUP_LEVELS = 8;
export const GROUP_PAGE_SIZE = 100;
export type GroupGranularity = Wire.GroupGranularity;
export const GROUP_GRANULARITIES = [
  ["exact", "Exact"], ["year", "Year (UTC)"], ["month", "Month (UTC)"],
  ["day", "Day (UTC)"], ["hour", "Hour (UTC)"], ["minute", "Minute (UTC)"],
] as const satisfies readonly (readonly [GroupGranularity, string])[];

export type GroupRow = {
  node: GroupTreeNode;
  depth: number;
  path: GroupTreeNode[];
  key: string;
};

// JSON cannot encode NaN/infinity: those keep their nonempty source labels.
export const isMissingGroup = (node: GroupTreeNode): boolean => node.value === null && node.label === "";

/** JSON tuple keys retain separators, types, null and empty string distinctly. */
export function groupPathKey(path: GroupTreeNode[]): string {
  return JSON.stringify(path.map((node) => [isMissingGroup(node), node.label]));
}

export function groupDisplayValue(node: GroupTreeNode): string {
  if (isMissingGroup(node)) return "(missing / null)";
  if (node.label === "") return "(empty string)";
  if (node.label.trim() === "") return JSON.stringify(node.label);
  return node.label;
}

/** Walk only expanded branches, keeping at most one page of rendered rows. */
export function groupRowsPage(
  nodes: GroupTreeNode[],
  expanded: ReadonlySet<string>,
  requestedPage: number,
  pageSize = GROUP_PAGE_SIZE,
): { rows: GroupRow[]; total: number; page: number; pages: number } {
  const size = Math.max(1, Math.floor(pageSize));
  const requested = Math.max(0, Math.floor(requestedPage));
  const rows: GroupRow[] = [];
  let count = 0;
  let finalRows: GroupRow[] = [];
  const visit = (children: GroupTreeNode[], parents: GroupTreeNode[]) => {
    for (const node of children) {
      const path = [...parents, node];
      const key = groupPathKey(path);
      const row = { node, depth: parents.length, path, key };
      if (count >= requested * size && rows.length < size) rows.push(row);
      if (count % size === 0) finalRows = [];
      finalRows.push(row);
      count += 1;
      if (expanded.has(key)) visit(node.children, path);
    }
  };
  visit(nodes, []);
  const pages = Math.max(1, Math.ceil(count / size));
  const page = Math.min(requested, pages - 1);
  return { rows: page === requested ? rows : finalRows, total: count, page, pages };
}

/** Append an exact group path while preserving the current AND/OR expression. */
export function appendGroupPath(
  root: EditorGroup,
  columns: string[],
  path: GroupTreeNode[],
  createId: () => string = () => crypto.randomUUID(),
): EditorGroup {
  const children: EditorCondition[] = [];
  path.forEach((node, index) => {
    const column = columns[index];
    if (!column) return;
    const condition = (op: EditorCondition["op"], value: string): EditorCondition => ({
      type: "condition", id: createId(), column, op, value, exact: true, negated: false,
    });
    if (node.range) {
      // Existing ordered comparisons reject blank source strings. Short-circuit
      // them before the exact nanosecond comparisons; null also stays excluded.
      children.push(condition("regex", "\\S"), condition("ge", node.range.start), condition("lt", node.range.end));
    } else if (isMissingGroup(node)) {
      // Regex sees null as empty, whereas exact equality distinguishes them.
      children.push(condition("ne", ""), condition("regex", "\\A\\z"));
    } else {
      // The original text is exact even for integers beyond JS Number precision.
      children.push(condition("eq", node.label));
    }
  });
  if (children.length === 0) return root;
  if (root.kind === "and") return { ...root, children: [...root.children, ...children] };
  return {
    type: "group", id: root.id, kind: "and", collapsed: false,
    children: [{ ...root, id: createId() }, ...children],
  };
}
