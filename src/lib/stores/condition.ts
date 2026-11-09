import { derived, writable } from "svelte/store";
import type { CompareOp, ConditionNode } from "../types";

export interface FlatCondition {
  id: string;
  column: string;
  op: CompareOp;
  value: string;
  negated: boolean;
}

export const COMPARE_OPS: Array<{ value: CompareOp; label: string }> = [
  { value: "eq", label: "=" },
  { value: "ne", label: "≠" },
  { value: "gt", label: ">" },
  { value: "lt", label: "<" },
  { value: "ge", label: "≥" },
  { value: "le", label: "≤" },
  { value: "contains", label: "contains" },
  { value: "startsWith", label: "starts" },
  { value: "endsWith", label: "ends" },
  { value: "regex", label: "regex" },
  { value: "in", label: "in" },
];

export const conditionRows = writable<FlatCondition[]>([]);
export const conditionKind = writable<"and" | "or">("and");

export function toConditionNode(
  rows: FlatCondition[],
  kind: "and" | "or"
): ConditionNode | null {
  const children: ConditionNode[] = rows
    .filter((row) => row.column.trim().length > 0)
    .map((row) => ({
      column: row.column,
      op: row.op,
      values:
        row.op === "in"
          ? row.value
              .split(",")
              .map((part) => part.trim())
              .filter((part) => part.length > 0)
          : [row.value],
      negated: row.negated,
    }));
  if (children.length === 0) {
    return null;
  }
  if (children.length === 1) {
    return children[0];
  }
  return { kind, children };
}

export const conditionPayload = derived(
  [conditionRows, conditionKind],
  ([$rows, $kind]) => toConditionNode($rows, $kind)
);

export function resetConditions(): void {
  conditionRows.set([]);
  conditionKind.set("and");
}

export function addCondition(column: string): void {
  conditionRows.update((rows) => [
    ...rows,
    {
      id: crypto.randomUUID(),
      column,
      op: "contains",
      value: "",
      negated: false,
    },
  ]);
}

export function removeCondition(id: string): void {
  conditionRows.update((rows) => rows.filter((row) => row.id !== id));
}

export function patchCondition(id: string, patch: Partial<FlatCondition>): void {
  conditionRows.update((rows) =>
    rows.map((row) => (row.id === id ? { ...row, ...patch, id: row.id } : row))
  );
}
