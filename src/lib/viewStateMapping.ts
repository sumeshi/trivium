import type { FlagFilterValue, ViewState } from "./types";
import type { EditorGroup } from "./stores/condition";
import type { SavedView } from "./viewState";

const COMPARE_OPS = new Set([
  "eq",
  "ne",
  "gt",
  "lt",
  "ge",
  "le",
  "contains",
  "startsWith",
  "endsWith",
  "regex",
  "in",
]);

const FLAG_FILTERS = new Set<FlagFilterValue>([
  "all",
  "none",
  "priority",
  "safe",
  "suspicious",
  "critical",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isNode(value: unknown): boolean {
  if (!isRecord(value) || typeof value.type !== "string") {
    return false;
  }
  if (value.type === "condition") {
    return (
      typeof value.id === "string" &&
      typeof value.column === "string" &&
      typeof value.op === "string" &&
      COMPARE_OPS.has(value.op) &&
      typeof value.value === "string" &&
      typeof value.negated === "boolean" &&
      (value.exact === undefined || typeof value.exact === "boolean")
    );
  }
  if (value.type === "group") {
    return (
      typeof value.id === "string" &&
      (value.kind === "and" || value.kind === "or") &&
      typeof value.collapsed === "boolean" &&
      Array.isArray(value.children) &&
      value.children.every((child) => isNode(child))
    );
  }
  return false;
}

function conditionTree(value: unknown): EditorGroup | null {
  if (!isNode(value) || (value as { type?: string }).type !== "group") {
    return null;
  }
  return value as EditorGroup;
}

function stringArray(value: unknown, known: Set<string>): string[] {
  return Array.isArray(value)
    ? value.filter(
        (item): item is string => typeof item === "string" && known.has(item)
      )
    : [];
}

function flagFilter(value: unknown): FlagFilterValue {
  return typeof value === "string" && FLAG_FILTERS.has(value as FlagFilterValue)
    ? (value as FlagFilterValue)
    : "all";
}

function sortDirection(value: unknown): "asc" | "desc" {
  return value === "desc" ? "desc" : "asc";
}

function nonNegativeInteger(value: unknown): number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0
    ? value
    : 0;
}

function columnWidths(
  value: unknown,
  known: Set<string>
): Record<string, number> {
  const widths: Record<string, number> = {};
  if (!isRecord(value)) {
    return widths;
  }
  for (const [column, width] of Object.entries(value)) {
    if (
      known.has(column) &&
      typeof width === "number" &&
      Number.isFinite(width) &&
      width > 0
    ) {
      widths[column] = width;
    }
  }
  return widths;
}

/**
 * Validates a legacy localStorage save or any parsed JSON into the current
 * SavedView shape. Returns null when the condition tree cannot be restored.
 */
export function savedViewFromUnknown(
  value: unknown,
  columns: readonly string[]
): SavedView | null {
  if (!isRecord(value)) {
    return null;
  }
  const condition = conditionTree(value.condition);
  if (!condition) {
    return null;
  }
  const known = new Set(columns);
  const rawOffset = nonNegativeInteger(value.offset);
  const logicalRow =
    value.logicalRow === undefined
      ? rawOffset
      : nonNegativeInteger(value.logicalRow);
  return {
    condition,
    search: typeof value.search === "string" ? value.search : "",
    searchColumns: stringArray(value.searchColumns, known),
    flagFilter: flagFilter(value.flagFilter),
    bookmarkedOnly: value.bookmarkedOnly === true,
    hiddenColumns: stringArray(value.hiddenColumns, known),
    sortKey:
      typeof value.sortKey === "string" && known.has(value.sortKey)
        ? value.sortKey
        : null,
    sortDirection: sortDirection(value.sortDirection),
    columnWidths: columnWidths(value.columnWidths, known),
    // `offset` is the legacy name for the result-relative row; the engine
    // keeps only `logicalRow`, so the two always agree after a round trip.
    offset: logicalRow,
    logicalRow,
  };
}

/**
 * Converts the engine DTO to the component view model. Returns null when the
 * stored condition cannot be read; the caller should report that instead of
 * silently replacing it with an empty filter.
 */
export function viewStateToSavedView(
  state: ViewState,
  columns: readonly string[]
): SavedView | null {
  return savedViewFromUnknown(state, columns);
}

/** Converts the component view model to the engine DTO, including version. */
export function savedViewToViewState(view: SavedView): ViewState {
  return {
    version: 1,
    search: view.search,
    searchColumns: [...view.searchColumns],
    condition: view.condition as unknown as Record<string, unknown>,
    flagFilter: view.flagFilter,
    bookmarkedOnly: view.bookmarkedOnly,
    sortKey: view.sortKey,
    sortDirection: view.sortDirection,
    hiddenColumns: [...view.hiddenColumns],
    columnWidths: { ...view.columnWidths },
    logicalRow: view.logicalRow ?? view.offset ?? 0,
  };
}
