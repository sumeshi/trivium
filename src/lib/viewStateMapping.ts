import type { ViewState } from "./types";
import type { EditorGroup } from "./stores/condition";
import type { SavedView } from "./viewState";
import { isFlagFilterValue } from "./flagFilters.ts";
import type { ColumnDisplayFormat } from "./dateDisplay";
import { isFormatDefinition } from "./formats.ts";

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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
      (value.exact === undefined || typeof value.exact === "boolean") &&
      (value.sourceValues === undefined || (Array.isArray(value.sourceValues) &&
        value.sourceValues.every((item) => typeof item === "string")))
    );
  }
  if (value.type === "format") {
    return typeof value.id === "string" && Array.isArray(value.columns)
      && value.columns.every(column => typeof column === "string") && isFormatDefinition(value.format)
      && typeof value.negated === "boolean" && (value.equals === undefined || typeof value.equals === "string");
  }
  if (value.type === "group") {
    return (
      typeof value.id === "string" &&
      (value.kind === "and" || value.kind === "or") &&
      typeof value.collapsed === "boolean" &&
      Array.isArray(value.children) &&
      value.children.every((child) => isNode(child)) &&
      (value.snippets === undefined || (Array.isArray(value.snippets) && value.snippets.every(snippet =>
        isRecord(snippet) && typeof snippet.id === "string" && isFormatDefinition(snippet.format))))
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

function knownColumns(value: readonly string[], known: Set<string>): string[] {
  return value.filter(column => known.has(column));
}

function strings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every(item => typeof item === "string");
}

function columnWidths(
  value: Record<string, unknown>,
  known: Set<string>
): Record<string, number> {
  return Object.fromEntries(Object.entries(value).filter(([column]) =>
    known.has(column)
  )) as Record<string, number>;
}

function columnFormats(value: Record<string, unknown>, known: Set<string>): Record<string, ColumnDisplayFormat> {
  return Object.fromEntries(Object.entries(value).filter(([id]) => known.has(id))) as Record<string, ColumnDisplayFormat>;
}

/**
 * Validates the current persisted schema before resolving its column IDs.
 */
export function savedViewFromUnknown(
  value: unknown,
  columns: readonly string[]
): SavedView | null {
  if (!isRecord(value) || value.version !== 1 || Object.hasOwn(value, "offset")
    || typeof value.search !== "string" || !strings(value.searchColumns)
    || !isFlagFilterValue(value.flagFilter) || typeof value.bookmarkedOnly !== "boolean"
    || !strings(value.hiddenColumns) || (value.sortKey !== null && typeof value.sortKey !== "string")
    || (value.sortDirection !== "asc" && value.sortDirection !== "desc")
    || !isRecord(value.columnWidths) || Object.values(value.columnWidths).some(width =>
      typeof width !== "number" || !Number.isFinite(width) || width <= 0)
    || typeof value.logicalRow !== "number" || !Number.isSafeInteger(value.logicalRow) || value.logicalRow < 0
    || (value.frozenThrough !== undefined && typeof value.frozenThrough !== "string")
    || (value.noteColumnWidth !== undefined && (typeof value.noteColumnWidth !== "number"
      || !Number.isFinite(value.noteColumnWidth) || value.noteColumnWidth <= 0))
    || (value.columnOrder !== undefined && !strings(value.columnOrder))
    || (value.columnFormats !== undefined && (!isRecord(value.columnFormats)
      || Object.values(value.columnFormats).some(format => format !== "iso")))) {
    return null;
  }
  const condition = conditionTree(value.condition);
  if (!condition) {
    return null;
  }
  const known = new Set(columns);
  const frozenThrough = typeof value.frozenThrough === "string" &&
    (value.frozenThrough === "" || known.has(value.frozenThrough)) ? value.frozenThrough : undefined;
  return {
    condition,
    search: value.search,
    searchColumns: knownColumns(value.searchColumns, known),
    flagFilter: value.flagFilter,
    bookmarkedOnly: value.bookmarkedOnly,
    hiddenColumns: knownColumns(value.hiddenColumns, known),
    ...(frozenThrough !== undefined ? { frozenThrough } : {}),
    sortKey:
      typeof value.sortKey === "string" && known.has(value.sortKey)
        ? value.sortKey
        : null,
    sortDirection: value.sortDirection,
    columnWidths: columnWidths(value.columnWidths, known),
    ...(value.noteColumnWidth !== undefined
      ? { noteColumnWidth: value.noteColumnWidth } : {}),
    ...(Array.isArray(value.columnOrder) ? { columnOrder: [...new Set(knownColumns(value.columnOrder, known))] } : {}),
    ...(isRecord(value.columnFormats) ? { columnFormats: columnFormats(value.columnFormats, known) } : {}),
    logicalRow: value.logicalRow,
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

/**
 * Names saved fields whose stored values cannot be applied to the current
 * project. The caller shows a notice; the original file is not rewritten.
 */
export function viewStateWarnings(
  state: ViewState,
  columns: readonly string[]
): string[] {
  const known = new Set(columns);
  const warnings: string[] = [];
  const unknownSearch = state.searchColumns.filter((column) => !known.has(column));
  if (unknownSearch.length > 0) {
    warnings.push(`search columns ${unknownSearch.join(", ")}`);
  }
  if (!isFlagFilterValue(state.flagFilter)) {
    warnings.push("flag filter");
  }
  if (state.sortKey !== null && !known.has(state.sortKey)) {
    warnings.push(`sort column ${state.sortKey}`);
  }
  if (state.sortDirection !== "asc" && state.sortDirection !== "desc") warnings.push("sort direction");
  if (state.frozenThrough !== undefined && state.frozenThrough !== "" && !known.has(state.frozenThrough)) {
    warnings.push(`frozen column ${state.frozenThrough}`);
  }
  const unknownHidden = state.hiddenColumns.filter((column) => !known.has(column));
  if (unknownHidden.length > 0) {
    warnings.push(`hidden columns ${unknownHidden.join(", ")}`);
  }
  if (state.columnOrder?.some((id) => !known.has(id))) warnings.push('column order');
  if (Object.entries(state.columnFormats ?? {}).some(([id, format]) => !known.has(id) || format !== 'iso')) warnings.push('column formats');
  if (Object.entries(state.columnWidths).some(([id, width]) => !known.has(id) || !Number.isFinite(width) || width <= 0)) warnings.push('column widths');
  if (state.noteColumnWidth !== undefined && (!Number.isFinite(state.noteColumnWidth) || state.noteColumnWidth <= 0)) warnings.push('note column width');
  return warnings;
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
    ...(view.frozenThrough !== undefined ? { frozenThrough: view.frozenThrough } : {}),
    columnWidths: { ...view.columnWidths },
    ...(view.noteColumnWidth !== undefined ? { noteColumnWidth: view.noteColumnWidth } : {}),
    ...(view.columnOrder ? { columnOrder: [...view.columnOrder] } : {}),
    ...(view.columnFormats ? { columnFormats: { ...view.columnFormats } } : {}),
    logicalRow: view.logicalRow,
  };
}
