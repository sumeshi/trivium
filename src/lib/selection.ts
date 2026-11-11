/**
 * Pure helpers for the explicit row selection (T22).
 *
 * Selection is a set of original row ids. It never stores row objects, so page
 * cache eviction and virtual-row replacement cannot change which rows are
 * selected.
 */

export type SelectionScope = {
  /** Project that owns the row ids. */
  projectId: string | null;
  /** Applied QuerySpec signature (filters, search scope, and sort). */
  specSignature: string;
};

export type HeaderSelectionState = "none" | "some" | "all";

export function toggleRowId(
  selected: ReadonlySet<number>,
  rowIndex: number
): Set<number> {
  const next = new Set(selected);
  if (next.has(rowIndex)) {
    next.delete(rowIndex);
  } else {
    next.add(rowIndex);
  }
  return next;
}

/** Adds or removes only the given rows; selection outside them is preserved. */
export function setRowsSelected(
  selected: ReadonlySet<number>,
  rowIndices: readonly number[],
  isSelected: boolean
): Set<number> {
  const next = new Set(selected);
  for (const rowIndex of rowIndices) {
    if (isSelected) {
      next.add(rowIndex);
    } else {
      next.delete(rowIndex);
    }
  }
  return next;
}

export function emptySelection(): Set<number> {
  return new Set();
}

export function scopeChanged(
  previous: SelectionScope | null,
  next: SelectionScope
): boolean {
  return (
    previous === null ||
    previous.projectId !== next.projectId ||
    previous.specSignature !== next.specSignature
  );
}

/**
 * Keeps the selection while the same project and QuerySpec are active and
 * clears it when either changes. Page moves do not call this with a new scope.
 */
export function selectionAfterScopeChange(
  selected: ReadonlySet<number>,
  previous: SelectionScope | null,
  next: SelectionScope
): Set<number> {
  return scopeChanged(previous, next) ? emptySelection() : new Set(selected);
}

/**
 * Header checkbox state for the rows currently rendered. `"some"` is the
 * indeterminate state; rows outside the viewport are ignored.
 */
export function headerSelectionState(
  visibleRowIds: readonly number[],
  selected: ReadonlySet<number>
): HeaderSelectionState {
  if (visibleRowIds.length === 0) {
    return "none";
  }
  let selectedCount = 0;
  for (const rowIndex of visibleRowIds) {
    if (selected.has(rowIndex)) {
      selectedCount += 1;
    }
  }
  if (selectedCount === 0) {
    return "none";
  }
  return selectedCount === visibleRowIds.length ? "all" : "some";
}
