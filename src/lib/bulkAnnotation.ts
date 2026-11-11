import type { AnnotationPatch, BulkAnnotateArgs } from "./backend";
import type { AppliedFilters } from "./components/project_view/state";

export type BulkTarget = "selection" | "filtered" | "all";

/**
 * Builds the flag/memo part of a bulk patch.
 *
 * An empty memo input means "leave the memo alone", not "delete it". Deletion
 * is an explicit `clearMemo` operation that sends `memo: null`. Tags and
 * bookmarks are never touched here.
 */
export function buildFlagMemoPatch(input: {
  flag: NonNullable<AnnotationPatch["flag"]>;
  memo: string;
  clearMemo: boolean;
}): AnnotationPatch {
  const patch: AnnotationPatch = { flag: input.flag };
  if (input.clearMemo) {
    patch.memo = null;
    return patch;
  }
  if (input.memo.trim().length > 0) {
    patch.memo = input.memo;
  }
  return patch;
}

/** Builds the full bulk IPC payload from the applied query snapshot. */
export function buildBulkAnnotateArgs(input: {
  projectId: string;
  target: BulkTarget;
  selectedRowIds: number[];
  filters: AppliedFilters;
  sortKey: string | null;
  sortDirection: "asc" | "desc";
  previewToken: string | null;
  patch: AnnotationPatch;
}): BulkAnnotateArgs {
  return {
    projectId: input.projectId,
    target: input.target,
    ...(input.target === "selection"
      ? { rowIndices: [...input.selectedRowIds] }
      : {}),
    search: input.filters.search.trim() || undefined,
    searchColumns: [...input.filters.searchColumns],
    flagFilter: input.filters.flag,
    bookmarkedOnly: input.filters.bookmarkedOnly,
    sortKey: input.sortKey,
    sortDirection: input.sortDirection,
    condition: input.filters.condition,
    ...(input.previewToken ? { previewToken: input.previewToken } : {}),
    ...input.patch,
  };
}
