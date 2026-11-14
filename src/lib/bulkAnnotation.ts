import type { AnnotationPatch, Backend, BulkAnnotateArgs } from "./backend";
import type { BulkApplied, BulkCount } from "./types";
import type { AppliedFilters } from "./components/project_view/state";

export type BulkTarget = "selection" | "filtered" | "all";
export type BulkFlag = "keep" | NonNullable<AnnotationPatch["flag"]>;
export type BulkTagOperation = "add" | "remove";

/** Omitted fields stay unchanged; tag text is one exact label, including commas. */
export function buildBulkPatch(input: {
  flag: BulkFlag;
  memo: string;
  clearMemo: boolean;
  tag: string;
  tagOperation: BulkTagOperation;
}): AnnotationPatch {
  const patch: AnnotationPatch = {};
  if (input.flag !== "keep") patch.flag = input.flag;
  if (input.clearMemo) patch.memo = null;
  else if (input.memo.trim()) patch.memo = input.memo;
  const tag = input.tag.trim();
  if (tag) patch[input.tagOperation === "remove" ? "remove_tags" : "add_tags"] = [tag];
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

/** Direct Apply and optional Preview share one frozen, token-checked operation. */
export async function applyBulkAnnotation(
  api: Pick<Backend, "bulkPreview" | "bulkUpdate">,
  args: BulkAnnotateArgs,
  options: {
    preview?: BulkCount | null;
    isCurrent: () => boolean;
  },
): Promise<BulkApplied | null> {
  if (!options.isCurrent()) return null;
  const snapshot = structuredClone(args);
  const preview = options.preview ?? await api.bulkPreview(snapshot);
  if (!options.isCurrent()) return null;
  // An empty operation must not replace the last useful undo record.
  if (preview.count === 0) return { updated: 0 };
  return api.bulkUpdate({ ...snapshot, previewToken: preview.previewToken });
}
