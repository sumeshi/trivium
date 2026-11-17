import type { AnnotationPatch } from "./backend";
import type { FlagEntry } from "./types";

/** Only edited fields belong to this draft; background changes keep their values. */
export function buildMemoPatch(
  baseline: Pick<FlagEntry, "memo" | "tags" | "bookmarked">,
  draft: { memo: string; tags: readonly string[]; bookmarked: boolean },
): AnnotationPatch {
  const patch: AnnotationPatch = {};
  if (draft.memo !== (baseline.memo ?? "")) patch.memo = draft.memo.length ? draft.memo : null;
  if (JSON.stringify(draft.tags) !== JSON.stringify(baseline.tags ?? [])) patch.tags = [...draft.tags];
  if (draft.bookmarked !== (baseline.bookmarked ?? false)) patch.bookmarked = draft.bookmarked;
  return patch;
}
