import type { BulkAnnotateArgs } from "./backend";
import type { IocEntry } from "./types";

export function iocRuleKey(projectId: string, entry: IocEntry): string {
  return JSON.stringify([projectId, entry.flag, entry.tag.trim(), entry.query.trim()]);
}

/** IOC rules use the normal bulk preview/apply transaction, never a live overlay. */
export function iocBulkArgs(projectId: string, entry: IocEntry, previewToken?: string): BulkAnnotateArgs {
  const search = entry.query.trim(), tag = entry.tag.trim();
  if (!search) throw new Error("Enter a query first.");
  if (!["safe", "suspicious", "critical"].includes(entry.flag)) throw new Error("Choose a flag first.");
  return {
    projectId, target: "filtered", search, searchColumns: [], condition: null,
    flagFilter: "all", bookmarkedOnly: false, sortKey: null, sortDirection: "asc",
    flag: entry.flag,
    ...(tag ? { add_tags: [tag] } : {}),
    ...(previewToken ? { previewToken } : {}),
  };
}
