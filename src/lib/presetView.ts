import { get } from "svelte/store";
import type { PresetData } from "./types";
import type { WorkspaceController } from "./workspaceController";
import { presetCondition } from "./stores/condition.ts";

/** Apply exactly the view fields saved by the preset, in this case only. */
export function applyPresetView(workspace: Pick<WorkspaceController,
  "conditionTree" | "hiddenColumns" | "sortKey" | "sortDirection" | "sortRevision">, preset: PresetData): void {
  workspace.conditionTree.set(presetCondition(preset.condition) ?? {
    type: "group", id: "root", kind: "and", collapsed: false, children: [],
  });
  workspace.hiddenColumns.set(new Set(preset.hiddenColumns));
  const nextDirection = preset.sortDirection === "desc" ? "desc" : "asc";
  const sortChanged = get(workspace.sortKey) !== preset.sortKey || get(workspace.sortDirection) !== nextDirection;
  workspace.sortKey.set(preset.sortKey);
  workspace.sortDirection.set(nextDirection);
  if (sortChanged) workspace.sortRevision.update((revision) => revision + 1);
}
