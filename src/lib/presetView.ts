import { get } from "svelte/store";
import type { PresetData } from "./types";
import type { WorkspaceController } from "./workspaceController";
import { presetCondition } from "./stores/condition.ts";
import { displayColumnOrder } from "./columnLayoutActions.ts";

/** Omitted layout fields leave the destination case's presentation alone. */
export function changedPresetLayout(
  columns: readonly string[], order: readonly string[], widths: ReadonlyMap<string, number>,
  defaultWidths: ReadonlyMap<string, number>,
): Pick<PresetData, "columnOrder" | "columnWidths"> {
  const layout: Pick<PresetData, "columnOrder" | "columnWidths"> = {};
  const displayed = displayColumnOrder(columns, order);
  if (displayed.some((column, index) => column !== columns[index])) layout.columnOrder = displayed;
  const changed: Record<string, number> = Object.create(null);
  for (const column of columns) {
    const width = widths.get(column);
    if (width !== undefined && Number.isFinite(width) && width > 0 && width !== defaultWidths.get(column)) changed[column] = width;
  }
  if (Object.keys(changed).length) layout.columnWidths = changed;
  return layout;
}

/** Apply exactly the view fields saved by the preset, in this case only. */
export function applyPresetView(workspace: Pick<WorkspaceController,
  "conditionTree" | "hiddenColumns" | "sortKey" | "sortDirection" | "sortRevision" | "columnOrder">, preset: PresetData): void {
  workspace.conditionTree.set(presetCondition(preset.condition) ?? {
    type: "group", id: "root", kind: "and", collapsed: false, children: [],
  });
  workspace.hiddenColumns.set(new Set(preset.hiddenColumns));
  if (preset.columnOrder !== undefined) workspace.columnOrder.set([...preset.columnOrder]);
  const nextDirection = preset.sortDirection === "desc" ? "desc" : "asc";
  const sortChanged = get(workspace.sortKey) !== preset.sortKey || get(workspace.sortDirection) !== nextDirection;
  workspace.sortKey.set(preset.sortKey);
  workspace.sortDirection.set(nextDirection);
  if (sortChanged) workspace.sortRevision.update((revision) => revision + 1);
}
