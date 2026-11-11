import type { ExportProjectArgs } from "./backend";
import type {
  ExportOptions,
  ExportSnapshot,
} from "./components/project_view/state";

const padNumber = (value: number) => value.toString().padStart(2, "0");

/** Timestamped, filesystem-safe default name shown by the save dialog. */
export function exportFileName(projectName: string, date = new Date()): string {
  const baseName = projectName || "trivium-export.csv";
  const stem = baseName.replace(/\.[^.]+$/, "");
  const timestamp = `${date.getFullYear()}${padNumber(date.getMonth() + 1)}${padNumber(
    date.getDate()
  )}-${padNumber(date.getHours())}${padNumber(date.getMinutes())}${padNumber(
    date.getSeconds()
  )}`;
  return `${timestamp}_trivium_${stem}.csv`;
}

/**
 * Builds the IPC payload from the frozen export snapshot. Selection ids are
 * sent only for the selected scope; output columns only when requested.
 * Search scope remains `searchColumns`, never the output columns.
 */
export function buildExportArgs(
  projectId: string,
  snapshot: ExportSnapshot,
  options: ExportOptions,
  destination: string
): ExportProjectArgs {
  return {
    projectId,
    destination,
    scope: options.scope,
    search: snapshot.search,
    searchColumns: snapshot.searchColumns,
    condition: snapshot.condition,
    flagFilter: snapshot.flagFilter,
    bookmarkedOnly: snapshot.bookmarkedOnly,
    sortKey: snapshot.sortKey,
    sortDirection: snapshot.sortDirection,
    ...(options.scope === "selected"
      ? { rowIndices: snapshot.rowIndices }
      : {}),
    ...(options.useVisibleColumns
      ? { columns: snapshot.visibleColumns }
      : {}),
    includeAnnotations: options.includeAnnotations,
  };
}
