import { get, writable } from "svelte/store";
import type { Backend } from "./backend";
import type { WorkspaceController } from "./workspaceController";
import type { WorkspaceQuery } from "./workspaceQuery";
import { PAGE_SIZE, ROW_HEIGHT } from "./components/project_view/state.ts";
import { clampRow, logicalRowAt, scrollTopForRow } from "./virtualScroll.ts";
import { errorMessage } from "./errors.ts";
import { effectiveSearchColumns, normalizeSearchText } from "./searchScope.ts";

interface NavigationOptions {
  backend: Backend;
  isReady: () => boolean;
  isFilterPending: () => boolean;
  beforeScroll: () => void;
  notify: (message: string) => void;
}

/** Distinguishes original evidence IDs from positions in the current result. */
export function createWorkspaceNavigation(
  workspace: WorkspaceController,
  queries: WorkspaceQuery,
  options: NavigationOptions,
) {
  const locatingRow = writable(false);
  let sequence = 0;
  let disposed = false;

  const rowOffset = () => clampRow(logicalRowAt(
    get(workspace.bodyScrollEl)?.scrollTop ?? get(workspace.scrollTop),
    get(workspace.totalFilteredRows), get(workspace.viewportHeight),
  ), get(workspace.totalFilteredRows));

  const applyScrollOffset = (offset: number, totalOverride?: number) => {
    const total = totalOverride ?? get(workspace.totalFilteredRows);
    const px = scrollTopForRow(clampRow(offset, total), total, get(workspace.viewportHeight));
    workspace.scrollTop.set(px);
    const body = get(workspace.bodyScrollEl);
    if (body) body.scrollTop = px;
  };

  const scrollToRow = (position: number, center = false) => {
    if (disposed || !options.isReady()) return;
    const total = get(workspace.totalFilteredRows);
    if (total <= 0) return;
    const target = clampRow(position, total);
    options.beforeScroll();
    const top = center
      ? Math.max(0, target - Math.floor(get(workspace.viewportHeight) / ROW_HEIGHT / 2))
      : target;
    applyScrollOffset(top, total);
    workspace.rememberOffset(rowOffset());
    void queries.requestPage(Math.floor(target / PAGE_SIZE));
  };

  const navigationQuery = () => ({
    projectId: get(workspace.currentProjectId),
    search: normalizeSearchText(get(workspace.search)),
    searchColumns: effectiveSearchColumns(get(workspace.search), get(workspace.searchColumns)),
    condition: structuredClone(get(workspace.conditionPayload)),
    flagFilter: get(workspace.flagFilter),
    bookmarkedOnly: get(workspace.bookmarkedOnly),
    sortKey: get(workspace.sortKey),
    sortDirection: get(workspace.sortDirection),
  });

  const jumpToEnteredRow = async (number: number | undefined) => {
    if (disposed || !options.isReady() || get(locatingRow) ||
      get(workspace.appliedResultLoading) || options.isFilterPending()) return;
    const total = get(workspace.totalRows);
    if (number === undefined || !Number.isSafeInteger(number) || number < 1 || number > total) {
      options.notify(`Enter an original row number from 1 to ${total.toLocaleString()}.`);
      return;
    }
    const query = navigationQuery();
    const projectId = query.projectId;
    if (!projectId) return;
    const signature = JSON.stringify(query);
    const generation = get(workspace.viewGeneration);
    const revision = queries.revision;
    const ticket = ++sequence;
    const isCurrent = () => !disposed && ticket === sequence &&
      generation === get(workspace.viewGeneration) && revision === queries.revision &&
      !options.isFilterPending() && !get(workspace.appliedResultLoading) &&
      signature === JSON.stringify(navigationQuery());
    locatingRow.set(true);
    try {
      const result = await queries.runLookup(isCurrent, (requestId) =>
        options.backend.locateProjectRow({ ...query, projectId, rowIndex: number - 1, requestId }));
      if (!result || !isCurrent()) return;
      if (result.position === null) {
        options.notify(`Row ${number.toLocaleString()} is not in the current filtered result.`);
        return;
      }
      scrollToRow(result.position, true);
    } catch (error) {
      if (isCurrent()) options.notify(errorMessage(error));
    } finally {
      if (ticket === sequence) locatingRow.set(false);
    }
  };

  const cancelLookup = () => {
    sequence += 1;
    locatingRow.set(false);
    queries.cancelLookup();
  };
  const jumpToBoundary = (last: boolean) => {
    cancelLookup();
    scrollToRow(last ? Math.max(0, get(workspace.totalFilteredRows) - 1) : 0);
  };
  workspace.onDispose(() => { disposed = true; sequence += 1; locatingRow.set(false); });
  return {
    locatingRow, rowOffset, applyScrollOffset, jumpToEnteredRow, cancelLookup,
    jumpToFirstRow: () => jumpToBoundary(false),
    jumpToLastRow: () => jumpToBoundary(true),
  };
}
