import { get } from "svelte/store";
import type { Backend, QueryProjectRowsResponse } from "./backend";
import type { AppliedFilters } from "./components/project_view/state";
import { PAGE_SIZE, PREFETCH_PAGES, normalizeRow } from "./components/project_view/state.ts";
import { errorMessage } from "./errors.ts";
import { isJobLimitError } from "./jobTracking.ts";
import { logicalRowAt } from "./virtualScroll.ts";
import { effectiveSearchColumns, normalizeSearchText } from "./searchScope.ts";
import type { WorkspaceController } from "./workspaceController";

interface QueryOptions {
  backend: Backend;
  isReady: () => boolean;
  restoreViewport: (result: QueryProjectRowsResponse, isCurrent: () => boolean) => Promise<void>;
  scopeChanged: () => void;
  summary: () => void;
  notify: (message: string) => void;
}

/** Paging, result generations and read-job ownership for one investigation. */
export function createWorkspaceQuery(workspace: WorkspaceController, options: QueryOptions) {
  const reads = workspace.createReadScope();
  let disposed = false;
  let revision = 0;
  let signature = "";
  let slots = 2;
  const waiters: Array<(ready: boolean) => void> = [];

  const acquire = async () => {
    if (disposed) return false;
    if (slots > 0) { slots -= 1; return true; }
    return new Promise<boolean>((resolve) => waiters.push(resolve));
  };
  const release = () => {
    const next = waiters.shift();
    if (next) next(!disposed);
    else slots += 1;
  };
  const withSlot = async <T>(isCurrent: () => boolean, work: () => Promise<T>): Promise<T | undefined> => {
    if (!await acquire()) return undefined;
    try { return !disposed && isCurrent() ? await work() : undefined; }
    finally { release(); }
  };
  const signatureFor = (filters: AppliedFilters) =>
    JSON.stringify([normalizeSearchText(filters.search), filters.flag, filters.bookmarkedOnly, effectiveSearchColumns(filters.search, filters.searchColumns),
      get(workspace.sortKey), get(workspace.sortDirection), filters.condition]);
  const resetCache = () => {
    workspace.rowsCache.set(new Map());
    workspace.positionToRowIndex.set(new Map());
    workspace.pendingPages.set(new Set());
    workspace.loadedPages.set(new Set());
    workspace.resetPageCache();
    workspace.totalRows.set(get(workspace.projectDetail)?.project.meta.total_records ?? 0);
    workspace.flaggedCount.set(0);
    workspace.filteredFlaggedCount.set(0);
  };
  const normalize = (filters: AppliedFilters): AppliedFilters => ({
    ...filters,
    search: normalizeSearchText(filters.search),
    searchColumns: effectiveSearchColumns(filters.search, filters.searchColumns),
    condition: structuredClone(filters.condition),
  });
  const initialize = (filters: AppliedFilters) => {
    reads.cancelAll();
    for (const resolve of waiters.splice(0)) resolve(false);
    revision += 1;
    const applied = normalize(filters);
    signature = signatureFor(applied);
    resetCache();
    workspace.appliedFilters.set(applied);
    workspace.syncSelectionScope(get(workspace.currentProjectId), signature);
  };

  const requestPage = async (pageIndex: number, force = false): Promise<void> => {
    const detail = get(workspace.projectDetail);
    if (disposed || !detail) return;
    const page = Math.max(0, Math.floor(pageIndex));
    const projectId = detail.project.meta.id;
    const generation = get(workspace.viewGeneration);
    const requestRevision = revision;
    const key = workspace.pendingPageKey(generation, requestRevision, page);
    const isCurrent = () => !disposed && revision === requestRevision &&
      generation === get(workspace.viewGeneration) && projectId === get(workspace.currentProjectId);
    if (!force && (get(workspace.loadedPages).has(page) || get(workspace.pendingPages).has(key))) return;
    const filters = get(workspace.appliedFilters);
    const payload = {
      projectId, search: filters.search, searchColumns: [...filters.searchColumns],
      flagFilter: filters.flag, bookmarkedOnly: filters.bookmarkedOnly,
      condition: structuredClone(filters.condition), offset: page * PAGE_SIZE, limit: PAGE_SIZE,
      sortKey: get(workspace.sortKey), sortDirection: get(workspace.sortDirection),
    };
    workspace.pendingPages.update((pending) => new Set([...pending, key]));
    try {
      await withSlot(isCurrent, async () => {
        for (let attempt = 0; attempt < 3; attempt += 1) {
          try {
            const response = await reads.run(key, options.backend, (requestId) =>
              options.backend.queryProjectRows({ ...payload, ...{ requestId } }));
            if (!isCurrent()) return;
            workspace.totalRows.set(response.total_rows);
            workspace.totalFilteredRows.set(response.total_filtered_rows);
            await options.restoreViewport(response, isCurrent);
            if (!isCurrent()) return;
            workspace.flaggedCount.set(response.annotated_total);
            workspace.filteredFlaggedCount.set(response.annotated_filtered);
            workspace.appliedResultLoading.set(false);
            workspace.registerLoadedPage(Math.floor(response.offset / PAGE_SIZE), response.offset, response.rows.map(normalizeRow));
            options.summary();
            return;
          } catch (error) {
            if (attempt < 2 && isJobLimitError(error) && isCurrent()) {
              await new Promise((resolve) => setTimeout(resolve, 250 * (attempt + 1)));
              if (!isCurrent()) return;
            } else throw error;
          }
        }
      });
    } catch (error) {
      if (isCurrent()) {
        if (get(workspace.appliedResultLoading)) workspace.totalFilteredRows.set(0);
        workspace.appliedResultLoading.set(false);
        options.notify(errorMessage(error, "Failed to load rows."));
      }
    } finally {
      if (page === 0 && isCurrent()) workspace.appliedResultLoading.set(false);
      workspace.pendingPages.update((pending) => {
        if (!pending.has(key)) return pending;
        const next = new Set(pending); next.delete(key); return next;
      });
    }
  };

  const ensureRangeLoaded = (start: number, end: number) => {
    if (disposed || !options.isReady() || end <= start) return;
    const first = Math.max(0, Math.floor(start / PAGE_SIZE) - PREFETCH_PAGES);
    const maximum = Math.floor(Math.max(get(workspace.totalFilteredRows) - 1, 0) / PAGE_SIZE);
    const last = Math.min(maximum, Math.floor(Math.max(start, end - 1) / PAGE_SIZE) + PREFETCH_PAGES);
    for (let page = first; page <= last; page += 1) {
      workspace.touchPage(page);
      const key = workspace.pendingPageKey(get(workspace.viewGeneration), revision, page);
      if (!get(workspace.largePages).has(page) && !get(workspace.loadedPages).has(page) && !get(workspace.pendingPages).has(key)) {
        void requestPage(page);
      }
    }
  };

  const applyFilters = (filters: AppliedFilters, resetScroll: boolean, force: boolean): Promise<void> => {
    if (disposed || !get(workspace.projectDetail)) return Promise.resolve();
    const normalized = normalize(filters);
    if (!force && signatureFor(normalized) === signature) return Promise.resolve();
    initialize(normalized);
    workspace.appliedResultLoading.set(true);
    options.scopeChanged();
    if (resetScroll) {
      workspace.scrollTop.set(0);
      workspace.scrollLeft.set(0);
      const body = get(workspace.bodyScrollEl), header = get(workspace.headerScrollEl);
      if (body) { body.scrollTop = 0; body.scrollLeft = 0; }
      if (header) header.scrollLeft = 0;
    }
    const row = resetScroll ? 0 : logicalRowAt(get(workspace.scrollTop), get(workspace.totalFilteredRows), get(workspace.viewportHeight));
    return requestPage(Math.floor(row / PAGE_SIZE), true);
  };

  const dispose = () => {
    if (disposed) return;
    disposed = true;
    revision += 1;
    reads.dispose();
    for (const resolve of waiters.splice(0)) resolve(false);
  };
  workspace.onDispose(dispose);
  return {
    get revision() { return revision; },
    get signature() { return signature; },
    signatureFor, initialize, resetCache, requestPage, ensureRangeLoaded, applyFilters,
    cancel: () => reads.cancelAll(),
    cancelLookup: () => reads.cancel("locate"),
    runLookup: <T>(isCurrent: () => boolean, run: (requestId: string) => Promise<T>) =>
      withSlot(isCurrent, () => reads.run("locate", options.backend, run)),
    dispose,
  };
}

export type WorkspaceQuery = ReturnType<typeof createWorkspaceQuery>;
