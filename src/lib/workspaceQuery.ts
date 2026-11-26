import { get } from "svelte/store";
import type { Backend, QueryProjectRowsResponse } from "./backend";
import type { AppliedFilters } from "./components/project_view/state";
import { PAGE_SIZE, PREFETCH_PAGES, normalizeRow } from "./components/project_view/state.ts";
import { errorMessage } from "./errors.ts";
import { isJobLimitError } from "./jobTracking.ts";
import { logicalRowAt } from "./virtualScroll.ts";
import { effectiveSearchColumns, normalizeSearchText } from "./searchScope.ts";
import { beginPageTiming } from "./renderPerformance.ts";
import { annotationVersions } from "./annotationVersions.ts";
import type { WorkspaceController } from "./workspaceController";

interface QueryOptions {
  backend: Backend;
  isReady: () => boolean;
  restoreViewport: (result: QueryProjectRowsResponse, isCurrent: () => boolean) => Promise<void>;
  scopeChanged: (logicalRow: number) => void;
  summary: () => void;
  notify: (message: string) => void;
}

type PageRequest = {
  key: string;
  page: number;
  explicit: boolean;
  running: boolean;
  cancelled: boolean;
  pageColumns: string[];
  priority: number;
  promise: Promise<void>;
};
type SlotWaiter = {
  priority: () => number;
  isCurrent: () => boolean;
  resolve: (ready: boolean) => void;
};

/** Paging, result generations and read-job ownership for one investigation. */
export function createWorkspaceQuery(workspace: WorkspaceController, options: QueryOptions) {
  const reads = workspace.createReadScope();
  let disposed = false;
  // Membership changes invalidate navigation; presentation changes retire pages only.
  let revision = 0;
  let pageEpoch = 0;
  let signature = "";
  let projectionProject: string | null = null;
  let projectionKey = "";
  let slots = 2;
  const waiters: SlotWaiter[] = [];
  const pageRequests = new Map<string, PageRequest>();
  let wantedPages = new Set<number>();
  let previousStart: number | null = null;
  let direction = 1;
  const failedPages = new Map<number, string>();
  const publishError = () => {
    const last = [...failedPages].at(-1);
    workspace.queryError.set(last ? { page: last[0], message: last[1] } : null);
  };

  const discardStaleWaiters = () => {
    for (let index = waiters.length - 1; index >= 0; index -= 1) {
      if (disposed || !waiters[index].isCurrent()) waiters.splice(index, 1)[0].resolve(false);
    }
  };
  const acquire = async (isCurrent: () => boolean, priority: () => number) => {
    if (disposed || !isCurrent()) return false;
    discardStaleWaiters();
    if (slots > 0) { slots -= 1; return true; }
    return new Promise<boolean>((resolve) => waiters.push({ isCurrent, priority, resolve }));
  };
  const release = () => {
    discardStaleWaiters();
    let best = 0;
    for (let index = 1; index < waiters.length; index += 1) {
      if (waiters[index].priority() < waiters[best].priority()) best = index;
    }
    if (waiters.length) waiters.splice(best, 1)[0].resolve(true);
    else slots += 1;
  };
  const withSlot = async <T>(isCurrent: () => boolean, work: () => Promise<T>, priority = () => 0): Promise<T | undefined> => {
    if (!await acquire(isCurrent, priority)) return undefined;
    try { return !disposed && isCurrent() ? await work() : undefined; }
    finally { release(); }
  };
  const forgetPageRequest = (request: PageRequest) => {
    if (pageRequests.get(request.key) !== request) return;
    pageRequests.delete(request.key);
    workspace.pendingPages.update((pending) => {
      if (!pending.has(request.key)) return pending;
      const next = new Set(pending); next.delete(request.key); return next;
    });
  };
  const signatureFor = (filters: AppliedFilters) =>
    JSON.stringify([normalizeSearchText(filters.search), filters.flag, filters.bookmarkedOnly, effectiveSearchColumns(filters.search, filters.searchColumns),
      get(workspace.sortKey), get(workspace.sortDirection), filters.condition]);
  const clearPages = () => {
    workspace.rowsCache.set(new Map());
    workspace.positionToRowIndex.set(new Map());
    workspace.pendingPages.set(new Set());
    workspace.loadedPages.set(new Set());
    workspace.resetPageCache();
    failedPages.clear();
    publishError();
  };
  const resetCache = () => {
    clearPages();
    workspace.totalRows.set(get(workspace.projectDetail)?.project.meta.total_records ?? 0);
    workspace.queryCountKnown.set(false);
    workspace.flaggedCount.set(0);
    workspace.filteredFlaggedCount.set(0);
  };
  const normalize = (filters: AppliedFilters): AppliedFilters => ({
    ...filters,
    search: normalizeSearchText(filters.search),
    searchColumns: effectiveSearchColumns(filters.search, filters.searchColumns),
    condition: structuredClone(filters.condition),
  });
  const retirePageReads = () => {
    pageEpoch += 1;
    for (const request of pageRequests.values()) reads.cancel(request.key);
    discardStaleWaiters();
    pageRequests.clear();
    wantedPages.clear();
    previousStart = null;
    direction = 1;
  };
  const initialize = (filters: AppliedFilters) => {
    revision += 1;
    reads.cancelAll();
    retirePageReads();
    for (const waiter of waiters.splice(0)) waiter.resolve(false);
    projectionProject = get(workspace.currentProjectId);
    projectionKey = JSON.stringify([...get(workspace.visibleColumns)].sort());
    const applied = normalize(filters);
    signature = signatureFor(applied);
    resetCache();
    workspace.appliedFilters.set(applied);
    workspace.syncSelectionScope(get(workspace.currentProjectId), signature);
  };

  const loadPage = async (pageIndex: number, force = false, explicit = true): Promise<void> => {
    const detail = get(workspace.projectDetail);
    if (disposed || !detail) return;
    const page = Math.max(0, Math.floor(pageIndex));
    const projectId = detail.project.meta.id;
    const generation = get(workspace.viewGeneration);
    const requestEpoch = pageEpoch;
    const key = workspace.pendingPageKey(generation, requestEpoch, page);
    const isCurrent = () => !disposed && pageEpoch === requestEpoch &&
      generation === get(workspace.viewGeneration) && projectId === get(workspace.currentProjectId);
    const existing = pageRequests.get(key);
    if (existing) {
      existing.explicit ||= explicit;
      existing.priority = existing.explicit ? 0 : Math.min(existing.priority, 1);
      return existing.promise;
    }
    if (!force && get(workspace.loadedPages).has(page)) return;
    if (!explicit && failedPages.has(page)) return;
    failedPages.delete(page);
    publishError();
    if (!get(workspace.queryCountKnown)) workspace.appliedResultLoading.set(true);
    const pageColumns = [...get(workspace.visibleColumns)];
    const request: PageRequest = { key, page, explicit, running: false, cancelled: false, pageColumns,
      priority: explicit ? 0 : 1, promise: Promise.resolve() };
    const timing = beginPageTiming(page * PAGE_SIZE, PAGE_SIZE);
    pageRequests.set(key, request);
    const filters = get(workspace.appliedFilters);
    const payload = {
      projectId, search: filters.search, searchColumns: [...filters.searchColumns],
      flagFilter: filters.flag, bookmarkedOnly: filters.bookmarkedOnly,
      condition: structuredClone(filters.condition), offset: page * PAGE_SIZE, limit: PAGE_SIZE,
      sortKey: get(workspace.sortKey), sortDirection: get(workspace.sortDirection),
      pageColumns,
    };
    request.promise = (async () => {
      try {
        for (let attempt = 0; attempt < 3; attempt += 1) {
          try {
            const published = await withSlot(() => isCurrent() && !request.cancelled &&
              (request.explicit || wantedPages.has(page)), async () => {
              request.running = true;
              const annotationVersion = get(annotationVersions)[projectId] ?? 0;
              timing?.mark("dispatched");
              const response = await reads.run(key, options.backend, (requestId) =>
                options.backend.queryProjectRows({ ...payload, ...{ requestId } }));
              timing?.mark("responded");
              const isFresh = () => isCurrent() && annotationVersion === (get(annotationVersions)[projectId] ?? 0)
                && workspace.isAnnotationRevisionCurrent(response.annotation_revision);
              if (!isCurrent()) return true;
              if (!isFresh()) return false;
              workspace.totalRows.set(response.total_rows);
              workspace.totalFilteredRows.set(response.total_filtered_rows);
              await options.restoreViewport(response, isFresh);
              if (!isCurrent()) return true;
              if (!isFresh()) return false;
              workspace.queryCountKnown.set(true);
              workspace.appliedResultLoading.set(false);
              failedPages.delete(page);
              publishError();
              timing?.rows(response.rows.length, Object.keys(response.rows[0]?.cells ?? {}).length);
              timing?.mark("normalizeStarted");
              const rows = response.rows.map(normalizeRow);
              timing?.mark("normalized");
              workspace.registerLoadedPage(Math.floor(response.offset / PAGE_SIZE), response.offset, rows, response.annotation_revision);
              workspace.applyAnnotationCounts(response.annotated_total, response.annotated_filtered, response.annotation_revision);
              timing?.mark("published");
              options.summary();
              timing?.present(isCurrent);
              return true;
            }, () => request.priority);
            if (published !== false) return;
            if (attempt === 2) throw new Error("Annotations keep changing while rows load. Refresh the project to retry.");
          } catch (error) {
            if (attempt < 2 && isJobLimitError(error) && isCurrent()) {
              await new Promise((resolve) => setTimeout(resolve, 250 * (attempt + 1)));
            } else throw error;
          } finally {
            request.running = false;
          }
        }
      } catch (error) {
        if (isCurrent() && !request.cancelled) {
          const message = errorMessage(error, "Failed to load rows.");
          failedPages.set(page, message);
          publishError();
          workspace.appliedResultLoading.set(false);
          options.notify(message);
        }
      } finally {
        forgetPageRequest(request);
      }
    })();
    workspace.pendingPages.update((pending) => new Set([...pending, key]));
    return request.promise;
  };
  const requestPage = (pageIndex: number, force = false) => loadPage(pageIndex, force);

  const ensureRangeLoaded = (start: number, end: number) => {
    if (disposed || !options.isReady() || end <= start
      || (!get(workspace.queryCountKnown) && failedPages.size > 0)) return;
    const maximum = Math.floor(Math.max(get(workspace.totalFilteredRows) - 1, 0) / PAGE_SIZE);
    const first = Math.min(maximum, Math.max(0, Math.floor(start / PAGE_SIZE)));
    const last = Math.min(maximum, Math.floor(Math.max(start, end - 1) / PAGE_SIZE));
    if (previousStart !== null && start !== previousStart) direction = Math.sign(start - previousStart);
    previousStart = start;
    const visible = Array.from({ length: last - first + 1 }, (_, index) => first + index);
    const prefetch: number[] = [];
    for (let distance = 1; distance <= PREFETCH_PAGES; distance += 1) {
      const ahead = last + distance, behind = first - distance;
      for (const page of direction > 0 ? [ahead, behind] : [behind, ahead]) {
        if (page >= 0 && page <= maximum) prefetch.push(page);
      }
    }
    wantedPages = new Set([...visible, ...prefetch]);
    // A fast jump must not queue every page crossed before the new viewport.
    // Running reads still have useful values for this query and may fill cache.
    for (const request of pageRequests.values()) {
      if (!request.running && !request.explicit && !wantedPages.has(request.page)) {
        request.cancelled = true;
        forgetPageRequest(request);
      }
    }
    discardStaleWaiters();
    for (const page of [...visible, ...prefetch]) {
      const key = workspace.pendingPageKey(get(workspace.viewGeneration), pageEpoch, page);
      const priority = visible.includes(page) ? 1 : 2 + prefetch.indexOf(page);
      const pending = pageRequests.get(key);
      if (pending) {
        if (!pending.explicit) pending.priority = priority;
      } else if (!failedPages.has(page) && !get(workspace.largePages).has(page) && !get(workspace.loadedPages).has(page)) {
        void loadPage(page, false, false);
        const request = pageRequests.get(key);
        if (request) request.priority = priority;
      }
    }
    for (const page of visible) workspace.touchPage(page);
  };

  const applyFilters = (filters: AppliedFilters, resetScroll: boolean, force: boolean, restoredRow?: number): Promise<void> => {
    if (disposed || !get(workspace.projectDetail)) return Promise.resolve();
    const normalized = normalize(filters);
    if (!force && signatureFor(normalized) === signature) return Promise.resolve();
    const body = get(workspace.bodyScrollEl);
    const row = resetScroll ? 0 : restoredRow ?? logicalRowAt(
      body?.isConnected ? body.scrollTop : get(workspace.scrollTop),
      get(workspace.totalFilteredRows), get(workspace.viewportHeight), get(workspace.rowHeight));
    initialize(normalized);
    workspace.appliedResultLoading.set(true);
    options.scopeChanged(row);
    if (resetScroll) {
      workspace.scrollTop.set(0);
      workspace.scrollLeft.set(0);
      const body = get(workspace.bodyScrollEl), header = get(workspace.headerScrollEl);
      if (body) { body.scrollTop = 0; body.scrollLeft = 0; }
      if (header) header.scrollLeft = 0;
    }
    return requestPage(Math.floor(row / PAGE_SIZE), true);
  };

  const stopAnnotationRefresh = workspace.annotationRefreshRequested.subscribe((sequence) => {
    if (sequence > 0 && !disposed && options.isReady()) void requestPage(workspace.currentVisiblePage(), true);
  });
  const stopProjectionRefresh = workspace.visibleColumns.subscribe(columns => {
    const projectId = get(workspace.currentProjectId);
    if (disposed || !projectId || projectId !== projectionProject) return;
    // Display order does not change which cells a page contains.
    const next = JSON.stringify([...columns].sort());
    if (next === projectionKey) return;
    projectionKey = next;
    if (!options.isReady()) return;
    // Hiding columns, or revealing cells retained from an earlier projection,
    // does not change any delivered evidence. Future pages capture the new set.
    const cachedComplete = [...get(workspace.rowsCache).values()]
      .every(row => columns.every(column => Object.hasOwn(row.cells, column)));
    const pendingComplete = [...pageRequests.values()]
      .every(request => columns.every(column => request.pageColumns.includes(column)));
    if (cachedComplete && pendingComplete) return;
    const page = workspace.currentVisiblePage();
    // Membership, counts, selection and viewport still describe the same
    // query. Only cached cells and in-flight page projections are obsolete.
    retirePageReads();
    clearPages();
    void requestPage(page, true);
  });
  const dispose = () => {
    if (disposed) return;
    stopAnnotationRefresh();
    stopProjectionRefresh();
    disposed = true;
    revision += 1;
    reads.dispose();
    for (const waiter of waiters.splice(0)) waiter.resolve(false);
    pageRequests.clear();
    wantedPages.clear();
    failedPages.clear();
  };
  workspace.onDispose(dispose);
  return {
    get revision() { return revision; },
    get signature() { return signature; },
    signatureFor, initialize, resetCache, requestPage, ensureRangeLoaded, applyFilters,
    retryFailedPage: () => {
      const error = get(workspace.queryError);
      return error ? requestPage(error.page, true) : Promise.resolve();
    },
    cancel: () => reads.cancelAll(),
    cancelLookup: () => { reads.cancel("locate"); discardStaleWaiters(); },
    runLookup: <T>(isCurrent: () => boolean, run: (requestId: string) => Promise<T>) =>
      withSlot(isCurrent, () => reads.run("locate", options.backend, run)),
    dispose,
  };
}

export type WorkspaceQuery = ReturnType<typeof createWorkspaceQuery>;
