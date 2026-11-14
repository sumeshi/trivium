import { get } from "svelte/store";
import type { Backend, QueryProjectRowsResponse } from "./backend";
import type { AppliedFilters } from "./components/project_view/state";
import { PAGE_SIZE, PREFETCH_PAGES, normalizeRow } from "./components/project_view/state.ts";
import { errorMessage } from "./errors.ts";
import { isJobLimitError } from "./jobTracking.ts";
import { logicalRowAt } from "./virtualScroll.ts";
import { effectiveSearchColumns, normalizeSearchText } from "./searchScope.ts";
import { beginPageTiming } from "./renderPerformance.ts";
import type { WorkspaceController } from "./workspaceController";

interface QueryOptions {
  backend: Backend;
  isReady: () => boolean;
  restoreViewport: (result: QueryProjectRowsResponse, isCurrent: () => boolean) => Promise<void>;
  scopeChanged: () => void;
  summary: () => void;
  notify: (message: string) => void;
}

type PageRequest = {
  key: string;
  page: number;
  explicit: boolean;
  running: boolean;
  cancelled: boolean;
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
  let revision = 0;
  let signature = "";
  let slots = 2;
  const waiters: SlotWaiter[] = [];
  const pageRequests = new Map<string, PageRequest>();
  let wantedPages = new Set<number>();
  let previousStart: number | null = null;
  let direction = 1;

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
    for (const waiter of waiters.splice(0)) waiter.resolve(false);
    pageRequests.clear();
    wantedPages.clear();
    previousStart = null;
    direction = 1;
    revision += 1;
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
    const requestRevision = revision;
    const key = workspace.pendingPageKey(generation, requestRevision, page);
    const isCurrent = () => !disposed && revision === requestRevision &&
      generation === get(workspace.viewGeneration) && projectId === get(workspace.currentProjectId);
    const existing = pageRequests.get(key);
    if (existing) {
      existing.explicit ||= explicit;
      existing.priority = existing.explicit ? 0 : Math.min(existing.priority, 1);
      return existing.promise;
    }
    if (!force && get(workspace.loadedPages).has(page)) return;
    const request: PageRequest = { key, page, explicit, running: false, cancelled: false,
      priority: explicit ? 0 : 1, promise: Promise.resolve() };
    const timing = beginPageTiming(page * PAGE_SIZE, PAGE_SIZE);
    pageRequests.set(key, request);
    const filters = get(workspace.appliedFilters);
    const payload = {
      projectId, search: filters.search, searchColumns: [...filters.searchColumns],
      flagFilter: filters.flag, bookmarkedOnly: filters.bookmarkedOnly,
      condition: structuredClone(filters.condition), offset: page * PAGE_SIZE, limit: PAGE_SIZE,
      sortKey: get(workspace.sortKey), sortDirection: get(workspace.sortDirection),
    };
    request.promise = (async () => {
      try {
        await withSlot(() => isCurrent() && !request.cancelled &&
          (request.explicit || wantedPages.has(page)), async () => {
          request.running = true;
          for (let attempt = 0; attempt < 3; attempt += 1) {
            try {
              timing?.mark("dispatched");
              const response = await reads.run(key, options.backend, (requestId) =>
                options.backend.queryProjectRows({ ...payload, ...{ requestId } }));
              timing?.mark("responded");
              if (!isCurrent()) return;
              workspace.totalRows.set(response.total_rows);
              workspace.totalFilteredRows.set(response.total_filtered_rows);
              await options.restoreViewport(response, isCurrent);
              if (!isCurrent()) return;
              workspace.flaggedCount.set(response.annotated_total);
              workspace.filteredFlaggedCount.set(response.annotated_filtered);
              workspace.appliedResultLoading.set(false);
              timing?.rows(response.rows.length, Object.keys(response.rows[0]?.cells ?? {}).length);
              timing?.mark("normalizeStarted");
              const rows = response.rows.map(normalizeRow);
              timing?.mark("normalized");
              workspace.registerLoadedPage(Math.floor(response.offset / PAGE_SIZE), response.offset, rows);
              timing?.mark("published");
              options.summary();
              timing?.present(isCurrent);
              return;
            } catch (error) {
              if (attempt < 2 && isJobLimitError(error) && isCurrent()) {
                if (!request.explicit && !wantedPages.has(page)) return;
                await new Promise((resolve) => setTimeout(resolve, 250 * (attempt + 1)));
                if (!isCurrent()) return;
              } else throw error;
            }
          }
        }, () => request.priority);
      } catch (error) {
        if (isCurrent()) {
          if (get(workspace.appliedResultLoading)) workspace.totalFilteredRows.set(0);
          workspace.appliedResultLoading.set(false);
          options.notify(errorMessage(error, "Failed to load rows."));
        }
      } finally {
        if (page === 0 && isCurrent() && !request.cancelled) workspace.appliedResultLoading.set(false);
        forgetPageRequest(request);
      }
    })();
    workspace.pendingPages.update((pending) => new Set([...pending, key]));
    return request.promise;
  };
  const requestPage = (pageIndex: number, force = false) => loadPage(pageIndex, force);

  const ensureRangeLoaded = (start: number, end: number) => {
    if (disposed || !options.isReady() || end <= start) return;
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
      const key = workspace.pendingPageKey(get(workspace.viewGeneration), revision, page);
      const priority = visible.includes(page) ? 1 : 2 + prefetch.indexOf(page);
      const pending = pageRequests.get(key);
      if (pending) {
        if (!pending.explicit) pending.priority = priority;
      } else if (!get(workspace.largePages).has(page) && !get(workspace.loadedPages).has(page)) {
        void loadPage(page, false, false);
        const request = pageRequests.get(key);
        if (request) request.priority = priority;
      }
    }
    for (const page of visible) workspace.touchPage(page);
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
    for (const waiter of waiters.splice(0)) waiter.resolve(false);
    pageRequests.clear();
    wantedPages.clear();
  };
  workspace.onDispose(dispose);
  return {
    get revision() { return revision; },
    get signature() { return signature; },
    signatureFor, initialize, resetCache, requestPage, ensureRangeLoaded, applyFilters,
    cancel: () => reads.cancelAll(),
    cancelLookup: () => { reads.cancel("locate"); discardStaleWaiters(); },
    runLookup: <T>(isCurrent: () => boolean, run: (requestId: string) => Promise<T>) =>
      withSlot(isCurrent, () => reads.run("locate", options.backend, run)),
    dispose,
  };
}

export type WorkspaceQuery = ReturnType<typeof createWorkspaceQuery>;
