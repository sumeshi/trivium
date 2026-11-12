import { get, writable } from "svelte/store";
import type { CachedRow } from "./components/project_view/state";
import { PAGE_SIZE } from "./components/project_view/state.ts";
import { estimatePageBytes, selectEvictions } from "./pageCache.ts";

const PAGE_CACHE_MAX_PAGES = 5;
const PAGE_CACHE_MAX_BYTES = 16 * 1024 * 1024;

/** Bounded row pages and result positions are always evicted together. */
export function createWorkspacePages(currentVisiblePage: () => number) {
  const rowsCache = writable<Map<number, CachedRow>>(new Map()); // Keyed by row_index
  const pendingPages = writable<Set<string>>(new Set());
  const loadedPages = writable<Set<number>>(new Set());
  /**
   * Pages that were too large to retain under the byte bound. They are evicted
   * immediately after loading and not retried automatically; the table shows an
   * explicit notice instead of "Loading…". Reset when the result set changes.
   */
  const largePages = writable<Set<number>>(new Set());

  // Maps display position (offset + i) to backend row_index to preserve backend ordering
  const positionToRowIndex = writable<Map<number, number>>(new Map());

  function updateCachedRow(
    rowIndex: number,
    updater: (row: CachedRow) => CachedRow
  ): CachedRow | null {
    const existing = get(rowsCache).get(rowIndex);
    if (!existing) return null;
    const updated = updater(existing);
    rowsCache.update((cache) => {
      const nextCache = new Map(cache);
      nextCache.set(rowIndex, updated);
      return nextCache;
    });
    return updated;
  }

  /**
   * Per-page access order and retained-byte estimate. Kept outside the Svelte
   * stores because only eviction needs it; the retained rows themselves live in
   * `rowsCache`/`positionToRowIndex`/`loadedPages`.
   */
  let pageCacheMeta = new Map<number, { at: number; bytes: number }>();
  let pageAccessClock = 0;

  /** Marks an already cached page as recently used without reloading it. */
  function touchPage(page: number): void {
    const entry = pageCacheMeta.get(page);
    if (!entry) return;
    pageAccessClock += 1;
    pageCacheMeta.set(page, { at: pageAccessClock, bytes: entry.bytes });
  }

  /**
   * Removes one page from every place it is cached: rows, display positions and
   * the loaded-page set. Annotation editor snapshots and the row-id selection
   * store are intentionally not touched.
   */
  function dropPage(page: number): void {
    const start = page * PAGE_SIZE;
    const end = start + PAGE_SIZE;
    const positions = get(positionToRowIndex);
    const nextPositions = new Map(positions);
    const rowIndexes = new Set<number>();
    for (const [position, rowIndex] of positions) {
      if (position >= start && position < end) {
        rowIndexes.add(rowIndex);
        nextPositions.delete(position);
      }
    }
    if (nextPositions.size !== positions.size) {
      positionToRowIndex.set(nextPositions);
    }
    if (rowIndexes.size > 0) {
      rowsCache.update((cache) => {
        const next = new Map(cache);
        for (const rowIndex of rowIndexes) {
          next.delete(rowIndex);
        }
        return next;
      });
    }
    loadedPages.update((pages) => {
      if (!pages.has(page)) return pages;
      const next = new Set(pages);
      next.delete(page);
      return next;
    });
    pageCacheMeta.delete(page);
  }

  /** Evicts pages past the page-count or byte bound, protecting the current page. */
  function evictPages(): void {
    const evicted = selectEvictions(
      pageCacheMeta,
      currentVisiblePage(),
      PAGE_CACHE_MAX_PAGES,
      PAGE_CACHE_MAX_BYTES
    );
    if (evicted.length === 0) return;

    const oversized = new Set<number>();
    for (const page of evicted) {
      const entry = pageCacheMeta.get(page);
      if (entry && entry.bytes > PAGE_CACHE_MAX_BYTES) {
        oversized.add(page);
      }
      dropPage(page);
    }
    if (oversized.size > 0) {
      largePages.update((pages) => {
        const next = new Set(pages);
        for (const page of oversized) {
          next.add(page);
        }
        return next;
      });
    }
  }

  /**
   * Stores a freshly loaded page and enforces the cache bounds. A page that does
   * not fit the byte bound on its own is dropped immediately and recorded in
   * `largePages`.
   */
  function registerLoadedPage(
    page: number,
    offset: number,
    rows: CachedRow[]
  ): void {
    if (rows.length > 0) {
      rowsCache.update((cache) => {
        const next = new Map(cache);
        for (const row of rows) {
          next.set(row.row_index, row);
        }
        return next;
      });
      positionToRowIndex.update((positions) => {
        const next = new Map(positions);
        rows.forEach((row, index) => {
          next.set(offset + index, row.row_index);
        });
        return next;
      });
    }
    loadedPages.update((pages) => {
      const next = new Set(pages);
      next.add(page);
      return next;
    });

    pageAccessClock += 1;
    pageCacheMeta.set(page, {
      at: pageAccessClock,
      bytes: estimatePageBytes(rows),
    });
    largePages.update((pages) => {
      if (!pages.has(page)) return pages;
      const next = new Set(pages);
      next.delete(page);
      return next;
    });

    evictPages();
  }

  /** Clears page-cache bookkeeping when the result set or project changes. */
  function resetPageCache(): void {
    pageCacheMeta = new Map();
    pageAccessClock = 0;
    largePages.set(new Set());
  }

  return {
    rowsCache, positionToRowIndex, pendingPages, loadedPages, largePages, updateCachedRow,
    currentVisiblePage, touchPage, registerLoadedPage, resetPageCache,
  };
}
