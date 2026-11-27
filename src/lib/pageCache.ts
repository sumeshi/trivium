import type { CachedRow } from "./components/project_view/state";

/**
 * Approximate per-row bookkeeping cost (row object, maps, annotation fields).
 * The limit is about retained text, so this is deliberately a rough
 * constant rather than a measurement.
 */
const ROW_OVERHEAD_BYTES = 512;

export type PageCacheEntry = {
  /** Monotonic access counter; a larger value means a more recent access. */
  at: number;
  /** Approximate retained text bytes for this page. */
  bytes: number;
};

/**
 * Annotation text is independent of the source-cell caches and can grow after
 * a page is loaded. Kept separate so normal flag updates never scan the cells.
 */
export function estimateAnnotationBytes(row: Pick<CachedRow, "memo" | "tags">): number {
  let bytes = row.memo.length;
  for (const tag of row.tags) bytes += tag.length;
  return bytes;
}

/**
 * Approximate the bytes retained for one page. Normalization conservatively
 * counts source/display text and exact datetime keys once. Only note/tag sizes
 * can change without new source cells.
 * Character count remains the proxy for bytes used by the eviction bound.
 */
export function estimatePageBytes(rows: readonly CachedRow[]): number {
  let bytes = 0;
  for (const row of rows) {
    bytes += ROW_OVERHEAD_BYTES + row.cellBytes + estimateAnnotationBytes(row);
  }
  return bytes;
}

/**
 * Chooses pages to evict.
 *
 * - A page whose own bytes exceed `maxBytes` is always evicted (it cannot be
 *   retained under the bound), even when it is the current page. Callers can
 *   detect this by comparing the entry's `bytes` before dropping it.
 * - Otherwise the current page is protected and the least recently accessed
 *   pages are evicted until both the page-count and byte bounds hold.
 * - The result is sorted by page number so eviction is deterministic.
 */
export function selectEvictions(
  pages: ReadonlyMap<number, PageCacheEntry>,
  current: number,
  maxPages: number,
  maxBytes: number
): number[] {
  const evicted = new Set<number>();
  let remainingPages = pages.size;
  let remainingBytes = 0;
  for (const entry of pages.values()) {
    remainingBytes += entry.bytes;
  }

  for (const [page, entry] of pages) {
    if (entry.bytes > maxBytes) {
      evicted.add(page);
      remainingPages -= 1;
      remainingBytes -= entry.bytes;
    }
  }

  const candidates = [...pages.entries()]
    .filter(([page]) => !evicted.has(page) && page !== current)
    .sort(([leftPage, left], [rightPage, right]) => {
      if (left.at !== right.at) return left.at - right.at;
      return leftPage - rightPage;
    });

  for (const [page, entry] of candidates) {
    if (remainingPages <= maxPages && remainingBytes <= maxBytes) {
      break;
    }
    evicted.add(page);
    remainingPages -= 1;
    remainingBytes -= entry.bytes;
  }

  return [...evicted].sort((left, right) => left - right);
}
