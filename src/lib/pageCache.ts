import type { CachedRow } from "./components/project_view/state";

/**
 * Approximate per-row bookkeeping cost (row object, maps, annotations).
 * The limit is about retained cell text, so this is deliberately a rough
 * constant rather than a measurement.
 */
const ROW_OVERHEAD_BYTES = 512;

export type PageCacheEntry = {
  /** Monotonic access counter; a larger value means a more recent access. */
  at: number;
  /** Approximate retained cell bytes for this page. */
  bytes: number;
};

/**
 * Approximate the bytes retained for one page. Only the cell strings the table
 * renders (`displayCache` and `previewCache`) are counted; raw `data` and
 * `cells` values are not added on top. Character count is used as a proxy for
 * bytes, which is close enough for an eviction bound.
 */
export function estimatePageBytes(rows: readonly CachedRow[]): number {
  let bytes = 0;
  for (const row of rows) {
    bytes += ROW_OVERHEAD_BYTES;
    for (const value of Object.values(row.displayCache)) {
      bytes += value.length;
    }
    for (const value of Object.values(row.previewCache)) {
      bytes += value.length;
    }
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
