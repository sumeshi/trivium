import { tick } from "svelte";

export type PageStage = "dispatched" | "responded" | "normalizeStarted" | "normalized" | "published";
export interface PageTiming {
  offset: number;
  limit: number;
  rows?: number;
  columns?: number;
  requested: number;
  dispatched?: number;
  responded?: number;
  normalizeStarted?: number;
  normalized?: number;
  published?: number;
  flushed?: number;
  presented?: number;
}

declare global {
  interface Window {
    /** Explicitly enabled by the performance harness; never stores cell values. */
    __TRIVIUM_PERF__?: { pages: PageTiming[] };
  }
}

/** No timers, marks or allocations in the ordinary, uninstrumented query path. */
export function beginPageTiming(offset: number, limit: number) {
  const sink = typeof window === "undefined" ? undefined : window.__TRIVIUM_PERF__;
  if (!sink || !Array.isArray(sink.pages)) return undefined;
  const sample: PageTiming = { offset, limit, requested: performance.now() };
  sink.pages.push(sample);
  if (sink.pages.length > 128) sink.pages.splice(0, sink.pages.length - 128);
  return {
    mark(stage: PageStage) { sample[stage] = performance.now(); },
    rows(count: number, columns: number) { sample.rows = count; sample.columns = columns; },
    present(isCurrent: () => boolean) {
      void tick().then(() => {
        if (!isCurrent()) return;
        sample.flushed = performance.now();
        requestAnimationFrame(() => requestAnimationFrame(() => {
          // Two frames include a paint opportunity, rather than calling a
          // promise resolving before rendering "visible".
          if (isCurrent()) sample.presented = performance.now();
        }));
      });
    },
  };
}
