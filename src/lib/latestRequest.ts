/**
 * Tracks a monotonically increasing request ticket and whether it is still
 * current. Components use one instance per independently loading panel.
 */
export type LatestRequest = {
  next(): number;
  isCurrent(ticket: number): boolean;
  invalidate(): void;
  dispose(): void;
};

export function createLatestRequest(): LatestRequest {
  let ticket = 0;
  let alive = true;
  return {
    next() {
      ticket += 1;
      return ticket;
    },
    isCurrent(candidate: number) {
      return alive && candidate === ticket;
    },
    invalidate() {
      ticket += 1;
    },
    dispose() {
      alive = false;
      ticket += 1;
    },
  };
}
