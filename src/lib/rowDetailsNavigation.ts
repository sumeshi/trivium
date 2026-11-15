import type { LocateProjectRowResponse, QueryProjectRowsResponse } from "./backend";
import type { ProjectRow, QuerySpec } from "./types";
import type { DetailTarget } from "./rowDetails";
import { errorMessage } from "./errors.ts";

export interface DetailResultScope {
  /** Includes project, result generation, filters, sort and annotation version. */
  key: string;
  projectId: string;
  query: QuerySpec;
  total: number;
}

export interface DetailNavigationState {
  busy: boolean;
  position: number | null;
  total: number;
  error: string;
}

interface NavigationOptions {
  current: () => { scope: string; projectId: string; rowIndex: number } | null;
  position: (rowIndex: number) => number | null;
  row: (position: number) => ProjectRow | undefined;
  locate: (scope: DetailResultScope, rowIndex: number) => Promise<LocateProjectRowResponse>;
  read: (scope: DetailResultScope, position: number) => Promise<QueryProjectRowsResponse>;
  cancel: () => void;
  changed: (state: DetailNavigationState) => void;
  open: (row: ProjectRow) => void;
}

/** Result positions navigate; original row ids identify the displayed records. */
export function createRowDetailsNavigation(options: NavigationOptions) {
  let generation = 0;
  let disposed = false;
  let scope: DetailResultScope | null = null;
  let target: DetailTarget | null = null;
  let useCache = true;
  let state: DetailNavigationState = { busy: false, position: null, total: 0, error: "" };
  const publish = () => options.changed({ ...state });
  const isCurrent = (ticket: number) => {
    const current = options.current();
    return !disposed && ticket === generation && scope && target && current
      && current.scope === scope.key && current.projectId === target.projectId
      && current.rowIndex === target.rowIndex;
  };
  const clear = () => {
    generation += 1;
    options.cancel();
    scope = null;
    target = null;
    state = { busy: false, position: null, total: 0, error: "" };
    publish();
  };

  return {
    async load(next: DetailTarget, result: DetailResultScope) {
      if (disposed) return;
      if (target?.projectId === next.projectId && target.rowIndex === next.rowIndex && scope?.key === result.key) return;
      // The table may still retain the preceding result's map while refreshed
      // rows arrive. A changed scope must resolve against the backend instead.
      const sameCache = scope === null || (scope.key === result.key && useCache);
      clear();
      useCache = sameCache;
      target = { ...next };
      scope = result;
      const ticket = generation;
      const cached = useCache ? options.position(next.rowIndex) : null;
      state = { busy: cached === null, position: cached, total: result.total, error: "" };
      publish();
      if (cached !== null) return;
      try {
        const located = await options.locate(result, next.rowIndex);
        if (!isCurrent(ticket)) return;
        state.position = located.position;
        state.total = located.total_filtered_rows;
        if (located.position === null) state.error = "This row is not in the current results.";
      } catch (error) {
        if (!isCurrent(ticket)) return;
        state.error = errorMessage(error, "Failed to locate this row in the current results.");
      } finally {
        if (isCurrent(ticket)) { state.busy = false; publish(); }
      }
    },
    async move(direction: -1 | 1) {
      if (!scope || !target || state.busy || state.position === null || !isCurrent(generation)) return;
      const position = state.position + direction;
      if (position < 0 || position >= state.total) return;
      const ticket = generation;
      const activeScope = scope;
      state.busy = true;
      state.error = "";
      publish();
      try {
        let row = useCache ? options.row(position) : undefined;
        if (!row) {
          const response = await options.read(activeScope, position);
          if (!isCurrent(ticket)) return;
          state.total = response.total_filtered_rows;
          // A one-row read must describe the requested result position. Never
          // substitute its result position for its original source row id.
          if (response.offset !== position || response.rows.length !== 1) {
            throw new Error("The result changed. Reopen the row to continue.");
          }
          row = response.rows[0];
        }
        if (!isCurrent(ticket)) return;
        target = { ...target, rowIndex: row.row_index };
        state.position = position;
        state.busy = false;
        options.open(row);
        publish();
      } catch (error) {
        if (!isCurrent(ticket)) return;
        state.error = errorMessage(error, "Failed to load the adjacent row.");
      } finally {
        if (isCurrent(ticket)) { state.busy = false; publish(); }
      }
    },
    clear,
    dispose() {
      if (disposed) return;
      clear();
      disposed = true;
    },
  };
}
