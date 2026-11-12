import type { GetCellResult } from "./backend";
import type { Cell } from "./types";
import { errorMessage } from "./errors.ts";

export type DetailTarget = { projectId: string; rowIndex: number; columnId: string };
export type DetailField =
  | { state: "loading" }
  | { state: "loaded"; value: GetCellResult }
  | { state: "error"; error: string };

export function detailFilterValue(value: GetCellResult | undefined): string | null {
  if (!value || value.truncated || value.kind === "null" || value.kind === "missing") return null;
  return value.text;
}

/** Missing has no clipboard value; explicit null copies its source spelling. */
export function detailCopyValue(value: GetCellResult | undefined): string | null {
  if (!value || value.truncated || value.kind === "missing") return null;
  return value.kind === "null" ? "null" : value.text;
}

interface DetailOptions {
  read: (target: DetailTarget, column: string) => Promise<GetCellResult>;
  cancel: () => void;
  changed: (fields: ReadonlyMap<string, DetailField>) => void;
}

/** One open row owns full values; only previews requiring expansion need IO. */
export function createRowDetails(options: DetailOptions) {
  let generation = 0;
  let disposed = false;
  let target: DetailTarget | null = null;
  let fields = new Map<string, DetailField>();
  let pending: string[] = [];
  let running = 0;
  const publish = () => options.changed(new Map(fields));

  const pump = (ticket: number) => {
    while (!disposed && target && ticket === generation && running < 2 && pending.length > 0) {
      const column = pending.shift()!;
      const activeTarget = target;
      running += 1;
      void (async () => {
        try {
          const value = await options.read(activeTarget, column);
          if (disposed || ticket !== generation) return;
          if (value.truncated) throw new Error("The full field value was not returned.");
          fields.set(column, { state: "loaded", value });
        } catch (error) {
          if (disposed || ticket !== generation) return;
          fields.set(column, { state: "error", error: errorMessage(error) });
        } finally {
          if (!disposed && ticket === generation) {
            running -= 1;
            publish();
            pump(ticket);
          }
        }
      })();
    }
  };

  const clear = () => {
    generation += 1;
    options.cancel();
    target = null;
    pending = [];
    running = 0;
    fields = new Map();
    publish();
  };

  return {
    load(next: DetailTarget, columns: readonly string[], cells: Record<string, Cell>) {
      if (disposed) return;
      clear();
      target = { ...next };
      // Fetch the clicked field first. Other immutable DTOs with an explicit
      // untruncated marker already contain exact text, including null/missing.
      const ordered = [...new Set([next.columnId, ...columns])];
      for (const column of ordered) {
        const cell = cells[column];
        if (column !== next.columnId && cell?.truncated === false) {
          fields.set(column, { state: "loaded", value: { ...cell, truncated: false, length: cell.length ?? null } });
        } else {
          fields.set(column, { state: "loading" });
          pending.push(column);
        }
      }
      publish();
      pump(generation);
    },
    retry(column: string) {
      if (disposed || !target || fields.get(column)?.state !== "error") return;
      fields.set(column, { state: "loading" });
      pending.push(column);
      publish();
      pump(generation);
    },
    clear,
    dispose() {
      if (disposed) return;
      clear();
      disposed = true;
    },
  };
}
