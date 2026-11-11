import type { FlagFilterSelection, FlagFilterValue } from "./types";

/** Stable wire order; `none` is the legacy token for an unflagged row. */
export const FLAG_FILTER_VALUES: readonly FlagFilterSelection[] = [
  "safe", "suspicious", "critical", "none",
];

/** Null means an unknown saved value; [] means no restriction. */
export function parseFlagFilter(value: unknown): FlagFilterSelection[] | null {
  if (typeof value !== "string") return null;
  const text = value.trim();
  if (text === "all" || text === "") return [];
  if (text === "priority") return ["suspicious", "critical"];
  const tokens = text.split("|").map((token) => token.trim());
  if (tokens.some((token) => !FLAG_FILTER_VALUES.includes(token as FlagFilterSelection))) {
    return null;
  }
  return FLAG_FILTER_VALUES.filter((value) => tokens.includes(value));
}

export function isFlagFilterValue(value: unknown): value is FlagFilterValue {
  return parseFlagFilter(value) !== null;
}

/** Keep one existing string field throughout paging, analysis, export and bulk. */
export function encodeFlagFilter(values: Iterable<FlagFilterSelection>): FlagFilterValue {
  const selected = new Set(values);
  const ordered = FLAG_FILTER_VALUES.filter((value) => selected.has(value));
  return ordered.length === 0 ? "all" : ordered.join("|") as FlagFilterValue;
}

export function toggleFlagFilter(value: FlagFilterValue, flag: FlagFilterSelection): FlagFilterValue {
  const selected = new Set(parseFlagFilter(value) ?? []);
  if (selected.has(flag)) selected.delete(flag);
  else selected.add(flag);
  return encodeFlagFilter(selected);
}
