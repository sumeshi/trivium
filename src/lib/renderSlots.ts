/** DOM slots are reused; logical row/column identities remain in their values. */
export interface RenderSlot<T> {
  slot: number;
  value: T;
}

/**
 * Retained items keep their DOM keys. Entering items reuse departing slots,
 * avoiding node construction during scrolling. Callers supply unique keys.
 * No values or keys outside the current window remain retained.
 */
export function recycleSlots<T, K>(
  values: readonly T[], keyOf: (value: T) => K, previous: readonly RenderSlot<T>[] = [],
): readonly RenderSlot<T>[] {
  const old = new Map(previous.map((entry) => [keyOf(entry.value), entry]));
  const keys = values.map(keyOf);
  const occupied = new Set<number>();
  for (const key of keys) {
    const retained = old.get(key);
    if (retained) occupied.add(retained.slot);
  }
  let available = 0;
  const next = values.map((value, index) => {
    const retained = old.get(keys[index]);
    if (retained) return retained.value === value ? retained : { slot: retained.slot, value };
    while (occupied.has(available)) available += 1;
    const slot = available++;
    occupied.add(slot);
    return { slot, value };
  });
  return next.length === previous.length && next.every((entry, index) => entry === previous[index])
    ? previous : next;
}
