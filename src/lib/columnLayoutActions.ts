/** Restore a saved order and append any new source columns once. */
export function displayColumnOrder(source: readonly string[], order: readonly string[]): string[] {
  const known = new Set(source), seen = new Set<string>(), result: string[] = [];
  for (const id of [...order, ...source]) {
    if (!known.has(id) || seen.has(id)) continue;
    seen.add(id);
    result.push(id);
  }
  return result;
}

/** Move display columns by their evidence IDs, keeping hidden columns in order. */
export function moveDisplayColumn(
  order: readonly string[], column: string, target: string, after = false,
): readonly string[] {
  if (column === target || !order.includes(column) || !order.includes(target)) return order;
  const next = order.filter((id) => id !== column);
  next.splice(next.indexOf(target) + (after ? 1 : 0), 0, column);
  return next.every((id, index) => id === order[index]) ? order : next;
}
