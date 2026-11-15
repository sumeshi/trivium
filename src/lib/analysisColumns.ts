/** Column kinds are import hints; a user may interpret any source column as time. */
export function datetimeAnalysisColumns(columns: readonly string[], kinds: Record<string, string> = {}) {
  return {
    inferred: columns.filter((column) => kinds[column] === "datetime"),
    other: columns.filter((column) => kinds[column] !== "datetime"),
  };
}

export function initialDatetimeColumn(columns: readonly string[], kinds: Record<string, string> = {}): string {
  return columns.find((column) => kinds[column] === "datetime") ?? "";
}
