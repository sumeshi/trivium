/** A recognizable, portable default filename for a saved investigation view. */
export function presetFileName(sourceName: string): string {
  const leaf = sourceName.split(/[\\/]/).pop() ?? "";
  const stem = leaf.replace(/\.[^.]+$/, "").replace(/[<>:"/\\|?*\u0000-\u001f]/g, "_").replace(/[. ]+$/, "").trim();
  return `trivium-preset-${stem || "investigation"}.json`;
}
