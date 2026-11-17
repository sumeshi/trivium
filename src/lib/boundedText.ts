import { invoke } from "@tauri-apps/api/core";

/** Limits and regular-file checks are enforced on the opened native file. */
export function readImportText(path: string, kind: "theme" | "pattern"): Promise<string> {
  return invoke<string>("read_import_text", { path, kind });
}
