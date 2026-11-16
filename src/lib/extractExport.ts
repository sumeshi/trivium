import { errorMessage } from './errors.ts';
import type { ExtractBackend, ExtractExport } from './extractTypes';

/** The chosen path is frozen across the backend's overwrite challenge. */
export async function exportExtractWithConfirmation(
  backend: ExtractBackend,
  input: { resultId: string; destination: string },
  confirmOverwrite: (destination: string) => boolean | Promise<boolean>,
): Promise<ExtractExport | null> {
  const snapshot = structuredClone(input);
  try {
    return await backend.exportExtract({ ...snapshot, allowOverwrite: false, requestId: crypto.randomUUID() });
  } catch (error) {
    if (!errorMessage(error).startsWith('Overwrite confirmation required:')) throw error;
    if (!await confirmOverwrite(snapshot.destination)) return null;
    return backend.exportExtract({ ...snapshot, allowOverwrite: true, requestId: crypto.randomUUID() });
  }
}
