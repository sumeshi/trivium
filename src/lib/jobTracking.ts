/** Normalizes an IPC/JS error to the text the toast and retry logic inspect. */
export function jobErrorMessage(error: unknown): string {
  if (typeof error === "string") return error;
  if (error instanceof Error) return error.message;
  return String(error);
}

/**
 * True when the engine rejected a job because its concurrency slots are full.
 * Page requests retry this instead of leaving the page unloaded.
 */
export function isJobLimitError(error: unknown): boolean {
  return jobErrorMessage(error).includes(
    "Too many long-running operations"
  );
}
