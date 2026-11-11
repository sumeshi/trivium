/** One error-normalization rule for Tauri's string rejections and JS errors. */
export function errorMessage(error: unknown, fallback = "Operation failed."): string {
  if (typeof error === "string" && error.trim().length > 0) {
    return error;
  }
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return fallback;
}
