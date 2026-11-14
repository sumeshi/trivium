/** WebKitGTK can report Shift+Tab as Unidentified while preserving code=Tab. */
export function isTabKey(event: Pick<KeyboardEvent, "key" | "code">): boolean {
  return event.key === "Tab" || (event.key === "Unidentified" && event.code === "Tab");
}
