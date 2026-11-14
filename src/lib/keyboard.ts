/** WebKitGTK can report Shift+Tab as Unidentified while preserving code=Tab. */
export function isTabKey(event: Pick<KeyboardEvent, "key" | "code">): boolean {
  return event.key === "Tab" || (event.key === "Unidentified" && event.code === "Tab");
}

/** Keep IME confirmation keys out of editor shortcuts, including WebKit's
 * compositionend-before-keydown ordering and its legacy keyCode 229. */
export function createCompositionGuard() {
  let composing = false;
  let endedAt = -Infinity;
  return {
    start() { composing = true; },
    end(event: Pick<CompositionEvent, "timeStamp">) {
      composing = false;
      endedAt = event.timeStamp;
    },
    reset() { composing = false; endedAt = -Infinity; },
    blocksShortcut(event: Pick<KeyboardEvent, "isComposing" | "keyCode" | "timeStamp">) {
      const sinceEnd = event.timeStamp - endedAt;
      return composing || event.isComposing || event.keyCode === 229
        || (sinceEnd >= 0 && sinceEnd <= 50);
    },
  };
}
