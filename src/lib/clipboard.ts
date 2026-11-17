/** Copy exact field text, falling back for WebViews that deny Clipboard API access. */
export async function writeClipboard(text: string): Promise<void> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
  } catch { /* Some WebViews expose the API without granting its permission. */ }

  const textarea = document.createElement('textarea');
  const focused = document.activeElement;
  textarea.value = text;
  textarea.readOnly = true;
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  document.body.append(textarea);
  try {
    textarea.select();
    if (!document.execCommand('copy')) throw new Error('Clipboard access was denied.');
  } finally {
    textarea.remove();
    if (focused instanceof HTMLElement && focused.isConnected) focused.focus({ preventScroll: true });
  }
}
