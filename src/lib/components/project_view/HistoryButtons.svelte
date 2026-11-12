<script lang="ts">
  import { useWorkspace } from "../../workspaceContext";
  const { canGoBack, canGoForward, goBack, goForward } = useWorkspace();

</script>

<div class="history-buttons" role="group" aria-label="Rule history">
  <span class="history-label" title="Undo or redo structured rules; this does not change search text or visible columns">Rules history</span>
  <button type="button" disabled={!$canGoBack} on:click={goBack} aria-label="Back" title="Previous rules (Alt+Left)">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m14 5-7 7 7 7" /></svg>
  </button>
  <button type="button" disabled={!$canGoForward} on:click={goForward} aria-label="Forward" title="Next rules (Alt+Right)">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m10 5 7 7-7 7" /></svg>
  </button>
</div>

<style>
  .history-buttons { display: inline-flex; align-items: center; gap: 3px; padding-right: 9px; border-right: 1px solid var(--border); }
  .history-label { color: var(--muted); font-size: 10.5px; margin-right: 3px; white-space: nowrap; }
  button { display: inline-flex; align-items: center; justify-content: center; height: 27px; width: 27px; padding: 0; background: transparent; color: var(--muted); border: 1px solid var(--border); border-radius: 4px; }
  button:hover:not(:disabled) { color: var(--fg); background: var(--surface-weak); }
  button:focus-visible { outline: 1px solid var(--accent-border); outline-offset: 1px; }
  button:disabled { opacity: .4; }
</style>
