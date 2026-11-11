<script lang="ts">
  import { canGoBack, canGoForward, goBack, goForward } from "../../stores/viewHistory";
</script>

<div class="history-buttons" role="group" aria-label="Condition history">
  <button type="button" disabled={!$canGoBack} on:click={goBack} aria-label="Back" title="Previous conditions (Alt+Left)">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m14 5-7 7 7 7" /></svg>
  </button>
  <button type="button" disabled={!$canGoForward} on:click={goForward} aria-label="Forward" title="Next conditions (Alt+Right)">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m10 5 7 7-7 7" /></svg>
  </button>
</div>

<style>
  .history-buttons { display: inline-flex; align-items: center; gap: 2px; }
  button { display: inline-flex; align-items: center; justify-content: center; height: 27px; width: 27px; padding: 0; background: transparent; color: var(--muted); border: 1px solid var(--border); border-radius: 4px; }
  button:hover:not(:disabled) { color: var(--fg); background: var(--surface-weak); }
  button:focus-visible { outline: 1px solid var(--accent-border); outline-offset: 1px; }
  button:disabled { opacity: .4; }
</style>
