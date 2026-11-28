<script lang="ts">
  import { createEventDispatcher, onDestroy } from 'svelte';
  import type { ExtractPending } from '../../extractTypes';

  export let pending: ExtractPending | null = null;
  const dispatch = createEventDispatcher<{ cancel: void }>();
  let requestId: string | null = null;
  let visible = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const reveal = (nextId: string | null) => {
    if (nextId === requestId) return;
    requestId = nextId;
    visible = false;
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
    if (nextId !== null) timer = setTimeout(() => {
      if (pending?.requestId !== nextId || requestId !== nextId) return;
      timer = undefined;
      visible = true;
    }, 150);
  };
  const cancel = () => dispatch('cancel');
  $: reveal(pending?.requestId ?? null);
  onDestroy(() => { if (timer !== undefined) clearTimeout(timer); });
</script>

{#if pending && visible}
  <div class="extraction-progress" role="status" aria-label="Extraction in progress" aria-live="polite">
    <div class="progress-heading">
      <strong>Extract</strong>
      <span class="progress-source" title={pending.sourceName}>{pending.sourceName}</span>
      <button type="button" aria-label="Cancel extraction" title="Cancel extraction" on:click={cancel}>Cancel</button>
    </div>
    <div class="progress-detail">
      <span>{pending.message}</span>
      {#if pending.total !== null || pending.done > 0}
        <span class="progress-count">{pending.total === null ? pending.done.toLocaleString() : `${pending.done.toLocaleString()} / ${pending.total.toLocaleString()}`}</span>
      {/if}
    </div>
    {#if pending.total !== null && pending.total > 0}
      <progress value={pending.done} max={pending.total} aria-label="Extraction progress"></progress>
    {:else}
      <progress aria-label="Extraction progress"></progress>
    {/if}
  </div>
{/if}

<style>
  .extraction-progress { display: flex; flex-direction: column; gap: 6px; width: min(320px, calc(100vw - 40px)); padding: 9px 10px; border: 1px solid var(--border); border-radius: var(--radius); background: var(--surface); color: var(--fg); box-shadow: var(--shadow-strong); font-size: 11px; pointer-events: auto; }
  .progress-heading, .progress-detail { display: flex; align-items: center; gap: 7px; min-width: 0; }
  .progress-heading strong { font-weight: 600; }
  .progress-source { flex: 1; min-width: 0; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; color: var(--muted); }
  .progress-heading button { padding: 3px 6px; border: 1px solid var(--border); border-radius: 3px; background: var(--surface-weak); color: var(--fg); font: inherit; cursor: pointer; }
  .progress-heading button:hover { border-color: var(--accent-border); }
  .progress-heading button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
  .progress-detail { justify-content: space-between; color: var(--muted); }
  .progress-count { flex-shrink: 0; font-variant-numeric: tabular-nums; }
  progress { width: 100%; height: 5px; accent-color: var(--accent); }
</style>
