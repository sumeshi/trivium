<script lang="ts">
  import { createEventDispatcher, onMount, tick } from 'svelte';
  import type { ColumnKind } from '../../types';
  import { columnKindLabel } from '../../columnTypes';

  export let columnId: string;
  export let label: string;
  export let x: number;
  export let y: number;
  export let frozen = false;
  export let displayFormat: 'original' | 'iso' = 'original';
  export let columnKind: ColumnKind | undefined = undefined;

  const dispatch = createEventDispatcher<{
    close: { restoreFocus: boolean };
    search: { columnId: string };
    rule: { columnId: string };
    group: { columnId: string };
    hide: { columnId: string };
    freeze: { columnId: string };
    unfreeze: void;
    format: { columnId: string; format: 'original' | 'iso' };
  }>();
  let menu: HTMLDivElement;
  let left = x;
  let top = y;
  const close = (restoreFocus = false) => dispatch('close', { restoreFocus });
  const toggleDisplayFormat = () => {
    if (columnKind !== 'datetime') return;
    dispatch('format', { columnId, format: displayFormat === 'iso' ? 'original' : 'iso' });
  };
  const outside = (event: MouseEvent) => {
    if (!menu?.contains(event.target as Node)) close();
  };
  const scrolling = (event: Event) => {
    if (!(event.target instanceof Node) || !menu?.contains(event.target)) close();
  };
  const keydown = (event: KeyboardEvent) => {
    if (event.key === 'Escape') { event.preventDefault(); close(true); return; }
    const buttons = [...menu.querySelectorAll<HTMLButtonElement>('button:not(:disabled)')];
    const at = buttons.indexOf(document.activeElement as HTMLButtonElement);
    const next = event.key === 'ArrowDown' ? (at + 1) % buttons.length
      : event.key === 'ArrowUp' ? (at - 1 + buttons.length) % buttons.length
      : event.key === 'Home' ? 0 : event.key === 'End' ? buttons.length - 1 : null;
    if (next === null) return;
    event.preventDefault();
    buttons[next]?.focus();
  };
  onMount(async () => {
    await tick();
    if (!menu?.isConnected) return;
    const rect = menu.getBoundingClientRect();
    left = Math.max(8, Math.min(x, innerWidth - rect.width - 8));
    top = Math.max(8, Math.min(y, innerHeight - rect.height - 8));
    menu.querySelector<HTMLButtonElement>('button:not(:disabled)')?.focus();
  });
</script>

<svelte:window on:mousedown={outside} on:scroll|capture={scrolling} on:resize={() => close()} />

<div class="cell-context-menu column-context-menu" bind:this={menu}
  style={`left: ${left}px; top: ${top}px;`} role="menu" aria-label={`Column actions: ${label}`}
  data-column-kind={columnKind ?? 'unknown'}
  on:keydown={keydown} tabindex="-1">
  <span class="column-menu-label" title={label}>
    <span class="column-menu-name">{label}</span>
    <span class="column-menu-type" data-column-kind={columnKind ?? 'unknown'}
      title={`Inferred type: ${columnKindLabel(columnKind)}`}>{columnKindLabel(columnKind)}</span>
  </span>
  <button type="button" role="menuitem" on:click={() => dispatch('search', { columnId })}>
    Search this column
  </button>
  <button type="button" role="menuitem" on:click={() => dispatch('rule', { columnId })}>
    Add rule for this column…
  </button>
  <button type="button" role="menuitem" on:click={() => dispatch('group', { columnId })}>
    Group by this column
  </button>
  <div class="column-menu-separator" role="separator"></div>
  <button type="button" role="menuitem" on:click={() => dispatch('hide', { columnId })}>
    Hide this column
  </button>
  <button type="button" role="menuitem"
    on:click={() => frozen ? dispatch('unfreeze') : dispatch('freeze', { columnId })}>
    {frozen ? 'Unfreeze data columns' : 'Freeze columns through this column'}
  </button>
  {#if columnKind === 'datetime'}
    <div class="column-menu-separator" role="separator"></div>
    <button type="button" role="menuitem"
      title="Changes display only; source values stay unchanged"
      on:click={toggleDisplayFormat}>
      {displayFormat === 'iso' ? 'Show original values' : 'Show ISO datetime'}
    </button>
  {/if}
</div>

<style>
  .column-context-menu { min-width: 190px; max-width: min(300px, calc(100vw - 16px)); max-height: calc(100vh - 16px); overflow-y: auto; overscroll-behavior: contain; }
  .column-menu-separator { border-top: 1px solid var(--border); margin: 3px 0; }
  .column-menu-label { display: flex; align-items: baseline; gap: 7px; padding: 4px 7px 5px; border-bottom: 1px solid var(--border); color: var(--muted); font-size: 10.5px; white-space: nowrap; }
  .column-menu-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
  .column-menu-type { flex-shrink: 0; font-weight: 400; }
</style>
