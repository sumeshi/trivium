<script lang="ts">
  import { createEventDispatcher, onMount, tick } from 'svelte';

  export let columnId: string;
  export let label: string;
  export let x: number;
  export let y: number;

  const dispatch = createEventDispatcher<{
    close: { restoreFocus: boolean };
    search: { columnId: string };
    rule: { columnId: string };
  }>();
  let menu: HTMLDivElement;
  let left = x;
  let top = y;
  const close = (restoreFocus = false) => dispatch('close', { restoreFocus });
  const outside = (event: MouseEvent) => {
    if (!menu?.contains(event.target as Node)) close();
  };
  const keydown = (event: KeyboardEvent) => {
    if (event.key === 'Escape') { event.preventDefault(); close(true); return; }
    const buttons = [...menu.querySelectorAll<HTMLButtonElement>('button')];
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
    menu.querySelector<HTMLButtonElement>('button')?.focus();
  });
</script>

<svelte:window on:mousedown={outside} on:scroll|capture={() => close()} on:resize={() => close()} />

<div class="cell-context-menu column-context-menu" bind:this={menu}
  style={`left: ${left}px; top: ${top}px;`} role="menu" aria-label={`Column actions: ${label}`}
  on:keydown={keydown} tabindex="-1">
  <span class="column-menu-label" title={label}>{label}</span>
  <button type="button" role="menuitem" on:click={() => dispatch('search', { columnId })}>
    Search this column
  </button>
  <button type="button" role="menuitem" on:click={() => dispatch('rule', { columnId })}>
    Add rule for this column…
  </button>
</div>

<style>
  .column-context-menu { min-width: 190px; max-width: min(300px, calc(100vw - 16px)); }
  .column-menu-label { display: block; padding: 4px 7px 5px; border-bottom: 1px solid var(--border); color: var(--muted); font-size: 10.5px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
</style>
