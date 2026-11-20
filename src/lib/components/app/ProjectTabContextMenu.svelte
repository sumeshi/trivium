<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount, tick } from 'svelte';

  export let label: string;
  export let x: number;
  export let y: number;
  export let canCloseOthers = true;
  export let canCloseRight = false;
  export let disabled = false;

  const dispatch = createEventDispatcher<{
    close: { restoreFocus: boolean };
    action: 'close' | 'others' | 'right' | 'all';
  }>();
  let menu: HTMLDivElement;
  let left = x;
  let top = y;
  let alive = true;
  const close = (restoreFocus = false) => dispatch('close', { restoreFocus });
  const choose = (action: 'close' | 'others' | 'right' | 'all') => {
    if (disabled || action === 'others' && !canCloseOthers || action === 'right' && !canCloseRight) return;
    dispatch('action', action);
  };
  const outside = (event: MouseEvent) => {
    if (!menu?.contains(event.target as Node)) close();
  };
  const scrolling = (event: Event) => {
    if (!(event.target instanceof Node) || !menu?.contains(event.target)) close();
  };
  const keydown = (event: KeyboardEvent) => {
    if (event.key === 'Escape' || event.key === 'Tab') {
      if (event.key === 'Escape') event.preventDefault();
      event.stopPropagation();
      close(true);
      return;
    }
    const buttons = [...menu.querySelectorAll<HTMLButtonElement>('button:not(:disabled)')];
    if (!buttons.length) return;
    const at = buttons.indexOf(document.activeElement as HTMLButtonElement);
    const next = event.key === 'ArrowDown' ? (at + 1) % buttons.length
      : event.key === 'ArrowUp' ? (at - 1 + buttons.length) % buttons.length
      : event.key === 'Home' ? 0 : event.key === 'End' ? buttons.length - 1 : null;
    if (next === null) return;
    event.preventDefault();
    event.stopPropagation();
    buttons[next]?.focus({ preventScroll: true });
  };
  const placeMenu = async () => {
    await tick();
    if (!alive || !menu?.isConnected) return;
    const bounds = menu.getBoundingClientRect();
    left = Math.max(8, Math.min(x, innerWidth - bounds.width - 8));
    top = Math.max(8, Math.min(y, innerHeight - bounds.height - 8));
    menu.querySelector<HTMLButtonElement>('button:not(:disabled)')?.focus({ preventScroll: true });
  };
  onMount(() => { void placeMenu(); });
  onDestroy(() => { alive = false; });
</script>

<svelte:window on:mousedown={outside} on:scroll|capture={scrolling} on:resize={() => close()} />

<div class="project-tab-context-menu" bind:this={menu} role="menu" aria-label={`Tab actions: ${label}`}
  style={`left: ${left}px; top: ${top}px;`} tabindex="-1" on:keydown={keydown}>
  <span class="tab-menu-label" title={label}>{label}</span>
  <button type="button" role="menuitem" disabled={disabled} title="Close this tab; export temporary CSVs to keep them" on:click={() => choose('close')}>Close</button>
  <button type="button" role="menuitem" disabled={disabled || !canCloseOthers} title="Close other tabs; saved projects remain in Recent, temporary CSVs are removed" on:click={() => choose('others')}>Close others</button>
  <button type="button" role="menuitem" disabled={disabled || !canCloseRight} title="Close tabs to the right; saved projects remain in Recent, temporary CSVs are removed" on:click={() => choose('right')}>Close tabs to the right</button>
  <button type="button" role="menuitem" disabled={disabled} title="Close all tabs; saved projects remain in Recent, temporary CSVs are removed" on:click={() => choose('all')}>Close all</button>
</div>

<style>
  .project-tab-context-menu { position: fixed; z-index: 220; display: flex; flex-direction: column; min-width: min(176px, calc(100vw - 16px)); max-width: min(280px, calc(100vw - 16px)); max-height: calc(100vh - 16px); padding: 4px; overflow-y: auto; overscroll-behavior: contain; border: 1px solid var(--border); border-radius: 6px; background: var(--surface-strong); box-shadow: var(--shadow-strong); }
  .tab-menu-label { padding: 4px 7px 5px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; border-bottom: 1px solid var(--border); color: var(--muted); font-size: 10.5px; }
  button { min-height: 27px; padding: 4px 8px; border: 0; border-radius: 3px; background: transparent; color: var(--fg); font: inherit; font-size: 11px; text-align: left; white-space: normal; }
  button:hover:not(:disabled), button:focus-visible { background: var(--accent-soft); outline: none; }
  button:disabled { opacity: .45; cursor: default; }
</style>
