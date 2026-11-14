<script lang="ts">
  import { createEventDispatcher, onMount, tick } from 'svelte';
  import { isTabKey } from '../../keyboard';
  import { protectRecycledActivation } from '../../recycledActivation';

  export let projectId: string;
  export let rowIndex: number;
  export let tags: readonly string[];
  export let trigger: HTMLButtonElement;
  export let menuId: string;
  const dispatch = createEventDispatcher<{
    search: { tag: string };
    close: { restoreFocus: boolean; tab?: KeyboardEvent };
  }>();
  let menu: HTMLDivElement;
  let left = 0;
  let top = 0;
  const identity = () => ({ projectId, rowIndex, columnId: null });
  const close = (restoreFocus = false) => dispatch('close', { restoreFocus });
  const outside = (event: MouseEvent) => {
    if (!menu?.contains(event.target as Node) && !trigger.contains(event.target as Node)) close();
  };
  const scrolling = (event: Event) => {
    if (!(event.target instanceof Node) || !menu?.contains(event.target)) close();
  };
  const keydown = (event: KeyboardEvent) => {
    if (event.key === 'Escape') {
      event.preventDefault(); event.stopPropagation(); close(true); return;
    }
    if (isTabKey(event) && !event.altKey && !event.ctrlKey && !event.metaKey) {
      event.preventDefault();
      dispatch('close', { restoreFocus: true, tab: event });
      return;
    }
    const buttons = [...menu.querySelectorAll<HTMLButtonElement>('[role="menuitem"]')];
    const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
    const next = event.key === 'ArrowDown' ? (index + 1) % buttons.length
      : event.key === 'ArrowUp' ? (index - 1 + buttons.length) % buttons.length
      : event.key === 'Home' ? 0 : event.key === 'End' ? buttons.length - 1 : null;
    if (next === null) return;
    event.preventDefault(); buttons[next]?.focus();
  };
  const portal = (node: HTMLElement) => {
    document.body.appendChild(node);
    return { destroy: () => node.remove() };
  };
  onMount(async () => {
    await tick();
    if (!menu?.isConnected || !trigger.isConnected) return;
    const anchor = trigger.getBoundingClientRect(), rect = menu.getBoundingClientRect();
    left = Math.max(8, Math.min(anchor.right - rect.width, innerWidth - rect.width - 8));
    top = anchor.bottom + rect.height + 4 <= innerHeight - 8
      ? anchor.bottom + 4 : Math.max(8, anchor.top - rect.height - 4);
    menu.querySelector<HTMLButtonElement>('[role="menuitem"]')?.focus();
  });
</script>

<svelte:window on:mousedown={outside} on:scroll|capture={scrolling} on:resize={() => close()} on:blur={() => close()} />

<div class="row-tags-menu" id={menuId} bind:this={menu} use:portal
  use:protectRecycledActivation={identity} style={`left: ${left}px; top: ${top}px;`}
  role="menu" aria-label={`Tags for row ${rowIndex + 1}`} tabindex="-1" on:keydown={keydown}>
  <div class="row-tags-heading">
    <span>Tags <span class="tag-count">{tags.length}</span></span>
    <button type="button" class="tag-menu-close" title="Close tags" aria-label="Close tags" on:click={() => close(true)}>
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="m6 6 12 12M6 18 18 6" /></svg>
    </button>
  </div>
  <div class="row-tags-list">
    {#each tags as tag}
      <button type="button" role="menuitem" class="row-tag-item" aria-label={`Search tag ${tag}`}
        title={`Search tag: ${tag}`} on:click={() => dispatch('search', { tag })}>
        <svg class="tag-search-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><circle cx="10" cy="10" r="6" /><path d="m15 15 5 5" /></svg>
        <span>{tag}</span>
      </button>
    {/each}
  </div>
</div>

<style>
  .row-tags-menu {
    position: fixed; z-index: 220; display: flex; flex-direction: column; padding: 5px;
    min-width: min(180px, calc(100vw - 16px)); max-width: min(320px, calc(100vw - 16px));
    max-height: min(300px, calc(100vh - 16px)); border: 1px solid var(--border);
    border-radius: 5px; background: var(--surface); color: var(--fg);
    box-shadow: 0 4px 18px #0003; font-size: 11px;
  }
  .row-tags-heading { display: flex; flex: 0 0 auto; align-items: center; justify-content: space-between; gap: 12px; padding: 0 2px 3px 5px; font-weight: 600; }
  .tag-count { margin-left: 3px; color: var(--muted); font-weight: 400; }
  .tag-menu-close { display: inline-flex; align-items: center; justify-content: center; width: 22px; height: 22px; border: 0; border-radius: 3px; padding: 0; background: transparent; color: var(--muted); }
  .row-tags-list { min-height: 0; overflow-y: auto; overscroll-behavior: contain; }
  .row-tag-item { display: flex; align-items: flex-start; gap: 6px; width: 100%; padding: 5px 6px; border: 0; border-radius: 3px; background: transparent; color: inherit; font: inherit; text-align: left; }
  .row-tag-item span { min-width: 0; white-space: pre-wrap; overflow-wrap: anywhere; }
  .tag-search-icon { flex: 0 0 auto; margin-top: 1px; color: var(--muted); }
  .row-tag-item:hover, .tag-menu-close:hover { background: var(--accent-soft); }
  button:focus-visible { outline: 1px solid var(--accent); outline-offset: -1px; }
</style>
