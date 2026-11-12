<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount, tick } from 'svelte';
  import type { CellKind } from '../../types';
  import type { CachedRow } from './state';

  import { copyText } from '../../ipcValues';
  import { useWorkspace } from "../../workspaceContext";
  const { backend, openCell, projectDetail, addFilterCondition, createReadScope } = useWorkspace();

  const reads = createReadScope();

  export let projectId: string;
  export let rowIndex: number;
  export let columnId: string;
  export let preview: string;
  export let truncated: boolean;
  export let kind: CellKind;
  export let row: CachedRow;
  export let x: number;
  export let y: number;

  const dispatch = createEventDispatcher<{
    close: void;
    notify: { message: string; tone: 'success' | 'error' };
  }>();

  let menuEl: HTMLDivElement | null = null;
  let firstButton: HTMLButtonElement | null = null;
  let left = x;
  let top = y;
  let busy = false;
  let alive = true;
  // A null/missing cell has no string value for an eq/ne condition, and the
  // evaluator intentionally treats null as non-matching. Do not offer a filter
  // that would silently match a literal "null" or empty string instead.
  $: filterable = kind !== 'null' && kind !== 'missing';

  onDestroy(() => {
    alive = false;
    reads.dispose();
  });

  const close = () => dispatch('close');

  const placeMenu = () => {
    if (!menuEl) return;
    const margin = 8;
    const rect = menuEl.getBoundingClientRect();
    left = x + rect.width + margin > window.innerWidth ? Math.max(margin, x - rect.width) : x;
    top = y + rect.height + margin > window.innerHeight ? Math.max(margin, y - rect.height) : y;
  };

  onMount(async () => {
    await tick();
    placeMenu();
    firstButton?.focus();
  });

  const handleWindowMouseDown = (event: MouseEvent) => {
    const target = event.target as Node | null;
    if (menuEl && target && menuEl.contains(target)) return;
    close();
  };

  const handleWindowKeydown = (event: KeyboardEvent) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      close();
    }
  };

  const handleWindowScroll = () => close();
  const handleWindowResize = () => close();

  /**
   * Returns the text used by Copy/Filter actions. A truncated list preview is
   * never promoted to the full value: the full cell is fetched instead, and a
   * failed fetch closes the menu with an error rather than falling back.
   */
  const resolveValue = async (): Promise<string | null> => {
    if (!truncated) {
      return preview;
    }
    const api = $backend;
    if (!api) {
      dispatch('notify', { message: 'Failed to load the full cell value.', tone: 'error' });
      close();
      return null;
    }
    try {
      const result = await reads.run("cell", api, (requestId) =>
        api.getCell({ projectId, rowIndex, columnId, requestId }));
      if (!alive) return null;
      return copyText(result);
    } catch (error) {
      console.error(error);
      if (!alive) return null;
      dispatch('notify', { message: 'Failed to load the full cell value.', tone: 'error' });
      close();
      return null;
    }
  };

  const applyFilter = async (op: 'eq' | 'ne') => {
    if (busy) return;
    busy = true;
    const value = await resolveValue();
    if (value === null) return;
    // The menu belongs to the project and row it was opened for. A project
    // switch must not add a condition built from another project's cell.
    if (!alive || $projectDetail?.project.meta.id !== projectId) {
      close();
      return;
    }
    addFilterCondition(columnId, op, value);
    close();
  };

  const writeClipboard = async (text: string) => {
    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
      await navigator.clipboard.writeText(text);
      return;
    }
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand('copy');
    document.body.removeChild(textarea);
  };

  const copyValue = async () => {
    if (busy) return;
    busy = true;
    const value = await resolveValue();
    if (value === null) return;
    try {
      await writeClipboard(value);
      dispatch('notify', { message: 'Copied cell value.', tone: 'success' });
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to copy cell value.', tone: 'error' });
    }
    close();
  };

  const viewDetails = () => {
    openCell(projectId, columnId, preview, row);
    close();
  };
</script>

<svelte:window
  on:mousedown={handleWindowMouseDown}
  on:keydown={handleWindowKeydown}
  on:scroll|capture={handleWindowScroll}
  on:resize={handleWindowResize}
/>

<div
  class="cell-context-menu"
  bind:this={menuEl}
  style={`left: ${left}px; top: ${top}px;`}
  role="menu"
  aria-label="Cell actions"
>
  <button
    type="button"
    role="menuitem"
    bind:this={firstButton}
    disabled={busy || !filterable}
    title={filterable ? '' : 'Null and missing cells have no filter value.'}
    on:click={() => applyFilter('eq')}
  >
    Filter in
  </button>
  <button
    type="button"
    role="menuitem"
    disabled={busy || !filterable}
    title={filterable ? '' : 'Null and missing cells have no filter value.'}
    on:click={() => applyFilter('ne')}
  >
    Filter out
  </button>
  <button type="button" role="menuitem" disabled={busy} on:click={copyValue}>
    Copy value
  </button>
  <button type="button" role="menuitem" disabled={busy} on:click={viewDetails}>
    View details
  </button>
</div>
