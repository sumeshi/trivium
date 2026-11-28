<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { isTabKey } from '../../keyboard';
  import type { ExportOptions, ExportSnapshot } from './state';

  export let snapshot: ExportSnapshot;

  const dispatch = createEventDispatcher<{
    close: void;
    export: ExportOptions;
  }>();

  // The default is the applied query result, not every row.
  let scope: ExportOptions['scope'] = 'filtered';
  let columnMode: 'all' | 'visible' = 'all';
  let includeAnnotations = true;
  let dialogEl: HTMLDivElement;
  let closeButton: HTMLButtonElement;
  let previousFocus: HTMLElement | null = null;
  let alive = true;

  const mountDialog = () => {
    if (!alive) return;
    previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    closeButton?.focus({ preventScroll: true });
  };
  onMount(mountDialog);
  onDestroy(() => {
    alive = false;
    if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true });
  });

  const keyboard = (event: KeyboardEvent) => {
    event.stopPropagation();
    if (event.key === 'Escape') {
      event.preventDefault();
      dispatch('close');
      return;
    }
    if ((!isTabKey(event) && event.keyCode !== 9) || event.altKey || event.ctrlKey || event.metaKey) return;
    const controls = [...dialogEl.querySelectorAll<HTMLElement>(
      'button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex="-1"])'
    )].filter(control => control.tabIndex >= 0 && control.getClientRects().length);
    const first = controls[0], last = controls.at(-1);
    if (!first || !last) {
      event.preventDefault();
      dialogEl.focus({ preventScroll: true });
    } else if (!controls.includes(document.activeElement as HTMLElement)) {
      event.preventDefault();
      (event.shiftKey ? last : first).focus({ preventScroll: true });
    } else if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus({ preventScroll: true });
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus({ preventScroll: true });
    }
  };

  const submit = () => {
    dispatch('export', {
      scope,
      useVisibleColumns: columnMode === 'visible',
      includeAnnotations
    });
  };
</script>

<div class="cell-dialog-backdrop" role="presentation">
  <div
    class="cell-dialog export-dialog"
    bind:this={dialogEl}
    role="dialog"
    aria-modal="true"
    aria-labelledby="export-dialog-title"
    tabindex="-1"
    on:keydown={keyboard}
  >
    <div class="cell-dialog-header">
      <h3 id="export-dialog-title">Export CSV</h3>
      <button type="button" class="ghost dialog-close" bind:this={closeButton} aria-label="Close" title="Close export dialog"
        on:click={() => dispatch('close')}>
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><path d="m6 6 12 12M18 6 6 18" /></svg>
      </button>
    </div>

    <fieldset class="export-section">
      <legend>Rows</legend>
      <div class="export-options">
        <label class="export-option">
          <input type="radio" value="all" bind:group={scope} />
          <span>All rows ({snapshot.totalRows})</span>
        </label>
        <label class="export-option">
          <input type="radio" value="filtered" bind:group={scope} />
          <span>Current results ({snapshot.totalFilteredRows})</span>
        </label>
        <label class="export-option" class:disabled={snapshot.selectedCount === 0}>
          <input
            type="radio"
            value="selected"
            bind:group={scope}
            disabled={snapshot.selectedCount === 0}
          />
          <span>Selected rows ({snapshot.selectedCount})</span>
        </label>
        {#if snapshot.selectedCount === 0}
          <p class="export-hint">
            Select rows in the table to export an explicit selection.
          </p>
        {/if}
      </div>
    </fieldset>

    <fieldset class="export-section">
      <legend>Columns</legend>
      <div class="export-options">
        <label class="export-option">
          <input type="radio" value="all" bind:group={columnMode} />
          <span>All data columns</span>
        </label>
        <label class="export-option">
          <input
            type="radio"
            value="visible"
            bind:group={columnMode}
            disabled={snapshot.visibleColumns.length === 0}
          />
          <span>Visible columns ({snapshot.visibleColumns.length})</span>
        </label>
      </div>
    </fieldset>

    <label class="export-option export-annotations">
      <input type="checkbox" bind:checked={includeAnnotations} />
      <span>Include annotations (flags, note, tags, bookmark)</span>
    </label>
    <p class="export-hint">
      {includeAnnotations
        ? 'Adds independent trivium-* columns. Tags are separate from the note.'
        : 'Writes data columns only.'}
    </p>
    <p class="export-hint"
      title="Values starting with =, +, -, or @ can be treated as formulas by spreadsheet software.">
      CSV preserves original values. Spreadsheet apps may interpret formulas.
    </p>

    <div class="cell-dialog-actions export-actions dialog-actions">
      <button type="button" class="ghost" on:click={() => dispatch('close')}>
        Cancel
      </button>
      <button type="button" class="primary" on:click={submit}>
        Export…
      </button>
    </div>
  </div>
</div>

<style>
  .export-dialog {
    /* WebKit shifts fieldset controls on hover when the dialog uses flex sizing. */
    display: block;
    width: min(520px, 92vw);
  }

  .export-dialog > :not(:first-child) {
    margin-top: 16px;
  }

  .export-section {
    margin: 0;
    padding: 12px 14px;
    border: 1px solid var(--border);
    border-radius: 12px;
  }

  .export-options {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .export-section legend {
    padding: 0 6px;
    font-size: 0.8rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .export-option {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.9rem;
    color: var(--fg);
  }

  .export-option.disabled {
    color: var(--muted);
  }

  .export-annotations {
    padding: 0 2px;
  }

  .export-hint {
    margin: 0;
    font-size: 0.8rem;
    color: var(--muted);
  }

  .export-actions {
    justify-content: flex-end;
  }
</style>
