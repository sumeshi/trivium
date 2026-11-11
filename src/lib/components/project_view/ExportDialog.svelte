<script lang="ts">
  import { createEventDispatcher } from 'svelte';
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
    role="dialog"
    aria-modal="true"
    aria-labelledby="export-dialog-title"
  >
    <div class="cell-dialog-header">
      <h3 id="export-dialog-title">Export CSV</h3>
      <button type="button" class="ghost" on:click={() => dispatch('close')}>
        Close
      </button>
    </div>

    <fieldset class="export-section">
      <legend>Rows</legend>
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
    </fieldset>

    <fieldset class="export-section">
      <legend>Columns</legend>
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
    </fieldset>

    <label class="export-option export-annotations">
      <input type="checkbox" bind:checked={includeAnnotations} />
      <span>Include annotations (flags, memo, tags, bookmark)</span>
    </label>
    <p class="export-hint">
      {includeAnnotations
        ? 'Adds independent trivium-* columns. Tags are separate from the memo.'
        : 'Writes data columns only.'}
    </p>

    <div class="cell-dialog-actions export-actions">
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
    width: min(520px, 92vw);
  }

  .export-section {
    margin: 0;
    padding: 12px 14px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    border: 1px solid var(--border);
    border-radius: 12px;
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
