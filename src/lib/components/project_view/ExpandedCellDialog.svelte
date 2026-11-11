<script lang="ts">
  import { errorMessage } from "../../errors";
  import { createEventDispatcher, onDestroy } from 'svelte';
  import { createLatestRequest } from '../../latestRequest';
  import type { GetCellResult } from '../../backend';
  import type { ProjectRow } from '../../types';
  import {
    backend,
    columnLabels,
    expandedCell,
    hiddenColumns,
    projectDetail,
  } from './state';
  import { copyText, displayText } from '../../ipcValues';

  const dispatch = createEventDispatcher();

  let timeColumn = '';
  let mode: 'neighbors' | 'window' = 'neighbors';
  let count: number | null = 5;
  let around: ProjectRow[] | null = null;
  let error = '';
  let pinnedRow = -1;
  const aroundRequests = createLatestRequest();
  const cellRequests = createLatestRequest();

  // Full value for the focused cell only. It is cleared when the dialog
  // closes, so no page or row cache keeps an untruncated value.
  let fullValue: GetCellResult | null = null;
  let fullState: 'loading' | 'loaded' | 'error' = 'loading';
  let fullError = '';
  let loadedKey = '';
  let retry = 0;

  onDestroy(() => {
    aroundRequests.dispose();
    cellRequests.dispose();
  });

  const closeCell = () => {
    expandedCell.set(null);
  };

  const handleBackdropKey = (event: KeyboardEvent) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      closeCell();
    }
  };

  const handleBackdropClick = (event: MouseEvent) => {
    if (event.target === event.currentTarget) {
      closeCell();
    }
  };

  const loadFullValue = async () => {
    const target = $expandedCell;
    const api = $backend;
    if (!target || !api) {
      fullState = 'error';
      fullError = 'Cell value is unavailable.';
      return;
    }
    const ticket = cellRequests.next();
    fullValue = null;
    fullState = 'loading';
    fullError = '';
    try {
      const result = await api.getCell({
        projectId: target.projectId,
        rowIndex: target.rowIndex,
        columnId: target.columnId,
      });
      // A response for a closed dialog or a different project/row/column is
      // stale even if it arrived successfully.
      if (!cellRequests.isCurrent(ticket)) return;
      fullValue = result;
      fullState = 'loaded';
    } catch (err) {
      if (!cellRequests.isCurrent(ticket)) return;
      fullValue = null;
      fullState = 'error';
      fullError = errorMessage(err);
    }
  };

  // Opening the same cell again must fetch again; a different target or a
  // retry must discard the previous response.
  $: sourceKey = $expandedCell
    ? `${$expandedCell.projectId}\u0000${$expandedCell.rowIndex}\u0000${$expandedCell.columnId}`
    : '';
  $: fetchKey = sourceKey ? `${sourceKey}\u0000${retry}` : '';
  $: if (fetchKey && $backend && fetchKey !== loadedKey) {
    loadedKey = fetchKey;
    void loadFullValue();
  } else if (!fetchKey) {
    loadedKey = '';
    cellRequests.invalidate();
    fullValue = null;
    fullState = 'loading';
    fullError = '';
  }

  const copyExpandedCell = async () => {
    // Copy reads only the fetched full value. While loading or after a
    // failure the button is disabled, so the preview is never copied.
    if (fullState !== 'loaded' || !fullValue) return;
    const text = copyText(fullValue);
    try {
      if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
        await navigator.clipboard.writeText(text);
      } else {
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
      }
      dispatch('notify', { message: 'Copied cell value.', tone: 'success' });
    } catch (err) {
      console.error(err);
      dispatch('notify', { message: 'Failed to copy cell value.', tone: 'error' });
    }
  };

  const ready = (value: number | null) => Number.isInteger(value) && (value ?? 0) >= 0;

  $: timeColumns = ($projectDetail?.columns ?? []).filter(
    (name) => $projectDetail?.column_kinds?.[name] === 'datetime'
  );
  $: detailColumns = $projectDetail?.columns ?? [];
  $: projectId = $projectDetail?.project.meta.id ?? '';

  $: if (!$expandedCell && pinnedRow !== -1) {
    pinnedRow = -1;
  } else if ($expandedCell && $expandedCell.rowIndex !== pinnedRow) {
    pinnedRow = $expandedCell.rowIndex;
    timeColumn = timeColumns.includes($expandedCell.columnId)
      ? $expandedCell.columnId
      : timeColumns[0] ?? '';
    mode = 'neighbors';
    count = 5;
  }

  const loadAround = async (
    activeProjectId: string,
    rowIndex: number,
    column: string,
    activeMode: 'neighbors' | 'window',
    activeCount: number
  ) => {
    const api = $backend;
    if (!api) return;
    const ticket = aroundRequests.next();
    around = null;
    error = '';
    try {
      const next = await api.surroundingRows({
        projectId: activeProjectId,
        rowIndex,
        column,
        mode: activeMode,
        count: activeCount,
      });
      if (!aroundRequests.isCurrent(ticket)) return;
      around = next.rows;
    } catch (err) {
      if (!aroundRequests.isCurrent(ticket)) return;
      around = [];
      error = errorMessage(err);
    }
  };

  $: if ($expandedCell && $backend && projectId && timeColumn && ready(count)) {
    void loadAround(projectId, $expandedCell.rowIndex, timeColumn, mode, count ?? 0);
  } else if ($expandedCell) {
    aroundRequests.invalidate();
    around = [];
    error = timeColumn ? '' : 'No datetime column.';
  }

  const rowLine = (row: ProjectRow) =>
    detailColumns.map((name) => displayText(row.cells?.[name])).join('  ');
</script>

{#if $expandedCell}
  {@const row = $expandedCell.row}
  {@const focusColumn = $expandedCell.columnId}
  <div
    class="cell-dialog-backdrop"
    role="button"
    tabindex="0"
    on:click={handleBackdropClick}
    on:keydown={handleBackdropKey}
  >
    <div
      class="cell-dialog row-detail-dialog"
      role="dialog"
      aria-modal="true"
      aria-label={`Row ${$expandedCell.rowIndex}`}
    >
      <div class="cell-dialog-header">
        <h3>Row {$expandedCell.rowIndex}</h3>
        <div class="cell-dialog-actions">
          <button
            type="button"
            class="ghost"
            on:click={copyExpandedCell}
            disabled={fullState !== 'loaded'}
          >
            Copy
          </button>
          <button type="button" class="ghost close-dialog" on:click={closeCell}>Close</button>
        </div>
      </div>
      <div class="row-detail-scroll">
        <dl class="row-fields">
          {#each detailColumns as name}
            <dt class:is-focus={name === focusColumn}>
              {$columnLabels.get(name) ?? name}{$hiddenColumns.has(name) ? ' (hidden)' : ''}
            </dt>
            <dd>
              {#if name === focusColumn}
                {#if fullState === 'loading'}
                  <span class="cell-load-state">Loading full value…</span>
                {:else if fullState === 'error'}
                  <span class="memo-error">Failed to load the full value.</span>
                  {#if fullError}
                    <span class="cell-load-error-detail">{fullError}</span>
                  {/if}
                  <button
                    type="button"
                    class="ghost retry-cell"
                    on:click={() => (retry += 1)}
                  >
                    Retry
                  </button>
                {:else if fullValue}
                  {displayText(fullValue)}
                {/if}
              {:else}
                {row.displayCache[name] ?? ''}
              {/if}
            </dd>
          {/each}
        </dl>
        <div class="condition-toolbar">
          <span class="filter-label">Around</span>
          {#if timeColumns.length > 0}
            <select bind:value={timeColumn} aria-label="Datetime column">
              {#each timeColumns as name}
                <option value={name}>{$columnLabels.get(name) ?? name}</option>
              {/each}
            </select>
          {/if}
          <select bind:value={mode} aria-label="Around mode">
            <option value="neighbors">Neighbors</option>
            <option value="window">Window</option>
          </select>
          <label class="condition-count">
            {mode === 'neighbors' ? 'Rows each side' : '± seconds'}
            <input type="number" min="0" step="1" bind:value={count} aria-label="Around amount" />
          </label>
        </div>
        {#if error}
          <p class="memo-error">{error}</p>
        {:else if around === null}
          <p class="condition-count">Loading…</p>
        {:else}
          <ul class="row-around">
            {#each around as item (item.row_index)}
              <li class:is-anchor={item.row_index === $expandedCell.rowIndex}>{rowLine(item)}</li>
            {/each}
          </ul>
        {/if}
      </div>
    </div>
  </div>
{/if}
