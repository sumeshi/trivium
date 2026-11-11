<script lang="ts">
  import { createEventDispatcher, onDestroy } from 'svelte';
  import type { ProjectRow } from '../../types';
  import {
    backend,
    columnLabels,
    expandedCell,
    hiddenColumns,
    projectDetail,
  } from './state';
  import { displayText } from '../../ipcValues';

  const dispatch = createEventDispatcher();

  let timeColumn = '';
  let mode: 'neighbors' | 'window' = 'neighbors';
  let count: number | null = 5;
  let around: ProjectRow[] | null = null;
  let error = '';
  let request = 0;
  let pinnedRow = -1;
  let alive = true;

  onDestroy(() => {
    alive = false;
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

  const copyExpandedCell = async () => {
    if (!$expandedCell) return;
    const text = $expandedCell.value ?? '';
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
  } else if ($expandedCell && $expandedCell.row.row_index !== pinnedRow) {
    pinnedRow = $expandedCell.row.row_index;
    timeColumn = timeColumns.includes($expandedCell.column) ? $expandedCell.column : timeColumns[0] ?? '';
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
    const ticket = ++request;
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
      if (!alive || ticket !== request) return;
      around = next.rows;
    } catch (err) {
      if (!alive || ticket !== request) return;
      around = [];
      error = err instanceof Error ? err.message : String(err);
    }
  };

  $: if ($expandedCell && $backend && projectId && timeColumn && ready(count)) {
    void loadAround(projectId, $expandedCell.row.row_index, timeColumn, mode, count ?? 0);
  } else if ($expandedCell) {
    request += 1;
    around = [];
    error = timeColumn ? '' : 'No datetime column.';
  }

  const rowLine = (row: ProjectRow) =>
    detailColumns.map((name) => displayText(row.cells?.[name])).join('  ');
</script>

{#if $expandedCell}
  {@const row = $expandedCell.row}
  {@const focusColumn = $expandedCell.column}
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
      aria-label={`Row ${row.row_index}`}
    >
      <div class="cell-dialog-header">
        <h3>Row {row.row_index}</h3>
        <div class="cell-dialog-actions">
          <button type="button" class="ghost" on:click={copyExpandedCell}>Copy</button>
          <button type="button" class="ghost close-dialog" on:click={closeCell}>Close</button>
        </div>
      </div>
      <div class="row-detail-scroll">
        <dl class="row-fields">
          {#each detailColumns as name}
            <dt class:is-focus={name === focusColumn}>
              {$columnLabels.get(name) ?? name}{$hiddenColumns.has(name) ? ' (hidden)' : ''}
            </dt>
            <dd>{row.displayCache[name] ?? ''}</dd>
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
              <li class:is-anchor={item.row_index === row.row_index}>{rowLine(item)}</li>
            {/each}
          </ul>
        {/if}
      </div>
    </div>
  </div>
{/if}
