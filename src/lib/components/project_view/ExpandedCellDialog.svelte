<script lang="ts">
  import { createEventDispatcher, onDestroy, tick } from 'svelte';
  import { errorMessage } from '../../errors';
  import { displayText } from '../../ipcValues';
  import { createRowDetails, detailCopyValue, detailFilterValue } from '../../rowDetails';
  import type { DetailField } from '../../rowDetails';
  import type { GetCellResult } from '../../backend';
  import { useWorkspace } from '../../workspaceContext';

  const {
    backend, columnLabels, expandedCell, hiddenColumns, projectDetail,
    addFilterCondition, createReadScope,
  } = useWorkspace();
  const reads = createReadScope();
  const dispatch = createEventDispatcher();
  let fields: ReadonlyMap<string, DetailField> = new Map();
  let loadedKey = '';
  let copyStatus = '';
  let closeButton: HTMLButtonElement | null = null;
  let dialogEl: HTMLDivElement | null = null;
  let scrollEl: HTMLDivElement | null = null;
  let previousFocus: HTMLElement | null = null;
  let focusProject = '';

  const details = createRowDetails({
    read: (target, columnId) => {
      const api = $backend;
      if (!api) return Promise.reject(new Error('Field value is unavailable.'));
      return reads.run(`field:${columnId}`, api, (requestId) => api.getCell({
        projectId: target.projectId, rowIndex: target.rowIndex, columnId, requestId,
      }));
    },
    cancel: () => reads.cancelAll(),
    changed: (next) => { fields = next; },
  });

  onDestroy(() => { details.dispose(); reads.dispose(); });

  const restoreFocus = () => {
    if (previousFocus?.isConnected && focusProject === $projectDetail?.project.meta.id) {
      previousFocus.focus({ preventScroll: true });
    }
    previousFocus = null;
    focusProject = '';
  };
  const closeCell = () => { expandedCell.set(null); restoreFocus(); };
  const handleBackdropClick = (event: MouseEvent) => {
    if (event.target === event.currentTarget) closeCell();
  };
  const handleDialogKey = (event: KeyboardEvent) => {
    if (!$expandedCell) return;
    if (event.key === 'Escape') {
      event.preventDefault();
      closeCell();
    } else if (event.key === 'Tab' && dialogEl) {
      const buttons = [...dialogEl.querySelectorAll<HTMLButtonElement>('button:not(:disabled)')];
      const first = buttons[0], last = buttons.at(-1);
      if (event.shiftKey && document.activeElement === first && last) {
        event.preventDefault(); last.focus();
      } else if (!event.shiftKey && document.activeElement === last && first) {
        event.preventDefault(); first.focus();
      }
    }
  };

  const revealFocus = async (key: string, columnId: string) => {
    await tick();
    if (key !== loadedKey) return;
    closeButton?.focus({ preventScroll: true });
    const field = [...(scrollEl?.querySelectorAll<HTMLElement>('[data-column-id]') ?? [])]
      .find((item) => item.dataset.columnId === columnId);
    field?.scrollIntoView({ block: 'nearest' });
  };

  $: detailColumns = $projectDetail?.columns ?? [];
  $: sourceKey = $expandedCell
    ? `${$expandedCell.projectId}\u0000${$expandedCell.rowIndex}\u0000${$expandedCell.columnId}` : '';
  $: if (sourceKey && $backend && $expandedCell && sourceKey !== loadedKey) {
    if (!loadedKey && typeof document !== 'undefined') {
      previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      focusProject = $expandedCell.projectId;
    }
    loadedKey = sourceKey;
    copyStatus = '';
    details.load($expandedCell, detailColumns, $expandedCell.row.cells);
    void revealFocus(sourceKey, $expandedCell.columnId);
  } else if (!sourceKey && loadedKey) {
    loadedKey = '';
    copyStatus = '';
    details.clear();
    restoreFocus();
  }

  const loadedValue = (column: string) => {
    const field = fields.get(column);
    return field?.state === 'loaded' ? field.value : undefined;
  };
  const labelFor = (column: string) => $columnLabels.get(column) ?? column;
  const filterTip = (value: GetCellResult | undefined) => {
    if (!value) return 'Wait for the full field value.';
    if (value.kind === 'null') return 'Null cannot be used as an exact text filter.';
    if (value.kind === 'missing') return 'This field is missing; it has no filter value.';
    return 'Add this exact value to the current filters.';
  };
  const copyTip = (value: GetCellResult | undefined) => {
    if (!value) return 'Wait for the full field value.';
    if (value.kind === 'missing') return 'This field is missing; there is no value to copy.';
    return value.kind === 'null' ? 'Copy null as the text null.' : 'Copy the full field value.';
  };

  const writeClipboard = async (text: string) => {
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        return;
      }
    } catch { /* The user-initiated fallback also works when the API is denied. */ }
    const textarea = document.createElement('textarea');
    const focused = document.activeElement;
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.append(textarea);
    try {
      textarea.select();
      if (!document.execCommand('copy')) throw new Error('Clipboard access was denied.');
    } finally {
      textarea.remove();
      if (focused instanceof HTMLElement && focused.isConnected) focused.focus({ preventScroll: true });
    }
  };

  const copyField = async (column: string) => {
    const text = detailCopyValue(loadedValue(column));
    if (text === null) return;
    const key = sourceKey;
    const label = labelFor(column);
    try {
      await writeClipboard(text);
      if (key !== sourceKey || !$expandedCell) return;
      copyStatus = text === '' ? `Copied empty string from ${label}.` : `Copied ${label}.`;
      dispatch('notify', { message: copyStatus, tone: 'success' });
    } catch (error) {
      if (key !== sourceKey || !$expandedCell) return;
      copyStatus = errorMessage(error, 'Failed to copy field value.');
      dispatch('notify', { message: copyStatus, tone: 'error' });
    }
  };
  const filterField = (column: string, op: 'eq' | 'ne') => {
    const target = $expandedCell;
    const value = detailFilterValue(loadedValue(column));
    if (!target || value === null || target.projectId !== $projectDetail?.project.meta.id) return;
    addFilterCondition(column, op, value);
    closeCell();
  };
</script>

<svelte:window on:keydown={handleDialogKey} />

{#if $expandedCell}
  <div class="cell-dialog-backdrop" role="presentation" on:click={handleBackdropClick}>
    <div class="cell-dialog row-detail-dialog" role="dialog" aria-modal="true"
      aria-label={`Row ${$expandedCell.rowIndex + 1}`} bind:this={dialogEl}>
      <div class="cell-dialog-header">
        <h3>Row {$expandedCell.rowIndex + 1}</h3>
        <button type="button" class="ghost close-dialog" bind:this={closeButton} on:click={closeCell}>Close</button>
      </div>
      <p class="row-detail-copy-status" role="status" aria-live="polite">{copyStatus}</p>
      <div class="row-detail-scroll" bind:this={scrollEl}>
        <dl class="row-fields">
          {#each detailColumns as column}
            {@const field = fields.get(column)}
            {@const value = field?.state === 'loaded' ? field.value : undefined}
            {@const label = labelFor(column)}
            <div class="row-detail-field" class:is-focus={column === $expandedCell.columnId} data-column-id={column}>
              <dt>
                <span class="row-field-label">{label}{$hiddenColumns.has(column) ? ' (hidden)' : ''}</span>
                <div class="row-field-actions">
                  <button type="button" class="ghost" aria-label={`Copy ${label}`} title={copyTip(value)}
                    disabled={detailCopyValue(value) === null} on:click={() => copyField(column)}>Copy</button>
                  <button type="button" class="ghost" aria-label={`Filter in ${label}`} title={filterTip(value)}
                    disabled={detailFilterValue(value) === null} on:click={() => filterField(column, 'eq')}>Filter in</button>
                  <button type="button" class="ghost" aria-label={`Filter out ${label}`} title={filterTip(value)}
                    disabled={detailFilterValue(value) === null} on:click={() => filterField(column, 'ne')}>Filter out</button>
                </div>
              </dt>
              <dd>
                {#if field?.state === 'error'}
                  <p class="memo-error">Failed to load the full field value: {field.error}</p>
                  <button type="button" class="ghost" aria-label={`Retry ${label}`} on:click={() => details.retry(column)}>Retry</button>
                {:else if value}
                  {#if value.kind === 'missing'}
                    <span class="row-field-value row-field-empty" data-value-kind="missing">Missing field</span>
                  {:else if value.kind === 'null'}
                    <span class="row-field-value row-field-empty" data-value-kind="null">null</span>
                  {:else if value.text === ''}
                    <span class="row-field-value row-field-empty" data-value-kind="empty">Empty string</span>
                  {:else}
                    <pre class="row-field-value">{displayText(value)}</pre>
                  {/if}
                {:else}
                  <span class="row-field-empty">Loading full value…</span>
                {/if}
              </dd>
            </div>
          {/each}
        </dl>
      </div>
    </div>
  </div>
{/if}

<style>
  .row-fields { display: flex; flex-direction: column; gap: 0; margin: 0; border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
  .row-detail-field { padding: 10px 12px; border-bottom: 1px solid var(--border); border-left: 3px solid transparent; min-width: 0; }
  .row-detail-field:last-child { border-bottom: 0; }
  .row-detail-field.is-focus { border-left-color: var(--accent); background: var(--surface-weak); }
  dt { display: flex; align-items: center; justify-content: space-between; gap: 8px; flex-wrap: wrap; color: var(--fg); }
  .row-field-label { font-size: 12px; font-weight: 600; overflow-wrap: anywhere; }
  .row-field-actions { display: flex; align-items: center; gap: 4px; flex-wrap: wrap; }
  .row-field-actions button { padding: 3px 6px; font-size: 10.5px; }
  dd { margin: 7px 0 0; min-width: 0; }
  .row-field-value { margin: 0; color: var(--fg); font: 12px/1.55 var(--font-mono); white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word; }
  .row-field-empty { color: var(--muted); font-size: 12px; }
  .row-detail-copy-status { margin: -10px 0 0; min-height: 0; color: var(--muted); font-size: 11px; }
  .row-detail-copy-status:empty { display: none; }
  .memo-error { margin: 0 0 6px; }
</style>
