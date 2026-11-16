<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { save } from '@tauri-apps/plugin-dialog';
  import { acceptSave } from '../../saveDrain';
  import { errorMessage } from '../../errors';
  import { tableTextSize, tableRowHeight } from '../../tableTextSize';
  import { createExtractView, EXTRACT_PAGE_SIZE, EXTRACT_SOURCE_PAGE_SIZE } from '../../extractView';
  import { extractFileName, extractValuePreview } from '../../extractPresentation';
  import { exportExtractWithConfirmation } from '../../extractExport';
  import type { ExtractBackend, ExtractFilter, ExtractIntent, ExtractNavigation, ExtractTab } from '../../extractTypes';
  export let backend: ExtractBackend;
  export let tab: ExtractTab;
  const dispatch = createEventDispatcher<{
    navigate: ExtractNavigation;
    filter: ExtractFilter;
    rerun: ExtractIntent;
    notify: { message: string; tone: 'success' | 'error' };
  }>();
  const view = createExtractView(backend, tab.id);
  const state = view.state;
  let disposed = false;
  let exporting = false;
  let exportError = '';
  const labels = new Map(tab.sourceColumns.map(column => [column.id, column.label]));
  const columnList = tab.result.columns.map(id => labels.get(id) ?? id).join(', ');
  const notify = (message: string, tone: 'success' | 'error') => dispatch('notify', { message, tone });
  const filter = (value: string) => dispatch('filter', { projectId: tab.result.projectId,
    columns: [...tab.result.columns], format: structuredClone(tab.result.format), value });
  const navigate = (rowIndex: number | null) => dispatch('navigate', { projectId: tab.result.projectId, rowIndex });
  const rerun = () => dispatch('rerun', { request: structuredClone(tab.request),
    sourceName: tab.sourceName, sourceColumns: structuredClone(tab.sourceColumns) });
  onMount(() => { void view.loadPage(); });
  onDestroy(() => { disposed = true; view.dispose(); });

  const copy = async (value: string) => {
    let copied = false;
    try { await navigator.clipboard.writeText(value); copied = true; }
    catch {
      const input = document.createElement('textarea');
      const focused = document.activeElement;
      input.value = value; input.style.position = 'fixed'; input.style.opacity = '0';
      document.body.append(input);
      try { input.select(); copied = document.execCommand('copy'); }
      catch { copied = false; }
      finally { input.remove(); if (focused instanceof HTMLElement && focused.isConnected) focused.focus({ preventScroll: true }); }
    }
    if (!disposed) notify(copied ? 'Value copied.' : 'Could not copy the value.', copied ? 'success' : 'error');
  };
  const exportCsv = async () => {
    if (exporting) return;
    exporting = true; exportError = '';
    try {
      await acceptSave(async () => {
        const destination = await save({ defaultPath: extractFileName(tab.sourceName, tab.result.format.id),
          filters: [{ name: 'Extracted values CSV', extensions: ['csv'] }] });
        if (!destination || disposed) return;
        const result = await exportExtractWithConfirmation(backend, { resultId: tab.id, destination },
          path => !disposed && window.confirm(`Replace the existing file?\n${path}`));
        if (!result) return;
        if (!disposed) notify(`Exported ${result.rows.toLocaleString()} values.${result.warnings.length ? ` ${result.warnings.join(' ')}` : ''}`, 'success');
      });
    } catch (error) {
      if (!disposed) exportError = errorMessage(error, 'Could not export the extracted values.');
    } finally { if (!disposed) exporting = false; }
  };
</script>

<section class="extract-view" aria-label={`${tab.result.format.name} extraction from ${tab.sourceName}`}
  style={`--extract-text-scale: ${$tableTextSize / 100}; --extract-row-height: ${$tableRowHeight}px;`}>
  <div class="extract-toolbar">
    <div class="extract-heading">
      <strong title={tab.result.format.name}>{tab.result.format.name}</strong>
      <button type="button" class="source-link" on:click={() => navigate(null)} title={`Open ${tab.sourceName}`}>{tab.sourceName}</button>
      <span class="scope-summary" title={`${tab.result.scope === 'all' ? 'All rows' : 'Current results at scan time'}\nColumns: ${columnList}`}>
        {tab.result.scope === 'all' ? 'All rows' : 'Results at scan time'} · {tab.result.columns.length.toLocaleString()} columns
      </span>
    </div>
    <div class="toolbar-actions">
      <button type="button" class="icon" aria-label="Run extraction again" title="Repeat the extraction with its original conditions" on:click={rerun}>
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M20 7v5h-5M4 17v-5h5M5.5 7a7 7 0 0 1 11.6-2L20 8M4 16l2.9 3A7 7 0 0 0 18.5 17" /></svg>
      </button>
      <button type="button" class="icon" disabled={exporting} aria-label="Export extracted values to CSV" title="Export extracted values to CSV" on:click={() => void exportCsv()}>
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M12 3v12m-4-4 4 4 4-4M4 15v6h16v-6" /></svg>
      </button>
    </div>
  </div>
  <div class="extract-summary" aria-label="Extraction totals">
    <span><b>{tab.result.uniqueValues.toLocaleString()}</b> unique values</span>
    <span><b>{tab.result.totalOccurrences.toLocaleString()}</b> occurrences</span>
    <span><b>{tab.result.matchedRows.toLocaleString()}</b> / {tab.result.sourceRows.toLocaleString()} rows</span>
    {#if exporting}<span role="status">Exporting…</span>{/if}
  </div>
  {#if exportError}<p class="extract-error" role="alert">{exportError}</p>{/if}
  <div class="ranking" aria-busy={$state.loading}>
    {#if $state.error}
      <div class="query-status" role="alert"><p>{$state.error}</p>
        <div><button type="button" on:click={() => void view.loadPage($state.offset)}>Retry</button><button type="button" on:click={rerun}>Run again</button></div>
      </div>
    {:else if $state.loading}
      <div class="query-status" role="status">Loading extracted values…</div>
    {:else if $state.page}
      <table class="value-table">
        <thead><tr><th scope="col">Value</th><th scope="col" class="number">Count</th><th scope="col" class="number">Rows</th>
          {#if tab.result.hasClassification}<th scope="col" class="classification">Scope</th>{/if}<th scope="col"><span class="sr-only">Actions</span></th></tr></thead>
        <tbody>
          {#each $state.page.rows as row (row.value)}
            <tr class:selected={$state.selectedValue === row.value}>
              <td class="value-cell"><button type="button" class="value-button" title={`${extractValuePreview(row.value, 512)}\nShow original occurrences`}
                on:click={() => void view.selectValue(row.value)}>{extractValuePreview(row.value)}</button></td>
              <td class="number">{row.count.toLocaleString()}</td><td class="number">{row.rows.toLocaleString()}</td>
              {#if tab.result.hasClassification}<td class="classification">{row.scope || 'Unclassified'}</td>{/if}
              <td class="row-actions">
                <button type="button" class="icon" aria-label="Copy extracted value" title="Copy value" on:click={() => void copy(row.value)}>
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><rect x="8" y="8" width="12" height="13" rx="2" /><path d="M16 8V3H3v13h5" /></svg>
                </button>
                <button type="button" class="icon" aria-label="Filter original rows by extracted value" title="Add this exact extracted value to the original row filters" on:click={() => filter(row.value)}>
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M4 4h16v4l-6 6v5l-4 2v-7L4 8Z" /></svg>
                </button>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
      {#if $state.page.total === 0}<p class="query-status">No values matched this pattern.</p>{/if}
    {/if}
  </div>
  <div class="pagination" aria-label="Extracted value pages">
    <span>{$state.page ? `${$state.page.total ? $state.page.offset + 1 : 0}–${$state.page.offset + $state.page.rows.length} of ${$state.page.total.toLocaleString()}` : 'Results unavailable'}</span>
    <div>
      <button type="button" class="icon" aria-label="Previous extracted values" title="Previous values" disabled={$state.loading || $state.offset === 0}
        on:click={() => void view.loadPage($state.offset - EXTRACT_PAGE_SIZE)}>‹</button>
      <button type="button" class="icon" aria-label="Next extracted values" title="Next values" disabled={$state.loading || !$state.page || $state.offset + $state.page.rows.length >= $state.page.total}
        on:click={() => void view.loadPage($state.offset + EXTRACT_PAGE_SIZE)}>›</button>
    </div>
  </div>
  {#if $state.selectedValue !== null}
    <section class="source-panel" aria-label="Original occurrences">
      <div class="source-header"><strong title={extractValuePreview($state.selectedValue, 512)}>{extractValuePreview($state.selectedValue)}</strong>
        <button type="button" class="icon" aria-label="Close original occurrences" title="Close original occurrences" on:click={view.closeSources}>×</button></div>
      {#if $state.sourcesError}<div class="source-status extract-error" role="alert">{$state.sourcesError}
        <button type="button" on:click={() => $state.selectedValue !== null && void view.selectValue($state.selectedValue, $state.sourcesOffset)}>Retry</button></div>
      {:else if $state.sourcesLoading}<p class="source-status" role="status">Loading original occurrences…</p>
      {:else if $state.sources}
        <div class="source-scroll"><table class="source-table"><thead><tr><th scope="col">Row</th><th scope="col">Column</th><th scope="col" title="UTF-8 byte positions in the complete original cell value">Position</th></tr></thead>
          <tbody>{#each $state.sources.rows as source}
            <tr><td><button type="button" class="source-link" aria-label={`Go to original row ${source.rowIndex + 1}`} title={`Go to original row ${source.rowIndex + 1}`} on:click={() => navigate(source.rowIndex)}>#{(source.rowIndex + 1).toLocaleString()}</button></td>
              <td title={labels.get(source.column) ?? source.column}>{labels.get(source.column) ?? source.column}</td><td>{source.start}–{source.end}</td></tr>
          {/each}</tbody></table>
          {#if !$state.sources.rows.length}<p class="source-status">No original occurrences.</p>{/if}
        </div>
        <div class="pagination source-pagination"><span>Occurrences {$state.sourcesOffset + ($state.sources.rows.length ? 1 : 0)}–{$state.sourcesOffset + $state.sources.rows.length}</span><div>
          <button type="button" class="icon" aria-label="Previous original occurrences" title="Previous occurrences" disabled={$state.sourcesOffset === 0}
            on:click={() => $state.selectedValue !== null && void view.selectValue($state.selectedValue, $state.sourcesOffset - EXTRACT_SOURCE_PAGE_SIZE)}>‹</button>
          <button type="button" class="icon" aria-label="Next original occurrences" title="Next occurrences" disabled={!$state.sources.hasMore}
            on:click={() => $state.selectedValue !== null && void view.selectValue($state.selectedValue, $state.sourcesOffset + EXTRACT_SOURCE_PAGE_SIZE)}>›</button>
        </div></div>
      {/if}
    </section>
  {/if}
</section>

<style>
  .extract-view { display: flex; flex-direction: column; min-width: 0; height: 100%; min-height: 0; padding: 8px 12px 0; color: var(--fg); background: var(--surface); font-size: 11px; }
  .extract-toolbar, .extract-heading, .toolbar-actions, .extract-summary, .pagination, .source-header { display: flex; align-items: center; gap: 9px; min-width: 0; }
  .extract-toolbar { justify-content: space-between; min-height: 30px; }
  .extract-heading { overflow: hidden; }
  .extract-heading strong { flex-shrink: 0; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .toolbar-actions { flex-shrink: 0; }
  .source-link { border: 0; background: transparent; color: var(--accent); padding: 2px 0; text-align: left; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .scope-summary { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--muted); }
  .extract-summary { flex-wrap: wrap; gap: 5px 16px; padding: 5px 0 8px; color: var(--muted); font-size: 10.5px; }
  .extract-summary b { color: var(--fg); font-weight: 600; }
  button { cursor: pointer; color: var(--fg); font: inherit; }
  button:disabled { opacity: .45; cursor: default; }
  button:not(.icon):not(.source-link):not(.value-button) { border: 1px solid var(--border); border-radius: 4px; background: var(--surface); padding: 4px 8px; }
  .icon { display: inline-flex; align-items: center; justify-content: center; flex-shrink: 0; width: 25px; height: 25px; border: 1px solid var(--border); border-radius: 4px; padding: 0; background: transparent; line-height: 1; font-size: 17px; }
  button:hover:not(:disabled) { background: var(--surface-weak); color: var(--accent); }
  button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: -1px; }
  .ranking { position: relative; flex: 1 1 auto; min-height: 90px; overflow: auto; border: 1px solid var(--border); border-radius: 4px; }
  table { width: 100%; border-collapse: collapse; table-layout: fixed; }
  .value-table { font-size: calc(11.5px * var(--extract-text-scale, 1)); }
  th { position: sticky; top: 0; z-index: 1; color: var(--muted); background: var(--surface-weak); text-align: left; font-size: 10px; font-weight: 500; }
  th, td { padding: 4px 9px; border-bottom: 1px solid var(--border); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  td { height: var(--extract-row-height, 32px); padding-top: 0; padding-bottom: 0; background: var(--surface); }
  tr:nth-child(even) td { background: var(--surface-soft); }
  tr.selected td { background: var(--accent-soft); }
  .number { width: 95px; text-align: right; font-variant-numeric: tabular-nums; }
  .classification { width: 110px; }
  .value-table th:last-child { width: 75px; }
  .value-button { display: block; width: 100%; height: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; border: 0; padding: 0; background: transparent; text-align: left; color: inherit; }
  .row-actions { white-space: nowrap; text-align: right; }
  .row-actions .icon { width: 22px; height: min(22px, calc(var(--extract-row-height, 32px) - 2px)); border: 0; vertical-align: middle; }
  .pagination { justify-content: space-between; flex-shrink: 0; min-height: 32px; color: var(--muted); font-size: 10.5px; }
  .pagination > div { display: flex; gap: 5px; }
  .query-status { display: flex; flex-direction: column; align-items: center; gap: 9px; padding: 24px 14px; text-align: center; color: var(--muted); white-space: pre-wrap; overflow-wrap: anywhere; }
  .query-status p { margin: 0; }
  .query-status > div { display: flex; gap: 8px; }
  .extract-error { margin: 0 0 7px; color: var(--danger); white-space: pre-wrap; overflow-wrap: anywhere; }
  .source-panel { display: flex; flex-direction: column; flex: 0 1 38%; min-height: 120px; max-height: 42%; border-top: 1px solid var(--border-strong); }
  .source-header { justify-content: space-between; flex-shrink: 0; padding: 5px 0; }
  .source-header strong { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 11px; font-weight: 500; }
  .source-scroll { flex: 1 1 auto; min-height: 0; overflow: auto; }
  .source-table { font-size: calc(11px * var(--extract-text-scale, 1)); }
  .source-table th:first-child { width: 105px; }.source-table th:last-child { width: 125px; }
  .source-pagination { min-height: 30px; }
  .source-status { margin: 0; padding: 8px 0; color: var(--muted); }
  .source-status.extract-error { color: var(--danger); }
  .sr-only { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0,0,0,0); }
  @media (max-width: 650px) { .scope-summary { display: none; }.number { width: 65px; }.classification { width: 85px; }.extract-view { padding-left: 7px; padding-right: 7px; } }
</style>
