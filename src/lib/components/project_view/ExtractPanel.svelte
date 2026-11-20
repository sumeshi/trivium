<script lang="ts">
  import { createEventDispatcher, tick } from 'svelte';
  import { get } from 'svelte/store';
  import { useWorkspace } from '../../workspaceContext';
  import { formats, formatCatalogError, ensureFormatsLoaded } from '../../patternCatalog';
  import type { FormatDefinition } from '../../formats';
  import type { ExtractIntent, ExtractPending } from '../../extractTypes';
  import PatternLabel from './PatternLabel.svelte';
  const { backend, projectDetail, columnLabels, appliedFilters, queryCountKnown, totalFilteredRows } = useWorkspace();
  export let open = false;
  export let pending: ExtractPending | null = null;
  export let error: string | null = null;
  export let selectedColumns: string[] | null = null;
  export let scope: 'all' | 'filtered' = 'filtered';
  const dispatch = createEventDispatcher<{ extract: ExtractIntent }>();
  let columnSearch = '';
  let patternSearch = '';
  let columnMenuOpen = false;
  let columnPicker: HTMLDivElement | null = null;
  let columnTrigger: HTMLButtonElement | null = null;
  let columnInput: HTMLInputElement | null = null;
  let columnMenuHeight = 320;
  $: columns = $projectDetail?.columns ?? [];
  $: selected = selectedColumns === null ? columns : columns.filter(column => selectedColumns?.includes(column));
  $: matchingColumns = columns.filter(column => ($columnLabels.get(column) ?? column).toLocaleLowerCase().includes(columnSearch.trim().toLocaleLowerCase()));
  $: patterns = $formats.filter(entry => entry.definition.name.toLocaleLowerCase().includes(patternSearch.trim().toLocaleLowerCase()));
  $: if (open && $backend) void ensureFormatsLoaded($backend).catch(() => {});
  $: cannotRun = !selected.length || Boolean(pending) || (scope === 'filtered' && !$queryCountKnown);
  $: if (pending) columnMenuOpen = false;

  const closeColumns = (restoreFocus = false) => {
    columnMenuOpen = false;
    if (restoreFocus) columnTrigger?.focus({ preventScroll: true });
  };
  const toggleColumns = async () => {
    if (columnMenuOpen) { closeColumns(); return; }
    if (pending) return;
    const bottom = columnPicker?.closest('.analysis-body')?.getBoundingClientRect().bottom ?? innerHeight;
    columnMenuHeight = Math.max(80, Math.min(340, bottom - (columnPicker?.getBoundingClientRect().bottom ?? 0) - 10));
    columnMenuOpen = true;
    await tick();
    if (columnMenuOpen) columnInput?.focus({ preventScroll: true });
  };
  const outside = (event: MouseEvent) => {
    if (columnMenuOpen && columnPicker && !event.composedPath().includes(columnPicker)) closeColumns();
  };
  const escape = (event: KeyboardEvent) => {
    if (event.key !== 'Escape' || !columnMenuOpen) return;
    event.preventDefault();
    event.stopPropagation();
    closeColumns(true);
  };

  const toggleColumn = (column: string) => {
    selectedColumns = selected.includes(column) ? selected.filter(id => id !== column) : [...selected, column];
  };
  const run = (format: FormatDefinition) => {
    const detail = get(projectDetail);
    if (!detail || cannotRun) return;
    const filters = get(appliedFilters);
    const all = scope === 'all';
    closeColumns();
    dispatch('extract', structuredClone({
      sourceName: detail.project.meta.source_file_name || detail.project.meta.name,
      request: { projectId: detail.project.meta.id, columns: selected, format, scope,
        search: all ? '' : filters.search, searchColumns: all ? [] : filters.searchColumns,
        flagFilter: all ? 'all' : filters.flag, bookmarkedOnly: !all && filters.bookmarkedOnly,
        condition: all ? null : filters.condition, sortKey: null, sortDirection: 'asc' },
    }));
  };
</script>

<svelte:document on:click={outside} on:keydown|capture={escape} />
<svelte:window on:resize={() => closeColumns()} />

<section class="extract-panel" aria-label="Extract values">
  <p class="extract-help">Extract matching values into a background tab, grouped by value.</p>
  <div class="extract-source">
    <label class="scope-setting">From
      <select aria-label="Extraction rows" bind:value={scope} disabled={Boolean(pending)}
        title={scope === 'filtered' ? `Current results${$queryCountKnown ? `: ${$totalFilteredRows.toLocaleString()} rows` : ''}` : `All rows: ${($projectDetail?.project.meta.total_records ?? 0).toLocaleString()}`}>
        <option value="filtered">Current results</option>
        <option value="all">All rows</option>
      </select>
    </label>
    <div class="column-picker" bind:this={columnPicker}>
    <button type="button" class="column-trigger" bind:this={columnTrigger} aria-label="Choose extraction columns" aria-expanded={columnMenuOpen} disabled={Boolean(pending)}
      aria-controls="extraction-column-list" title={`Extract from ${selected.length.toLocaleString()} of ${columns.length.toLocaleString()} columns`}
      on:click={toggleColumns}>
      <span>Columns <span class="column-count">{selected.length === columns.length ? 'All' : `${selected.length}/${columns.length}`}</span></span><span aria-hidden="true">▾</span>
    </button>
    {#if columnMenuOpen}
      <div id="extraction-column-list" class="column-menu" role="group" aria-label="Extraction columns" style={`max-height: ${columnMenuHeight}px`}>
        <div class="column-menu-head"><strong>Columns</strong>
          <button type="button" class="column-close" aria-label="Close extraction columns" title="Close" on:click={() => closeColumns(true)}>×</button>
        </div>
        <div class="column-actions">
          <button type="button" disabled={Boolean(pending)} on:click={() => selectedColumns = null}>Select all</button>
          <button type="button" disabled={Boolean(pending)} on:click={() => selectedColumns = []}>Deselect all</button>
        </div>
        <input type="search" aria-label="Find an extraction column" placeholder="Find column" bind:this={columnInput} bind:value={columnSearch} />
        <div class="column-list">
          {#each matchingColumns as column}
            <label title={$columnLabels.get(column) ?? column}>
              <input type="checkbox" checked={selected.includes(column)} disabled={Boolean(pending)} on:change={() => toggleColumn(column)} />
              <span>{$columnLabels.get(column) ?? column}</span>
            </label>
          {/each}
          {#if !matchingColumns.length}<p class="extract-help">No matching columns.</p>{/if}
        </div>
      </div>
    {/if}
    </div>
  </div>
  {#if !selected.length}<p class="extract-help">Choose at least one column.</p>{/if}
  {#if scope === 'filtered' && !$queryCountKnown}<p class="extract-help">Wait for the current results before scanning.</p>{/if}
  {#if error}<p class="extract-error" role="alert">{error}</p>{/if}
  {#if $formatCatalogError}
    <p class="extract-error" role="alert">{$formatCatalogError}</p>
    <button type="button" on:click={() => $backend && void ensureFormatsLoaded($backend, true).catch(() => {})}>Retry loading patterns</button>
  {/if}
  <input type="search" aria-label="Find an extraction pattern" placeholder="Find pattern" bind:value={patternSearch} />
  <div class="pattern-list">
    {#each patterns as entry (entry.definition.id)}
      <button type="button" class="pattern-button" disabled={cannotRun} on:click={() => run(entry.definition)}
        aria-label={entry.definition.name} aria-describedby={`extract-pattern-${entry.definition.id}`}
        title={`Extract ${entry.definition.name} from the selected columns`}>
        <PatternLabel {entry} showExample descriptionId={`extract-pattern-${entry.definition.id}`} />
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M4 4h16v4l-6 6v5l-4 2v-7L4 8Z" /></svg>
      </button>
    {/each}
    {#if !patterns.length && !$formatCatalogError}<p class="extract-help">No matching patterns.</p>{/if}
  </div>
</section>

<style>
  .extract-panel { display: flex; flex: 1; min-height: 0; flex-direction: column; gap: 9px; color: var(--fg); font-size: 11px; }
  .extract-panel > :not(.pattern-list) { flex-shrink: 0; }
  .extract-help { margin: 0; color: var(--muted); line-height: 1.5; }
  .extract-error { margin: 0; color: var(--danger); white-space: pre-wrap; overflow-wrap: anywhere; }
  .extract-source { position: relative; display: flex; align-items: center; gap: 10px; min-width: 0; }
  .scope-setting { display: flex; align-items: center; gap: 7px; flex: 0 1 auto; min-width: 0; }
  .scope-setting select { width: 132px; min-width: 0; }
  .scope-setting select, .column-trigger { box-sizing: border-box; height: 29px; min-height: 29px; }
  button, input, select { min-height: 27px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); padding: 4px 7px; font: inherit; }
  button { cursor: pointer; }
  button:hover:not(:disabled) { background: var(--surface-weak); border-color: var(--accent-border); }
  button:disabled { cursor: default; opacity: .5; }
  input[type=search] { width: 100%; min-width: 0; }
  .column-picker { flex: 1; min-width: 0; }
  .column-trigger { display: flex; align-items: center; justify-content: space-between; gap: 8px; max-width: 100%; }
  .column-trigger > span:first-child { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .column-count { margin-left: 4px; color: var(--muted); }
  .column-menu { position: absolute; top: calc(100% + 5px); right: 0; z-index: 10; width: min(320px, 100%); display: flex; flex-direction: column; gap: 6px; padding: 7px; overflow: auto; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); box-shadow: var(--shadow-soft); }
  .column-menu-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .column-menu-head .column-close { display: inline-flex; align-items: center; justify-content: center; width: 23px; min-height: 23px; padding: 0; background: transparent; border-color: transparent; font-size: 16px; }
  .column-actions { display: flex; gap: 6px; }
  .column-actions button { flex: 1; }
  .column-list { display: flex; flex: 1; flex-direction: column; min-height: 0; overflow: auto; }
  .column-list label { display: flex; align-items: center; gap: 7px; min-height: 25px; }
  .column-list label span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .column-list input { min-height: 0; flex-shrink: 0; }
  .pattern-list { display: flex; flex: 1; min-height: 0; flex-direction: column; gap: 5px; margin-top: 3px; overflow: auto; overscroll-behavior: contain; }
  .pattern-button { flex: 0 0 auto; display: flex; align-items: center; justify-content: space-between; gap: 7px; text-align: left; }
  .pattern-button > svg { flex: 0 0 14px; }
  button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
</style>
