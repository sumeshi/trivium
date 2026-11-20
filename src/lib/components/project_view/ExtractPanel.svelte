<script lang="ts">
  import { createEventDispatcher } from 'svelte';
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
  $: columns = $projectDetail?.columns ?? [];
  $: selected = selectedColumns === null ? columns : columns.filter(column => selectedColumns?.includes(column));
  $: matchingColumns = columns.filter(column => ($columnLabels.get(column) ?? column).toLocaleLowerCase().includes(columnSearch.trim().toLocaleLowerCase()));
  $: patterns = $formats.filter(entry => entry.definition.name.toLocaleLowerCase().includes(patternSearch.trim().toLocaleLowerCase()));
  $: if (open && $backend) void ensureFormatsLoaded($backend).catch(() => {});
  $: cannotRun = !selected.length || Boolean(pending) || (scope === 'filtered' && !$queryCountKnown);

  const toggleColumn = (column: string) => {
    selectedColumns = selected.includes(column) ? selected.filter(id => id !== column) : [...selected, column];
  };
  const run = (format: FormatDefinition) => {
    const detail = get(projectDetail);
    if (!detail || cannotRun) return;
    const filters = get(appliedFilters);
    const all = scope === 'all';
    dispatch('extract', structuredClone({
      sourceName: detail.project.meta.source_file_name || detail.project.meta.name,
      request: { projectId: detail.project.meta.id, columns: selected, format, scope,
        search: all ? '' : filters.search, searchColumns: all ? [] : filters.searchColumns,
        flagFilter: all ? 'all' : filters.flag, bookmarkedOnly: !all && filters.bookmarkedOnly,
        condition: all ? null : filters.condition, sortKey: null, sortDirection: 'asc' },
    }));
  };
</script>

<section class="extract-panel" aria-label="Extract values">
  <p class="extract-help">Extract matching values into a background tab, grouped by value.<br />
    <strong>Count</strong>: occurrences. <strong>Rows</strong>: source rows containing the value.</p>
  <label class="scope-setting">Rows
    <select aria-label="Extraction rows" bind:value={scope} disabled={Boolean(pending)}>
      <option value="filtered">Current results{$queryCountKnown ? ` (${$totalFilteredRows.toLocaleString()})` : ''}</option>
      <option value="all">All rows ({($projectDetail?.project.meta.total_records ?? 0).toLocaleString()})</option>
    </select>
  </label>
  <div class="column-picker">
    <button type="button" class="column-trigger" aria-label="Choose extraction columns" aria-expanded={columnMenuOpen} disabled={Boolean(pending)}
      aria-controls="extraction-column-list" on:click={() => columnMenuOpen = !columnMenuOpen}>
      <span>Columns: {selected.length === columns.length ? 'All columns' : `${selected.length} selected`}</span><span aria-hidden="true">▾</span>
    </button>
    {#if columnMenuOpen}
      <div id="extraction-column-list" class="column-menu" role="group" aria-label="Extraction columns">
        <div class="column-actions">
          <button type="button" disabled={Boolean(pending)} on:click={() => selectedColumns = null}>Select all</button>
          <button type="button" disabled={Boolean(pending)} on:click={() => selectedColumns = []}>Deselect all</button>
        </div>
        <input type="search" aria-label="Find an extraction column" placeholder="Find column" bind:value={columnSearch} />
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
  .extract-panel { display: flex; flex-direction: column; gap: 9px; color: var(--fg); font-size: 11px; }
  .extract-help { margin: 0; color: var(--muted); line-height: 1.5; }
  .extract-error { margin: 0; color: var(--danger); white-space: pre-wrap; overflow-wrap: anywhere; }
  .scope-setting { display: flex; align-items: center; gap: 8px; }
  .scope-setting select { flex: 1; min-width: 0; }
  button, input, select { min-height: 27px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); padding: 4px 7px; font: inherit; }
  button { cursor: pointer; }
  button:hover:not(:disabled) { background: var(--surface-weak); border-color: var(--accent-border); }
  button:disabled { cursor: default; opacity: .5; }
  input[type=search] { width: 100%; min-width: 0; }
  .column-trigger { display: flex; justify-content: space-between; width: 100%; }
  .column-menu { display: flex; flex-direction: column; gap: 6px; padding: 7px; margin-top: 5px; border: 1px solid var(--border); border-radius: 4px; }
  .column-actions { display: flex; gap: 6px; }
  .column-actions button { flex: 1; }
  .column-list { display: flex; flex-direction: column; max-height: 220px; overflow: auto; }
  .column-list label { display: flex; align-items: center; gap: 7px; min-height: 25px; }
  .column-list label span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .column-list input { min-height: 0; flex-shrink: 0; }
  .pattern-list { display: flex; flex-direction: column; gap: 5px; max-height: 300px; overflow: auto; }
  .pattern-button { flex: 0 0 auto; display: flex; align-items: center; justify-content: space-between; gap: 7px; text-align: left; }
  .pattern-button > svg { flex: 0 0 14px; }
  button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
</style>
