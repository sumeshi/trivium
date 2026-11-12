<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { describeAppliedFilters } from '../../appliedFilterSummary';
  import { useWorkspace } from '../../workspaceContext';
  const { appliedFilters, appliedResultLoading, columnLabels, search, searchColumns,
    flagFilter, bookmarkedOnly, conditionTree } = useWorkspace();
  const dispatch = createEventDispatcher<{ clear: void }>();
  let expanded = false;
  $: description = describeAppliedFilters($appliedFilters, $columnLabels);
  $: canClear = description.active || $search !== '' || $searchColumns.length > 0 ||
    $flagFilter !== 'all' || $bookmarkedOnly || $conditionTree.children.length > 0;
</script>

<footer class="applied-filter-status" class:has-filters={description.active} aria-label="Applied filters">
  <div class="filter-status-line">
    <span class="filter-status-state">{$appliedResultLoading ? 'Applying…' : description.active ? 'Filtered' : 'All rows'}</span>
    {#if description.active}
      <button type="button" class="filter-summary" title={description.text}
        aria-label="Show applied filter details" aria-expanded={expanded} aria-controls="applied-filter-details"
        on:click={() => (expanded = !expanded)}>
        <span>{description.text}</span><span class="detail-toggle" aria-hidden="true">{expanded ? '▾' : '▸'}</span>
      </button>
    {:else}
      <span class="filter-summary-empty">No filters applied</span>
    {/if}
    <button type="button" class="clear-filters" aria-label="Clear all filters"
      title="Clear search, search columns, rules, flags and bookmark filters"
      disabled={!canClear} on:click={() => { expanded = false; dispatch('clear'); }}>Clear filters</button>
  </div>
  {#if expanded && description.active}
    <dl id="applied-filter-details">
      {#each description.filters as filter}
        <dt>{filter.label}</dt><dd>{filter.value}</dd>
      {/each}
    </dl>
  {/if}
</footer>

<style>
  .applied-filter-status { flex: 0 0 auto; min-width: 0; border-top: 1px solid var(--border); color: var(--muted); font-size: 10.5px; }
  .filter-status-line { display: flex; align-items: center; gap: 8px; min-height: 25px; padding: 2px 3px; }
  .filter-status-state { flex: 0 0 auto; font-weight: 600; }
  .has-filters .filter-status-state { color: var(--accent); }
  .filter-summary { display: flex; align-items: center; gap: 5px; min-width: 0; flex: 1; border: 0; padding: 0; background: transparent; color: var(--fg); text-align: left; font-size: inherit; }
  .filter-summary > span:first-child { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .detail-toggle { flex-shrink: 0; color: var(--muted); }
  .filter-summary-empty { flex: 1; }
  .clear-filters { flex-shrink: 0; padding: 2px 5px; border: 1px solid transparent; border-radius: 4px; background: transparent; color: var(--fg); font-size: inherit; }
  .clear-filters:hover:not(:disabled), .filter-summary:hover { color: var(--accent); }
  .clear-filters:disabled { opacity: .45; cursor: default; }
  button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
  dl { display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 4px 10px; margin: 0; max-height: min(160px, 25vh); overflow: auto; padding: 5px 4px 8px; border-top: 1px solid var(--border); }
  dt { color: var(--muted); }
  dd { margin: 0; color: var(--fg); white-space: pre-wrap; overflow-wrap: anywhere; font-family: var(--font-mono); }
</style>
