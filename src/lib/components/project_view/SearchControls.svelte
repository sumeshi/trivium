<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { useWorkspace } from '../../workspaceContext';
  import { SEARCH_EXAMPLES, SEARCH_SYNTAX_HELP } from '../../searchHelp';
  const { search, searchColumns, projectDetail, columnLabels } = useWorkspace();
  const dispatch = createEventDispatcher<{ open: void }>();
  let searchToolsEl: HTMLDivElement | null = null;
  let scopeOpen = false;
  let helpOpen = false;
  let scopeSearch = '';
  let scopeTrigger: HTMLButtonElement | null = null;
  let helpTrigger: HTMLButtonElement | null = null;
  $: scopeColumns = ($projectDetail?.columns ?? []).filter((column) =>
    ($columnLabels.get(column) ?? column).toLocaleLowerCase().includes(scopeSearch.trim().toLocaleLowerCase()));
  $: selectedNames = $searchColumns.map((column) => $columnLabels.get(column) ?? column);
  $: scopeLabel = selectedNames.length === 0 ? 'All columns' : selectedNames.length === 1
    ? selectedNames[0] : `${selectedNames.length} columns`;
  $: scopeTitle = `Search columns: ${selectedNames.length ? selectedNames.join(', ') : 'all data columns'}.`;
  export function close(): void { scopeOpen = helpOpen = false; }
  const toggleSearchColumn = (column: string) => {
    searchColumns.update((current) => current.includes(column)
      ? current.filter((item) => item !== column) : [...current, column]);
  };
  const openSearchPopover = (kind: 'scope' | 'help') => {
    if (kind === 'scope') { scopeOpen = !scopeOpen; helpOpen = false; }
    else { helpOpen = !helpOpen; scopeOpen = false; }
    if (scopeOpen || helpOpen) dispatch('open');
  };
  const outside = (event: MouseEvent) => {
    if ((scopeOpen || helpOpen) && !searchToolsEl?.contains(event.target as Node)) close();
  };
  const escape = (event: KeyboardEvent) => {
    if (event.key !== 'Escape' || (!scopeOpen && !helpOpen)) return;
    (scopeOpen ? scopeTrigger : helpTrigger)?.focus();
    close();
  };
</script>

<svelte:document on:click={outside} on:keydown={escape} />

<div class="search-controls" bind:this={searchToolsEl}>
  <div class="search-field" role="search" aria-label="Search table rows">
    <svg class="search-icon" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true">
      <circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" />
    </svg>
    <input placeholder='Search: error timeout · error | warning · -noise' aria-label="Search rows"
      title={SEARCH_SYNTAX_HELP}
      bind:value={$search} type="search" />
    <button type="button" class="scope-chip scope-trigger" bind:this={scopeTrigger} aria-label="Choose search columns"
      aria-controls="search-column-menu" aria-expanded={scopeOpen} class:active={scopeOpen}
      title={scopeTitle}
      on:click={() => openSearchPopover('scope')}>
      <span class="scope-current">Search in: {scopeLabel}</span>
      <span aria-hidden="true">▾</span>
    </button>
    <button type="button" class="search-help-trigger" bind:this={helpTrigger} aria-label="Search syntax help" title="Search syntax and examples"
      aria-controls="search-syntax-help" aria-expanded={helpOpen} on:click={() => openSearchPopover('help')}>?</button>
  </div>
  {#if scopeOpen}
    <div class="search-popover scope-panel" id="search-column-menu" role="group" aria-label="Search columns">
      <div class="search-popover-head"><strong>Search columns</strong><button type="button" on:click={() => (scopeOpen = false)}>Done</button></div>
      <p>Search selected columns. <code>column:value</code> targets a named column.</p>
      <button type="button" class="all-search-columns" class:active={$searchColumns.length === 0}
        on:click={() => searchColumns.set([])}>All data columns</button>
      <input type="search" class="column-search" aria-label="Find a search column" placeholder="Find column" bind:value={scopeSearch} />
      <div class="search-column-list">
        {#each scopeColumns as column}
          <label title={$columnLabels.get(column) ?? column}>
            <input type="checkbox" checked={$searchColumns.includes(column)} on:change={() => toggleSearchColumn(column)} />
            <span>{$columnLabels.get(column) ?? column}</span>
          </label>
        {/each}
        {#if scopeColumns.length === 0}<p>No matching columns</p>{/if}
      </div>
      <p class="scope-footnote">No selected columns means all data columns.</p>
    </div>
  {:else if helpOpen}
    <div class="search-popover search-help" id="search-syntax-help" role="note" aria-label="Search syntax">
      <div class="search-popover-head"><strong>Search syntax</strong><button type="button" on:click={() => (helpOpen = false)}>Done</button></div>
      <p>Case-insensitive text search.</p>
      <dl>
        {#each SEARCH_EXAMPLES as [query, meaning]}
          <dt><code>{query}</code></dt><dd>{meaning}</dd>
        {/each}
      </dl>
      <p>Quote literal colons: <code>"https://example.test"</code>.</p>
      <p>For grouped AND/OR, comparisons, dates or regular expressions, use <strong>Filter → Rules</strong>.</p>
    </div>
  {/if}
</div>

<style>
  .search-controls { position: relative; flex: 1 1 240px; min-width: 240px; }
  .search-field { width: 100%; }
  .scope-trigger { display: inline-flex; align-items: center; gap: 5px; border: 0; cursor: pointer; color: var(--muted); white-space: nowrap; }
  .scope-current { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
  .scope-trigger > span:last-child { flex-shrink: 0; }
  .scope-trigger:hover, .scope-trigger.active { color: var(--fg); background: var(--surface-weak); }
  .search-help-trigger { display: inline-flex; align-items: center; justify-content: center; flex: 0 0 23px; width: 23px; height: 23px; border: 1px solid var(--border); border-radius: 50%; padding: 0; background: transparent; color: var(--muted); font-size: 11px; margin-right: 5px; }
  .search-help-trigger:hover { color: var(--fg); border-color: var(--accent-border); }
  .search-popover { position: absolute; top: calc(100% + 5px); right: 0; width: min(430px, calc(100vw - 30px)); z-index: 45; padding: 9px; border: 1px solid var(--border-strong); border-radius: 5px; background: var(--surface); box-shadow: var(--shadow-soft); color: var(--fg); font-size: 11px; }
  .search-popover-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 5px; }
  .search-popover-head button { border: 0; padding: 2px 4px; background: transparent; color: var(--accent); font-size: 11px; }
  .search-popover p { margin: 5px 0; color: var(--muted); line-height: 1.5; }
  .search-popover code { font-family: var(--font-mono); color: var(--fg); }
  .search-help dl { display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 5px 9px; margin: 8px 0; }
  .search-help dt, .search-help dd { margin: 0; }
  .search-help dd { color: var(--muted); }
  .all-search-columns { width: 100%; padding: 5px 7px; text-align: left; border: 1px solid var(--border); border-radius: 4px; background: transparent; color: var(--fg); font-size: 11px; margin: 4px 0 7px; }
  .all-search-columns.active { border-color: var(--accent-border); background: var(--accent-soft); }
  .search-column-list { display: flex; flex-direction: column; max-height: min(300px, 40vh); overflow: auto; }
  .search-column-list label { display: flex; align-items: center; gap: 7px; min-height: 25px; padding: 3px 5px; color: var(--fg); font-size: 11px; }
  .search-column-list label:hover { background: var(--surface-weak); }
  .search-column-list input { accent-color: var(--accent); flex-shrink: 0; }
  .search-column-list span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .scope-footnote { font-size: 10px; }
  button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
</style>
