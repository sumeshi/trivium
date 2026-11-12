<script lang="ts">
  import { createEventDispatcher, onMount, onDestroy, tick } from 'svelte';
  import { FLAG_FILTER_OPTIONS } from './state';
  import { get } from 'svelte/store';
  import { appendEmptyColumnRule } from '../../appliedFilterSummary';
  import { MAX_GROUP_LEVELS } from '../../grouping';
  import type { FlagFilterSelection } from '../../types';
  import { parseFlagFilter, toggleFlagFilter } from '../../flagFilters';
  import { showToast } from '../../utils/toast';
  import { errorMessage } from '../../errors';
  import ConditionEditor from './ConditionEditor.svelte';
  import SearchControls from './SearchControls.svelte';
  import StatsPanel from './StatsPanel.svelte';
  import GroupingPanel from './GroupingPanel.svelte';
  import TimePanel from './TimePanel.svelte';
  import PresetControls from './PresetControls.svelte';
  import BulkControls from './BulkControls.svelte';
  import TimelinePanel from './TimelinePanel.svelte';
  import { useWorkspace } from "../../workspaceContext";
  const workspace = useWorkspace();
  const {
    visibleColumns, hiddenColumns, flagFilter, bookmarkedOnly, frozenThrough,
    isExporting, projectDetail, backend, columnLabels, conditionTree,
  } = workspace;

  let columnsOpen = false;
  let columnPickerEl: HTMLDivElement | null = null;
  let columnSearch = '';

  const groups = [
    { id: 'filters', label: 'Filters', first: 'conditions' },
    { id: 'analysis', label: 'Analysis', first: 'stats' },
    { id: 'annotations', label: 'Annotations', first: 'bulk' },
  ] as const;
  const tools = [
    { id: 'conditions', label: 'Rules', group: 'filters' },
    { id: 'presets', label: 'Presets', group: 'filters' },
    { id: 'stats', label: 'Column stats', group: 'analysis' },
    { id: 'groups', label: 'Groups', group: 'analysis' },
    { id: 'time', label: 'Time analysis', group: 'analysis' },
    { id: 'timeline', label: 'Timeline', group: 'analysis' },
    { id: 'bulk', label: 'Bulk annotate', group: 'annotations' },
    { id: 'iocs', label: 'IOC rules', group: 'annotations' },
  ] as const;
  type Tool = typeof tools[number]['id'];
  type ToolGroup = typeof groups[number]['id'];
  let activeTool: Tool | null = null;
  let rememberedTools: Partial<Record<ToolGroup, Tool>> = {};
  let drawerEl: HTMLDivElement | null = null;
  let drawerTrigger: HTMLButtonElement | null = null;
  let searchControls: SearchControls | undefined;
  let groupingPanel: GroupingPanel | undefined;
  let groupColumns: string[] = [];
  $: activeGroup = tools.find((tool) => tool.id === activeTool)?.group;
  $: categoryTools = tools.filter((tool) => tool.group === activeGroup);
  $: categoryLabel = groups.find((group) => group.id === activeGroup)?.label ?? 'Tools';

  const closeTool = () => {
    activeTool = null;
    if (drawerTrigger?.isConnected) drawerTrigger.focus();
  };
  const selectTool = async (tool: Tool, focusPanel = false) => {
    activeTool = tool;
    const group = tools.find((entry) => entry.id === tool)!.group;
    rememberedTools[group] = tool;
    flagMenuOpen = columnsOpen = false;
    searchControls?.close();
    if (focusPanel) {
      await tick();
      drawerEl?.querySelector<HTMLButtonElement>('[aria-label="Close analysis panel"]')?.focus();
    }
  };
  const openGroup = (group: typeof groups[number], event: MouseEvent) => {
    if (activeGroup === group.id) { closeTool(); return; }
    drawerTrigger = event.currentTarget as HTMLButtonElement;
    void selectTool(rememberedTools[group.id] ?? group.first, true);
  };
  const moveToolTab = async (event: KeyboardEvent, current: Tool) => {
    const index = categoryTools.findIndex((tool) => tool.id === current);
    const delta = event.key === 'ArrowRight' ? 1 : event.key === 'ArrowLeft' ? -1 : 0;
    if (!delta && event.key !== 'Home' && event.key !== 'End') return;
    event.preventDefault();
    const target = event.key === 'Home' ? 0 : event.key === 'End' ? categoryTools.length - 1
      : (index + delta + categoryTools.length) % categoryTools.length;
    const tool = categoryTools[target];
    await selectTool(tool.id);
    await tick();
    document.getElementById(`tool-tab-${tool.id}`)?.focus();
  };

  export async function openRuleForColumn(columnId: string): Promise<void> {
    if (!$projectDetail?.columns.includes(columnId)) return;
    const result = appendEmptyColumnRule(get(conditionTree), columnId);
    conditionTree.set(result.tree);
    drawerTrigger = document.getElementById('tool-group-filters') as HTMLButtonElement | null;
    await selectTool('conditions');
    await tick();
    drawerEl?.querySelector<HTMLInputElement>(`[data-condition-id="${result.ruleId}"]`)?.focus();
  }
  export async function openGroupingForColumn(columnId: string): Promise<void> {
    if (!$projectDetail?.columns.includes(columnId)) return;
    // Set the requested levels before mounting, so reopening the panel does
    // not start an aggregation for its old levels followed by a second one.
    if (!groupColumns.includes(columnId) && groupColumns.length < MAX_GROUP_LEVELS) {
      groupColumns = [...groupColumns, columnId];
    }
    drawerTrigger = document.getElementById('tool-group-analysis') as HTMLButtonElement | null;
    await selectTool('groups');
    await tick();
    groupingPanel?.groupByColumn(columnId);
  }
  let flagMenuOpen = false;
  let flagPickerEl: HTMLDivElement | null = null;

  const dispatch = createEventDispatcher();

  $: selectedFlags = parseFlagFilter($flagFilter) ?? [];
  $: selectedFlagNames = FLAG_FILTER_OPTIONS
    .filter((option) => selectedFlags.includes(option.value)).map((option) => option.label);
  $: matchingColumns = ($projectDetail?.columns ?? []).filter((column) =>
    ($columnLabels.get(column) ?? column).toLocaleLowerCase().includes(columnSearch.trim().toLocaleLowerCase())
  );

  const toggleFlagMenu = () => {
    flagMenuOpen = !flagMenuOpen;
    if (flagMenuOpen) {
      columnsOpen = false;
      searchControls?.close();
    }
  };

  const toggleFlag = (value: FlagFilterSelection) => {
    flagFilter.update((current) => toggleFlagFilter(current, value));
  };

  const persistColumns = () => {
    // Capture this project's snapshot before another tab can replace stores.
    const service = $backend;
    const projectId = $projectDetail?.project.meta.id;
    const nextHidden = Array.from($hiddenColumns);
    if (!service || !projectId) return;
    // Register immediately; the backend orders saves across tab instances.
    void service.setHiddenColumns({
      projectId, hiddenColumns: nextHidden,
    }).catch((error) => {
      console.error('Failed to save hidden columns:', error);
      showToast(errorMessage(error, 'Failed to save visible columns.'), 'error');
    });
  };

  const toggleColumn = (column: string) => {
    hiddenColumns.update(hidden => {
      const nextHidden = new Set(hidden);
      if (nextHidden.has(column)) {
        nextHidden.delete(column);
      } else {
        nextHidden.add(column);
      }
      return nextHidden;
    });
    persistColumns();
  };

  const setAllColumns = (visible: boolean) => {
    hiddenColumns.set(new Set(visible ? [] : $projectDetail?.columns ?? []));
    persistColumns();
  };

  const handleEscape = (event: KeyboardEvent) => {
    if (event.key !== 'Escape') return;
    if (flagMenuOpen) flagPickerEl?.querySelector('button')?.focus();
    if (columnsOpen) columnPickerEl?.querySelector('button')?.focus();
    flagMenuOpen = false;
    columnsOpen = false;
    searchControls?.close();
    if (activeTool) closeTool();
  };

  const handleClickOutside = (event: MouseEvent) => {
    const target = event.target as HTMLElement;
    
    // Outside click for flag picker
    if (flagMenuOpen && flagPickerEl && !flagPickerEl.contains(target)) {
      flagMenuOpen = false;
    }
    
    // Outside click for column picker
    if (columnsOpen && columnPickerEl && !columnPickerEl.contains(target)) {
      columnsOpen = false;
    }
  };

  onMount(() => {
    document.addEventListener('click', handleClickOutside);
    document.addEventListener('keydown', handleEscape);
  });

  onDestroy(() => {
    document.removeEventListener('click', handleClickOutside);
    document.removeEventListener('keydown', handleEscape);
  });
</script>

<section class="filters">
  <div class="filter-row">
    <div class="flag-picker" bind:this={flagPickerEl}>
      <button
        type="button"
        class="flag-trigger"
        class:open={flagMenuOpen}
        class:filtered={selectedFlags.length > 0}
        on:click={toggleFlagMenu}
        aria-label={`Filter by flag: ${selectedFlagNames.join(', ') || 'All rows'}`}
        aria-pressed={selectedFlags.length > 0}
        title={`Filter by flag: ${selectedFlagNames.join(', ') || 'All rows'}; no selection shows all rows`}
        aria-controls="flag-filter-menu"
        aria-expanded={flagMenuOpen}
      >
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <path d="M5 21V4m0 0c4-3 10 3 14 0v11c-4 3-10-3-14 0" />
        </svg>
        {#if selectedFlags.length > 0}<span class="flag-filter-indicator" aria-hidden="true"></span>{/if}
      </button>
      {#if flagMenuOpen}
        <div class="flag-menu" id="flag-filter-menu" role="group" aria-label="Flag filters">
          {#each FLAG_FILTER_OPTIONS as option}
            <label class="flag-checkbox-option" class:active={selectedFlags.includes(option.value)}>
              <input type="checkbox" checked={selectedFlags.includes(option.value)}
                on:change={() => toggleFlag(option.value)} />
              <span class={`filter-flag-dot filter-flag-${option.value}`} aria-hidden="true"></span>
              <span>{option.label}</span>
            </label>
          {/each}
          <button type="button" class="clear-flag-filter" title="Clear flag selection and show all rows"
            on:click={() => flagFilter.set('all')}>Show all</button>
        </div>
      {/if}
    </div>

    <SearchControls bind:this={searchControls} on:open={() => { flagMenuOpen = columnsOpen = false; }} />

    <button
      type="button"
      class="tool-button bookmark-toggle"
      class:active={$bookmarkedOnly}
      aria-pressed={$bookmarkedOnly}
      aria-label="Show bookmarked rows only"
      title="Show bookmarked rows only"
      on:click={() => bookmarkedOnly.update((value) => !value)}
    >
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="m12 3 2.8 5.7 6.2.9-4.5 4.4 1.1 6.2L12 17.3 6.4 20.2l1.1-6.2L3 9.6l6.2-.9z" />
      </svg>
    </button>

    <div class="column-picker" bind:this={columnPickerEl}>
      <button
        type="button"
        class="tool-button"
        on:click={() => {
          flagMenuOpen = false;
          searchControls?.close();
          columnsOpen = !columnsOpen;
        }}
        aria-expanded={columnsOpen}
        aria-controls="visible-column-menu"
        aria-label="Choose visible columns"
        title="Choose visible columns"
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round">
          <rect x="3" y="4" width="18" height="16" rx="2" />
          <path d="M9 4v16m6-16v16" />
        </svg>
        <span class="tool-label">Columns</span>
        <span class="count-chip">{$visibleColumns.length}/{$projectDetail?.columns.length ?? 0}</span>
      </button>
      {#if columnsOpen}
        <div class="column-panel" id="visible-column-menu">
          <div class="column-panel-head">
            <span>Visible columns</span>
            <button type="button" class="column-close" title="Close visible columns" on:click={() => (columnsOpen = false)}>Done</button>
          </div>
          <div class="column-actions">
            <button type="button" title="Show every data column" on:click={() => setAllColumns(true)}>Select all</button>
            <button type="button" title="Hide every data column" on:click={() => setAllColumns(false)}>Deselect all</button>
          </div>
          <button type="button" class="unfreeze-columns" title="Let every data column scroll horizontally"
            aria-label="Unfreeze data columns" disabled={$frozenThrough === ''}
            on:click={() => frozenThrough.set('')}>Unfreeze data columns</button>
          <input class="column-search" type="search" aria-label="Find a column" placeholder="Find column"
            bind:value={columnSearch} />
          <ul>
            {#each matchingColumns as column}
              <li>
                <input
                  type="checkbox"
                  id="column-{column}"
                  checked={!$hiddenColumns.has(column)}
                  on:change={() => toggleColumn(column)}
                />
                <label for="column-{column}" title={$columnLabels.get(column) ?? column}>{$columnLabels.get(column) ?? column}</label>
              </li>
            {/each}
          </ul>
          {#if matchingColumns.length === 0}<p class="column-empty">No matching columns</p>{/if}
        </div>
      {/if}
    </div>

    <button type="button" class="tool-button export-button" on:click={() => dispatch('export')} disabled={$isExporting}
      aria-label={$isExporting ? 'Exporting rows' : 'Export rows'} title={$isExporting ? 'Exporting rows…' : 'Export rows to CSV'} aria-busy={$isExporting}>
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 4v11m0 0 4-4m-4 4-4-4M4 20h16" />
      </svg>

    </button>
  </div>

  <div class="advanced-row">
    <slot name="navigation" />
    <div class="tool-groups" role="group" aria-label="Investigation tools">
      {#each groups as group}
        <button type="button" class="aux-trigger" class:active={activeGroup === group.id}
          id={`tool-group-${group.id}`} aria-label={`Open ${group.label}`} title={`Open ${group.label}`}
          aria-controls="analysis-drawer" aria-expanded={activeGroup === group.id}
          on:click={(event) => openGroup(group, event)}>{group.label}</button>
      {/each}
    </div>
  </div>

  {#if activeTool}
    <div class="analysis-drawer" id="analysis-drawer" role="dialog" aria-labelledby="analysis-tool-label"
      bind:this={drawerEl} tabindex="-1">
      <div class="analysis-head">
        <h3 id="analysis-tool-label">{categoryLabel}</h3>
        <button type="button" class="analysis-close" aria-label="Close analysis panel"
          title="Close panel" on:click={closeTool}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
            <path d="m6 6 12 12M6 18 18 6" />
          </svg>
        </button>
      </div>
      <div class="analysis-tabs" role="tablist" aria-label={`${categoryLabel} tools`}>
        {#each categoryTools as tool}
          <button type="button" role="tab" id={`tool-tab-${tool.id}`} aria-selected={activeTool === tool.id}
            tabindex={activeTool === tool.id ? 0 : -1} aria-controls="analysis-tool-content"
            on:click={() => void selectTool(tool.id)} on:keydown={(event) => void moveToolTab(event, tool.id)}>{tool.label}</button>
        {/each}
      </div>
      <div class="analysis-body" id="analysis-tool-content" role="tabpanel" aria-labelledby={`tool-tab-${activeTool}`}>
        {#if activeTool === 'conditions'}
          <ConditionEditor />
        {:else if activeTool === 'stats'}
          <StatsPanel open={true} />
        {:else if activeTool === 'groups'}
          <GroupingPanel bind:this={groupingPanel} bind:groupColumns />
        {:else if activeTool === 'time'}
          <TimePanel open={true} />
        {:else if activeTool === 'timeline'}
          <TimelinePanel open={true} />
        {:else if activeTool === 'presets'}
          <PresetControls />
        {:else if activeTool === 'bulk'}
          <BulkControls on:refresh />
        {:else}
          <div class="filter-ioc">
            <p class="condition-count">Save reusable IOC queries, flag labels and tags. Saving a definition does not change row annotations.</p>
            <button type="button" class="ghost" title="Manage saved IOC definitions" on:click={() => {
              closeTool(); dispatch('iocManagerOpen');
            }}>IOC Rules</button>
          </div>
        {/if}
      </div>
    </div>
  {/if}
</section>

<style>
  /* Let the project view contain the overlay; opening a tool never adds a
     vertical block above the table. Individual pickers remain positioned. */
  .filters { position: static; }
  .aux-trigger {
    display: inline-flex;
    align-items: center;
    min-height: 25px;
    padding: 3px 7px;
    border: 1px solid transparent;
    border-radius: 4px;
    background: transparent;
    color: var(--muted);
    font-size: 10.5px;
  }
  .aux-trigger:hover, .aux-trigger.active {
    background: var(--surface-weak);
    border-color: var(--border);
    color: var(--fg);
  }
  .analysis-drawer {
    position: absolute;
    top: 0;
    right: 0;
    bottom: calc(var(--filter-status-height, 26px) + 4px);
    z-index: 35;
    display: flex;
    flex-direction: column;
    width: min(560px, calc(100% - 16px));
    min-height: 0;
    border: 1px solid var(--border);
    background: var(--surface);
    box-shadow: var(--shadow-strong);
  }
  .analysis-head {
    display: flex;
    align-items: center;
    gap: 8px;
    justify-content: space-between;
    min-height: 28px;
    padding: 2px 8px;
    border-bottom: 1px solid var(--border);
    background: var(--surface-strong);
  }
  .analysis-head h3 { margin: 0; color: var(--muted); font-size: 10px; font-weight: 600; letter-spacing: .07em; text-transform: uppercase; cursor: default; }
  .analysis-tabs { display: flex; min-width: 0; gap: 3px; flex-wrap: wrap; padding: 4px 7px; background: var(--surface-weak); border-bottom: 1px solid var(--border); }
  .analysis-tabs button { border: 1px solid transparent; border-radius: 4px; padding: 4px 7px; background: transparent; color: var(--muted); font-size: 11px; }
  .analysis-tabs button[aria-selected='true'] { background: var(--accent-soft); border-color: var(--accent-border); color: var(--fg); }
  .analysis-tabs button:hover { color: var(--fg); background: var(--surface-weak); }
  .tool-groups { display: flex; align-items: center; gap: 3px; margin-left: 4px; padding-left: 9px; border-left: 1px solid var(--border); }
  .export-button { width: 33px; min-width: 33px; padding: 0; justify-content: center; }
  button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }

  .analysis-close {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 27px;
    height: 27px;
    padding: 0;
    border: 1px solid transparent;
    border-radius: 4px;
    color: var(--muted);
    background: transparent;
  }
  .analysis-close:hover { background: var(--surface-weak); color: var(--fg); }
  .analysis-body { flex: 1; min-height: 0; overflow: auto; padding: 10px; }
  .flag-checkbox-option {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 7px;
    border-radius: 4px;
    color: var(--fg);
    cursor: pointer;
    font-size: 12px;
  }
  .flag-checkbox-option:hover, .flag-checkbox-option.active {
    background: var(--surface-weak);
  }
  .flag-checkbox-option input {
    width: 14px;
    height: 14px;
    margin: 0;
    accent-color: var(--accent);
  }
  .filter-flag-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    flex: 0 0 7px;
    background: var(--muted);
  }
  .filter-flag-safe { background: var(--flag-safe-text); }
  .filter-flag-suspicious { background: var(--flag-suspicious-text); }
  .filter-flag-critical { background: var(--flag-critical-text); }
  .clear-flag-filter {
    margin-top: 3px;
    padding: 5px 7px;
    border: 0;
    border-top: 1px solid var(--border);
    border-radius: 0;
    text-align: left;
    background: transparent;
    color: var(--muted);
    font-size: 11px;
  }
  .column-panel {
    max-height: min(440px, 65vh);
  }
  .column-actions {
    display: flex;
    gap: 10px;
    padding: 0 6px 7px;
  }
  .column-actions button {
    padding: 0;
    border: 0;
    border-radius: 0;
    background: transparent;
    color: var(--accent);
    font-size: 11px;
  }
  .column-search {
    width: 100%;
    margin-bottom: 5px;
    min-height: 27px;
  }
  .column-panel ul {
    min-height: 0;
  }
  .column-panel li {
    padding: 3px 6px;
    min-height: 25px;
  }
  .column-panel li label {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .column-empty {
    margin: 6px;
    color: var(--muted);
    font-size: 11px;
  }
  .unfreeze-columns { width: 100%; margin: 0 0 5px; padding: 4px 6px; border: 1px solid var(--border); border-radius: 4px; background: transparent; color: var(--muted); font-size: 10.5px; text-align: left; }
  .unfreeze-columns:hover:not(:disabled) { color: var(--fg); background: var(--surface-weak); }
</style>
