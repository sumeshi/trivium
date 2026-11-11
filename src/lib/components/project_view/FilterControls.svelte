<script lang="ts">
  import { createEventDispatcher, onMount, onDestroy } from 'svelte';
  import {
    visibleColumns,
    hiddenColumns,
    search,
    searchColumns,
    flagFilter,
    bookmarkedOnly,
    isExporting,
    projectDetail,
    backend,
    columnLabels,
    FLAG_FILTER_OPTIONS
  } from './state';
  import type { FlagFilterValue } from './state';
  import ConditionEditor from './ConditionEditor.svelte';
  import StatsPanel from './StatsPanel.svelte';
  import TimePanel from './TimePanel.svelte';
  import PresetControls from './PresetControls.svelte';
  import BulkControls from './BulkControls.svelte';


  let columnsOpen = false;
  let columnPickerEl: HTMLDivElement | null = null;

  let statsOpen = false;
  let timeOpen = false;

  let flagMenuOpen = false;
  let flagPickerEl: HTMLDivElement | null = null;

  const dispatch = createEventDispatcher();

  const getFlagFilterDetails = (value: FlagFilterValue) => {
    return (
      FLAG_FILTER_OPTIONS.find((option) => option.value === value) ?? FLAG_FILTER_OPTIONS[0]
    );
  };

  const toggleFlagMenu = () => {
    flagMenuOpen = !flagMenuOpen;
    if (flagMenuOpen) {
      columnsOpen = false;
    }
  };

  const selectFlagFilter = (value: FlagFilterValue) => {
    flagFilter.set(value);
    flagMenuOpen = false;
  };

  const toggleColumn = async (column: string) => {
    hiddenColumns.update(hidden => {
      const nextHidden = new Set(hidden);
      if (nextHidden.has(column)) {
        nextHidden.delete(column);
      } else {
        nextHidden.add(column);
      }
      return nextHidden;
    });
    
    // Save to backend
    if ($backend && $projectDetail) {
      try {
        await $backend.setHiddenColumns({
          projectId: $projectDetail.project.meta.id,
          hiddenColumns: Array.from($hiddenColumns)
        });
      } catch (error) {
        console.error('Failed to save hidden columns:', error);
      }
    }
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
  });

  onDestroy(() => {
    document.removeEventListener('click', handleClickOutside);
  });
</script>

<section class="filters">
  <div class="filter-row">
    <div class="flag-picker" bind:this={flagPickerEl}>
      <button
        type="button"
        class="flag-trigger"
        class:open={flagMenuOpen}
        on:click={toggleFlagMenu}
        aria-haspopup="listbox"
        aria-expanded={flagMenuOpen}
      >
        {#if getFlagFilterDetails($flagFilter).hint}
          <span class="flag-hint">{getFlagFilterDetails($flagFilter).hint}</span>
        {/if}
        <span class="flag-label">{getFlagFilterDetails($flagFilter).label}</span>
        <svg class="chevron" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
          <path d="m6 9 6 6 6-6" />
        </svg>
      </button>
      {#if flagMenuOpen}
        <div class="flag-menu" role="listbox">
          {#each FLAG_FILTER_OPTIONS as option}
            <button
              type="button"
              class="flag-option"
              class:active={$flagFilter === option.value}
              on:click={() => selectFlagFilter(option.value)}
              role="option"
              aria-selected={$flagFilter === option.value}
            >
              {#if option.hint}
                <span class="flag-hint">{option.hint}</span>
              {/if}
              <span>{option.label}</span>
              {#if $flagFilter === option.value}
                <span class="flag-check" aria-hidden="true">✓</span>
              {/if}
            </button>
          {/each}
        </div>
      {/if}
    </div>

    <button
      type="button"
      class="tool-button bookmark-toggle"
      class:active={$bookmarkedOnly}
      aria-pressed={$bookmarkedOnly}
      title="Show bookmarked rows only"
      on:click={() => bookmarkedOnly.update((value) => !value)}
    >
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="m12 3 2.8 5.7 6.2.9-4.5 4.4 1.1 6.2L12 17.3 6.4 20.2l1.1-6.2L3 9.6l6.2-.9z" />
      </svg>
      <span class="tool-label">Bookmarked</span>
    </button>

    <label class="search-field">
      <svg class="search-icon" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round">
        <circle cx="11" cy="11" r="7" />
        <path d="m20 20-3.5-3.5" />
      </svg>
      <input placeholder="Search rows" bind:value={$search} type="search" />
      <span class="scope-chip" title="Search scope">
        {$searchColumns.length === 0
          ? 'All columns'
          : $searchColumns.map((column) => $columnLabels.get(column) ?? column).join(', ')}
      </span>
    </label>

    <div class="column-picker" bind:this={columnPickerEl}>
      <button
        type="button"
        class="tool-button"
        on:click={() => {
          flagMenuOpen = false;
          columnsOpen = !columnsOpen;
        }}
        aria-expanded={columnsOpen}
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round">
          <rect x="3" y="4" width="18" height="16" rx="2" />
          <path d="M9 4v16m6-16v16" />
        </svg>
        <span class="tool-label">Columns</span>
        <span class="count-chip">{$visibleColumns.length}/{$projectDetail?.columns.length ?? 0}</span>
      </button>
      {#if columnsOpen}
        <div class="column-panel">
          <div class="column-panel-head">
            <span>Visible columns</span>
            <button type="button" class="column-close" on:click={() => (columnsOpen = false)}>Done</button>
          </div>
          <ul>
            {#each $projectDetail?.columns ?? [] as column}
              <li>
                <input
                  type="checkbox"
                  id="column-{column}"
                  checked={!$hiddenColumns.has(column)}
                  on:change={() => toggleColumn(column)}
                />
                <label for="column-{column}">{$columnLabels.get(column) ?? column}</label>
              </li>
            {/each}
          </ul>
        </div>
      {/if}
    </div>

    <button class="tool-button export-button" on:click={() => dispatch('export')} disabled={$isExporting}>
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 4v11m0 0 4-4m-4 4-4-4M4 20h16" />
      </svg>
      <span class="tool-label">{$isExporting ? 'Exporting…' : 'Export'}</span>
    </button>
  </div>

  <div class="advanced-row">
    <details class="filter-advanced">
      <summary>Conditions</summary>
      <ConditionEditor />
    </details>
    <details class="filter-advanced" bind:open={statsOpen}>
      <summary>Column stats</summary>
      <StatsPanel open={statsOpen} />
    </details>
    <details class="filter-advanced" bind:open={timeOpen}>
      <summary>Time analysis</summary>
      <TimePanel open={timeOpen} />
    </details>
    <details class="filter-advanced">
      <summary>Presets</summary>
      <PresetControls />
    </details>
    <details class="filter-advanced">
      <summary>Bulk annotation</summary>
      <BulkControls on:refresh />
    </details>
    <details class="filter-advanced">
      <summary>IOC rules (legacy)</summary>
      <div class="filter-ioc">
        <p class="condition-count">Definitions are kept for reference; rows are not flagged automatically.</p>
        <button type="button" class="ghost" on:click={() => {
          dispatch('iocManagerOpen');
        }}>
          IOC Rules
        </button>
      </div>
    </details>
  </div>
</section>
