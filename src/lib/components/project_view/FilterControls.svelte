<script lang="ts">
  import { createEventDispatcher, onMount, onDestroy } from 'svelte';
  import {
    visibleColumns,
    hiddenColumns,
    search,
    flagFilter,
    isExporting,
    isUpdatingColumns,
    projectDetail,
    FLAG_FILTER_OPTIONS
  } from './state';
  import type { FlagFilterValue } from './state';


  let columnsOpen = false;
  let columnPickerEl: HTMLDivElement | null = null;

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
  };

  const handleClickOutside = (event: MouseEvent) => {
    const target = event.target as HTMLElement;
    
    // Flag pickerの外部クリック
    if (flagMenuOpen && flagPickerEl && !flagPickerEl.contains(target)) {
      flagMenuOpen = false;
    }
    
    // Column pickerの外部クリック
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
  <div class="filter-flag">
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
                <span class="flag-dot" aria-hidden="true">●</span>
              {/if}
            </button>
          {/each}
        </div>
      {/if}
    </div>
  </div>
  <label class="filter-search">
    <input
      placeholder="Enter search text"
      bind:value={$search}
      type="search"
    />
  </label>
  <div class="filter-columns">
    <div class="column-picker" bind:this={columnPickerEl}>
      <button
        type="button"
        class="column-trigger"
        on:click={() => {
          flagMenuOpen = false;
          columnsOpen = !columnsOpen;
        }}
        aria-expanded={columnsOpen}
      >
        Columns ({$visibleColumns.length}/{$projectDetail.columns.length})
      </button>
      {#if columnsOpen}
        <div class="column-panel">
          <ul>
            {#each $projectDetail.columns as column}
              <li>
                <input
                  type="checkbox"
                  id="column-{column}"
                  checked={!$hiddenColumns.has(column)}
                  disabled={$isUpdatingColumns}
                  on:change={() => toggleColumn(column)}
                />
                <label for="column-{column}">{column}</label>
              </li>
            {/each}
          </ul>
          <button type="button" class="close-panel" on:click={() => (columnsOpen = false)}>
            Done
          </button>
        </div>
      {/if}
    </div>
  </div>
  <div class="filter-ioc">
    <button type="button" class="ghost" on:click={() => {
      console.log('IOC Rules button clicked');
      dispatch('iocManagerOpen');
    }}>
      IOC Rules
    </button>
  </div>
  <div class="filter-export">
    <button class="primary" on:click={() => dispatch('export')} disabled={$isExporting}>
      {$isExporting ? 'Exporting…' : 'Export CSV'}
    </button>
  </div>
</section>
