<script lang="ts">
  import { onDestroy } from "svelte";
  import type { Backend } from "../../backend";
  import type { ColumnStats } from "../../types";
  import { filterByValue } from "../../stores/condition";
  import { backend, projectDetail } from "./state";

  let column = "";
  let rareMax: number | null = 1;
  let stats: ColumnStats | null = null;
  let error = "";
  let request = 0;
  let alive = true;

  onDestroy(() => {
    alive = false;
  });

  $: columns = $projectDetail?.columns ?? [];
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: if (projectId && !columns.includes(column)) {
    column = columns[0] ?? "";
  }

  const loadStats = async (
    api: Backend,
    activeProjectId: string,
    activeColumn: string,
    activeRareMax: number | null
  ) => {
    if (!Number.isInteger(activeRareMax) || (activeRareMax ?? 0) < 1) {
      return;
    }
    const ticket = ++request;
    stats = null;
    error = "";
    try {
      const next = await api.columnStats({
        projectId: activeProjectId,
        column: activeColumn,
        rareMax: activeRareMax ?? 1,
      });
      if (!alive || ticket !== request) {
        return;
      }
      stats = next;
    } catch (err) {
      if (!alive || ticket !== request) {
        return;
      }
      error = err instanceof Error ? err.message : String(err);
    }
  };

  $: if ($backend && projectId && column) {
    void loadStats($backend, projectId, column, rareMax);
  }
</script>

<div class="stats-panel">
  <div class="condition-toolbar">
    <span class="filter-label">Column stats</span>
    <select bind:value={column} aria-label="Stats column">
      {#each columns as name}
        <option value={name}>{name}</option>
      {/each}
    </select>
    <label class="condition-count">
      Rare up to
      <input type="number" min="1" step="1" bind:value={rareMax} aria-label="Rare maximum frequency" />
    </label>
  </div>
  {#if error}
    <p class="condition-count">{error}</p>
  {:else if stats}
    {@const columnName = stats.column}
    <p class="condition-count">
      {stats.kind} · missing {stats.missing} · unique {stats.unique}
    </p>
    <div class="condition-toolbar">
      <span class="condition-count">Top</span>
      {#each stats.top as item (item.value)}
        <button type="button" class="ghost" on:click={() => filterByValue(columnName, item.value)}>
          {item.value} ({item.count})
        </button>
      {/each}
    </div>
    <div class="condition-toolbar">
      <span class="condition-count">Rare</span>
      {#each stats.rare as item (item.value)}
        <button type="button" class="ghost" on:click={() => filterByValue(columnName, item.value)}>
          {item.value} ({item.count})
        </button>
      {/each}
    </div>
  {/if}
</div>
