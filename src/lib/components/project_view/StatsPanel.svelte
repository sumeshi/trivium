<script lang="ts">
  import { onDestroy } from "svelte";
  import type { Backend } from "../../backend";
  import type { ColumnStats, ConditionNode, Histogram } from "../../types";
  import { conditionPayload, filterByValue } from "../../stores/condition";
  import { backend, flagFilter, projectDetail, search, visibleColumns } from "./state";
  import type { FlagFilterValue } from "./state";

  let column = "";
  let rareMax: number | null = 1;
  let bucketCount: number | null = 50;
  let stats: ColumnStats | null = null;
  let histogram: Histogram | null = null;
  let error = "";
  let histogramError = "";
  let request = 0;
  let histogramRequest = 0;
  let alive = true;

  onDestroy(() => {
    alive = false;
  });

  $: columns = $projectDetail?.columns ?? [];
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: kind = $projectDetail?.column_kinds?.[column] ?? "";
  $: numeric = kind === "int" || kind === "float" || kind === "datetime";
  $: if (projectId && !columns.includes(column)) {
    column = columns[0] ?? "";
  }
  $: if (!numeric) {
    histogramRequest += 1;
    histogram = null;
    histogramError = "";
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

  const loadHistogram = async (
    api: Backend,
    activeProjectId: string,
    activeColumn: string,
    activeBuckets: number | null,
    activeSearch: string,
    activeFlag: FlagFilterValue,
    activeColumns: string[],
    activeCondition: ConditionNode | null
  ) => {
    if (!Number.isInteger(activeBuckets) || (activeBuckets ?? 0) < 1) {
      return;
    }
    const ticket = ++histogramRequest;
    histogram = null;
    histogramError = "";
    try {
      const next = await api.histogram({
        projectId: activeProjectId,
        column: activeColumn,
        buckets: activeBuckets ?? 50,
        search: activeSearch.trim() || undefined,
        flagFilter: activeFlag,
        columns: activeColumns,
        condition: activeCondition,
      });
      if (!alive || ticket !== histogramRequest) {
        return;
      }
      histogram = next;
    } catch (err) {
      if (!alive || ticket !== histogramRequest) {
        return;
      }
      histogramError = err instanceof Error ? err.message : String(err);
    }
  };

  $: if ($backend && projectId && column) {
    void loadStats($backend, projectId, column, rareMax);
  }

  $: if ($backend && projectId && column && numeric) {
    void loadHistogram(
      $backend,
      projectId,
      column,
      bucketCount,
      $search,
      $flagFilter,
      $visibleColumns,
      $conditionPayload
    );
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
    {#if numeric}
      <label class="condition-count">
        Buckets
        <input type="number" min="1" step="1" bind:value={bucketCount} aria-label="Histogram buckets" />
      </label>
    {/if}
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
  {#if histogramError}
    <p class="condition-count">{histogramError}</p>
  {:else if histogram}
    <div class="condition-toolbar">
      <span class="condition-count">Histogram</span>
      {#each histogram.buckets as bucket, index (`${bucket.start}:${bucket.end}:${index}`)}
        <span class="condition-count">{bucket.start}–{bucket.end} ({bucket.count})</span>
      {/each}
      {#if histogram.missing > 0}
        <span class="condition-count">missing ({histogram.missing})</span>
      {/if}
    </div>
  {/if}
</div>
