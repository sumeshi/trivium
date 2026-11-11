<script lang="ts">
  import { errorMessage } from "../../errors";
  import { onDestroy } from "svelte";
  import { createLatestRequest } from "../../latestRequest";
  import type { Backend } from "../../backend";
  import type { ColumnStats, Histogram } from "../../types";
  import { filterByValue } from "../../stores/condition";
  import { shouldRequestPanel } from "../../panelRequest";
  import { appliedFilters, backend, columnLabels, projectDetail } from "./state";
  import type { AppliedFilters } from "./state";

  export let open = false;

  let column = "";
  let rareMax: number | null = 1;
  let bucketCount: number | null = 50;
  let stats: ColumnStats | null = null;
  let histogram: Histogram | null = null;
  let error = "";
  let histogramError = "";
  const statsRequests = createLatestRequest();
  const histogramRequests = createLatestRequest();

  onDestroy(() => {
    statsRequests.dispose();
    histogramRequests.dispose();
  });

  $: columns = $projectDetail?.columns ?? [];
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: kind = $projectDetail?.column_kinds?.[column] ?? "";
  $: numeric = kind === "int" || kind === "float" || kind === "datetime";
  $: if (projectId && !columns.includes(column)) {
    column = columns[0] ?? "";
  }
  $: if (!numeric) {
    histogramRequests.invalidate();
    histogram = null;
    histogramError = "";
  }

  const loadStats = async (
    api: Backend,
    activeProjectId: string,
    activeColumn: string,
    activeRareMax: number | null,
    spec: AppliedFilters
  ) => {
    if (!Number.isInteger(activeRareMax) || (activeRareMax ?? 0) < 1) {
      return;
    }
    const ticket = statsRequests.next();
    stats = null;
    error = "";
    try {
      const next = await api.columnStats({
        projectId: activeProjectId,
        column: activeColumn,
        rareMax: activeRareMax ?? 1,
        search: spec.search.trim(),
        searchColumns: [...spec.searchColumns],
        flagFilter: spec.flag,
        bookmarkedOnly: spec.bookmarkedOnly,
        condition: spec.condition,
        sortKey: null,
        sortDirection: "asc",
      });
      if (!statsRequests.isCurrent(ticket)) {
        return;
      }
      stats = next;
    } catch (err) {
      if (!statsRequests.isCurrent(ticket)) {
        return;
      }
      error = errorMessage(err);
    }
  };

  const loadHistogram = async (
    api: Backend,
    activeProjectId: string,
    activeColumn: string,
    activeBuckets: number | null,
    spec: AppliedFilters
  ) => {
    if (!Number.isInteger(activeBuckets) || (activeBuckets ?? 0) < 1) {
      return;
    }
    const ticket = histogramRequests.next();
    histogram = null;
    histogramError = "";
    try {
      const next = await api.histogram({
        projectId: activeProjectId,
        column: activeColumn,
        buckets: activeBuckets ?? 50,
        search: spec.search.trim() || undefined,
        searchColumns: [...spec.searchColumns],
        flagFilter: spec.flag,
        bookmarkedOnly: spec.bookmarkedOnly,
        condition: spec.condition,
      });
      if (!histogramRequests.isCurrent(ticket)) {
        return;
      }
      histogram = next;
    } catch (err) {
      if (!histogramRequests.isCurrent(ticket)) {
        return;
      }
      histogramError = errorMessage(err);
    }
  };

  $: if (!open) {
    // Ignore any response that was already in flight.
    statsRequests.invalidate();
    histogramRequests.invalidate();
  }

  $: if ($backend && shouldRequestPanel({ open, projectId, column })) {
    void loadStats($backend, projectId, column, rareMax, $appliedFilters);
  }

  $: if ($backend && shouldRequestPanel({ open, projectId, column }) && numeric) {
    void loadHistogram(
      $backend,
      projectId,
      column,
      bucketCount,
      $appliedFilters
    );
  }
</script>

<div class="stats-panel">
  <div class="condition-toolbar">
    <span class="filter-label">Column stats</span>
    <select bind:value={column} aria-label="Stats column">
      {#each columns as name}
        <option value={name}>{$columnLabels.get(name) ?? name}</option>
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
