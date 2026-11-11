<script lang="ts">
  import { errorMessage } from "../../errors";
  import { onDestroy } from "svelte";
  import { createLatestRequest } from "../../latestRequest";
  import type { Backend } from "../../backend";
  import type { ColumnStats, Histogram } from "../../types";
  import { shouldRequestPanel } from "../../panelRequest";
  import { compactTimestamp } from "../../panelPresentation";
  import ValueCountList from "./ValueCountList.svelte";
  import { annotationVersions, appliedFilters, backend, columnLabels, projectDetail } from "./state";
  import type { AppliedFilters } from "./state";

  export let open = false;

  let column = "";
  let rareMax: number | null = 1;
  let bucketCount: number | null = 50;
  let stats: ColumnStats | null = null;
  let histogram: Histogram | null = null;
  let error = "";
  let histogramError = "";
  let statsLoading = false;
  let histogramLoading = false;
  let lastStatsKey = "";
  let lastHistogramKey = "";
  const statsRequests = createLatestRequest();
  const histogramRequests = createLatestRequest();

  onDestroy(() => {
    statsRequests.dispose();
    histogramRequests.dispose();
  });

  $: columns = $projectDetail?.columns ?? [];
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: annotationRevision = $annotationVersions[projectId] ?? 0;
  $: scope = { projectId, annotationRevision };
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
    activeScope: { projectId: string; annotationRevision: number },
    activeColumn: string,
    activeRareMax: number | null,
    spec: AppliedFilters
  ) => {
    const ticket = statsRequests.next();
    stats = null;
    error = "";
    if (!Number.isSafeInteger(activeRareMax) || (activeRareMax ?? 0) < 1) {
      statsLoading = false;
      error = "Rare frequency must be a positive whole number.";
      return;
    }
    statsLoading = true;
    try {
      const next = await api.columnStats({
        projectId: activeScope.projectId,
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
    } finally {
      if (statsRequests.isCurrent(ticket)) statsLoading = false;
    }
  };

  const loadHistogram = async (
    api: Backend,
    activeScope: { projectId: string; annotationRevision: number },
    activeColumn: string,
    activeBuckets: number | null,
    spec: AppliedFilters
  ) => {
    const ticket = histogramRequests.next();
    histogram = null;
    histogramError = "";
    if (!Number.isInteger(activeBuckets) || (activeBuckets ?? 0) < 1 || (activeBuckets ?? 0) > 200) {
      histogramLoading = false;
      histogramError = "Choose between 1 and 200 histogram buckets.";
      return;
    }
    histogramLoading = true;
    try {
      const next = await api.histogram({
        projectId: activeScope.projectId,
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
    } finally {
      if (histogramRequests.isCurrent(ticket)) histogramLoading = false;
    }
  };

  $: if (!open) {
    // Ignore any response that was already in flight.
    statsRequests.invalidate();
    histogramRequests.invalidate();
    statsLoading = false;
    histogramLoading = false;
    lastStatsKey = "";
    lastHistogramKey = "";
  }

  // Annotation updates can also publish an equivalent applied QuerySpec.
  // One semantic scope should trigger one aggregation, not both notifications.
  $: statsKey = JSON.stringify([scope, column, rareMax, $appliedFilters]);
  $: histogramKey = JSON.stringify([scope, column, bucketCount, $appliedFilters]);
  $: if ($backend && shouldRequestPanel({ open, projectId, column }) && statsKey !== lastStatsKey) {
    lastStatsKey = statsKey;
    void loadStats($backend, scope, column, rareMax, $appliedFilters);
  }

  $: if ($backend && shouldRequestPanel({ open, projectId, column }) && numeric && histogramKey !== lastHistogramKey) {
    lastHistogramKey = histogramKey;
    void loadHistogram(
      $backend,
      scope,
      column,
      bucketCount,
      $appliedFilters
    );
  }
  $: histogramMaximum = Math.max(1, ...(histogram?.buckets ?? []).map(bucket => bucket.count));
  $: histogramTotal = (histogram?.buckets ?? []).reduce((sum, bucket) => sum + bucket.count, 0);
</script>

<div class="stats-panel">
  <div class="stats-controls">
    <label for="stats-column">Column</label>
    <select id="stats-column" bind:value={column} aria-label="Stats column">
      {#each columns as name}
        <option value={name}>{$columnLabels.get(name) ?? name}</option>
      {/each}
    </select>
  </div>
  {#if error}
    <p class="panel-error" role="alert">{error}</p>
  {:else if statsLoading}
    <p class="condition-count" role="status">Loading column stats…</p>
  {:else if stats}
    <p class="stats-summary">{stats.kind} <span>Missing <b>{stats.missing.toLocaleString()}</b></span> <span>Unique <b>{stats.unique.toLocaleString()}</b></span></p>
    <section class="stat-section" aria-label="Most frequent values">
      <h3>Top values <small>Click a value to filter</small></h3>
      <ValueCountList column={stats.column} items={stats.top} />
    </section>
  {/if}
  <details class="rare-section">
      <summary>Rare values <span>{stats?.rare.length ?? 0}</span></summary>
      <label class="rare-control">
        Maximum frequency
        <input type="number" min="1" step="1" bind:value={rareMax} aria-label="Rare maximum frequency" />
      </label>
      {#if stats}<ValueCountList column={stats.column} items={stats.rare} />{/if}
  </details>
  {#if numeric}
    <section class="stat-section histogram-section" aria-label="Column histogram">
      <div class="histogram-heading">
        <h3>Histogram</h3>
        <label>Buckets <input type="number" min="1" max="200" step="1" bind:value={bucketCount} aria-label="Histogram buckets" /></label>
      </div>
      {#if histogramError}
        <p class="panel-error" role="alert">{histogramError}</p>
      {:else if histogramLoading}
        <p class="condition-count" role="status">Loading histogram…</p>
      {:else if histogram && histogram.buckets.length > 0}
        <svg viewBox="0 0 400 115" class="column-histogram" role="img" aria-label={`Histogram: ${histogramTotal.toLocaleString()} values in ${histogram.buckets.length} buckets`}>
          {#each histogram.buckets as bucket, index (`${bucket.start}:${bucket.end}:${index}`)}
            <rect
              x={index * 400 / histogram.buckets.length}
              y={105 - bucket.count / histogramMaximum * 100}
              width={Math.max(0.5, 400 / histogram.buckets.length - 1)}
              height={bucket.count / histogramMaximum * 100}
            ><title>{bucket.start} – {bucket.end}: {bucket.count.toLocaleString()}</title></rect>
          {/each}
          <line x1="0" y1="105" x2="400" y2="105" />
        </svg>
        <div class="histogram-boundaries"><span title={histogram.buckets[0].start}>{compactTimestamp(histogram.buckets[0].start)}</span><span title={histogram.buckets[histogram.buckets.length - 1].end}>{compactTimestamp(histogram.buckets[histogram.buckets.length - 1].end)}</span></div>
        <p class="condition-count">{histogramTotal.toLocaleString()} values · Missing {histogram.missing.toLocaleString()}{histogram.outside ? ` · Outside ${histogram.outside.toLocaleString()}` : ""}</p>
      {:else if histogram}
        <p class="condition-count">No values in this result.</p>
      {/if}
    </section>
  {/if}
</div>

<style>
  .stats-panel { gap: 12px; min-width: 0; }
  .stats-controls { display: flex; align-items: center; gap: 8px; font-size: 11px; color: var(--muted); }
  .stats-controls select { flex: 1; min-width: 0; }
  .stats-summary { display: flex; gap: 14px; flex-wrap: wrap; margin: 0; color: var(--muted); font-size: 11px; }
  .stats-summary b { color: var(--fg); font-weight: 500; font-variant-numeric: tabular-nums; }
  .stat-section { display: grid; gap: 7px; }
  h3 { font-size: 11px; font-weight: 600; color: var(--fg); margin: 0; }
  h3 small { font-size: 10px; font-weight: 400; margin-left: 7px; color: var(--muted); }
  .rare-section { border-top: 1px solid var(--border); padding-top: 9px; }
  summary { cursor: pointer; color: var(--muted); font-size: 11px; }
  summary span { font-variant-numeric: tabular-nums; margin-left: 5px; }
  .rare-control { display: flex; align-items: center; justify-content: space-between; gap: 8px; color: var(--muted); font-size: 11px; margin: 9px 0 7px; }
  .rare-control input { width: 65px; }
  .histogram-heading { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
  .histogram-heading label { display: flex; align-items: center; gap: 6px; color: var(--muted); font-size: 10px; }
  .histogram-heading input { width: 62px; }
  .column-histogram { width: 100%; height: 115px; overflow: visible; }
  .column-histogram rect { fill: var(--accent); opacity: .8; }
  .column-histogram line { stroke: var(--border-strong); stroke-width: 1; }
  .histogram-boundaries { display: flex; justify-content: space-between; gap: 10px; font-family: var(--font-mono); font-size: 9px; color: var(--muted); }
  .histogram-boundaries span { max-width: 48%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .panel-error { color: var(--critical); font-size: 11px; margin: 0; }
</style>
