<script lang="ts">
  import { errorMessage } from "../../errors";
  import { onDestroy } from "svelte";
  import { createLatestRequest } from "../../latestRequest";
  import type { Backend } from "../../backend";
  import type { Histogram, HistogramBucket } from "../../types";
  import { formatTimelineNanos, panTimelineRange, parseTimelineNanos, shortTimelineLabel, zoomTimelineRange } from "../../timelineRange";
  import type { TimelineRange } from "../../timelineRange";
  import { requestTimelineHistogram } from "../../timelineRequests";
  import { filterByRange } from "../../stores/condition";
  import { shouldRequestPanel } from "../../panelRequest";
  import { annotationVersions, appliedFilters, backend, columnLabels, projectDetail } from "./state";
  import type { AppliedFilters } from "./state";

  export let open = false;

  const BUCKET_COUNT = 60;
  let plotWidth = 1000;
  $: viewWidth = Math.max(1, plotWidth);
  const VIEW_HEIGHT = 170;
  const PLOT_TOP = 10;
  const PLOT_BOTTOM = 130;
  const FLAG_STACK = ["unflagged", "safe", "suspicious", "critical"] as const;

  let column = "";
  let zoomRange: { min: string; max: string } | null = null;
  let histogram: Histogram | null = null;
  let autoFocus = true;
  let loading = false;
  let lastRequestKey = "";
  let requestTimer: ReturnType<typeof setTimeout> | null = null;
  let queued: Parameters<typeof loadHistogram> | null = null;
  let inFlight = false;
  let destroyed = false;
  let error = "";
  const requests = createLatestRequest();
  let dragging = false;
  let dragStart: number | null = null;
  let dragEnd: number | null = null;
  let svgEl: SVGSVGElement | null = null;
  let lastResetKey = "";

  onDestroy(() => {
    destroyed = true;
    if (requestTimer !== null) clearTimeout(requestTimer);
    requestTimer = null;
    queued = null;
    requests.dispose();
  });

  const visibleRange = (source: Histogram | null): TimelineRange | null => {
    const first = source?.buckets[0], last = source?.buckets.at(-1);
    return first?.start && last?.end ? { min: first.start, max: last.end } : null;
  };

  const zoom = (factor: number) => {
    const current = zoomRange ?? visibleRange(histogram);
    const full = histogram?.full_range;
    if (current && full) zoomRange = zoomTimelineRange(current, full, factor);
    autoFocus = false;
  };

  const pan = (direction: -1 | 1) => {
    const current = zoomRange ?? visibleRange(histogram);
    const full = histogram?.full_range;
    if (current && full) zoomRange = panTimelineRange(current, full, direction);
    autoFocus = false;
  };

  const showAll = () => {
    zoomRange = null;
    autoFocus = false;
  };

  const selectBuckets = (low: number, high: number) => {
    const first = histogram?.buckets[low], last = histogram?.buckets[high];
    if (!first?.start || !last?.end || !column) return;
    const min = parseTimelineNanos(first.start), rawEnd = parseTimelineNanos(last.end);
    if (min === null || rawEnd === null) return;
    const max = rawEnd - (last.end_inclusive ? 0n : 1n);
    if (max >= min) filterByRange(column, first.start, formatTimelineNanos(max));
  };

  const bucketIndexAt = (event: MouseEvent): number | null => {
    if (!svgEl || buckets.length === 0) {
      return null;
    }
    const matrix = svgEl.getScreenCTM();
    if (!matrix) {
      return null;
    }
    const point = svgEl.createSVGPoint();
    point.x = event.clientX;
    point.y = event.clientY;
    const local = point.matrixTransform(matrix.inverse());
    const ratio = local.x / viewWidth;
    return Math.min(buckets.length - 1, Math.max(0, Math.floor(ratio * buckets.length)));
  };

  const startDrag = (event: MouseEvent) => {
    if (event.button !== 0) {
      return;
    }
    const index = bucketIndexAt(event);
    if (index === null) {
      return;
    }
    dragging = true;
    dragStart = index;
    dragEnd = index;
    event.preventDefault();
  };

  const moveDrag = (event: MouseEvent) => {
    if (!dragging) {
      return;
    }
    const index = bucketIndexAt(event);
    if (index === null) {
      return;
    }
    dragEnd = index;
    event.preventDefault();
  };

  const endDrag = () => {
    if (!dragging) {
      return;
    }
    dragging = false;
    if (dragStart === null || dragEnd === null || !histogram || !column) {
      dragStart = null;
      dragEnd = null;
      return;
    }
    const low = Math.min(dragStart, dragEnd);
    const high = Math.max(dragStart, dragEnd);
    dragStart = null;
    dragEnd = null;
    selectBuckets(low, high);
  };

  const handleWheel = (event: WheelEvent) => {
    if (!canZoom || event.deltaY === 0) return;
    event.preventDefault();
    zoom(event.deltaY < 0 ? 0.8 : 1.2);
  };

  const bucketTitle = (bucket: HistogramBucket) =>
    `[${bucket.start}, ${bucket.end}${bucket.end_inclusive ? "]" : ")"}\n` +
    `Total: ${bucket.count} · Safe: ${bucket.flags.safe} · Suspicious: ${bucket.flags.suspicious} · Critical: ${bucket.flags.critical} · Unflagged: ${bucket.flags.unflagged}`;

  const loadHistogram = async (
    api: Backend,
    activeProjectId: string,
    activeColumn: string,
    activeRange: { min: string; max: string } | null,
    spec: AppliedFilters,
    focus: boolean
  ) => {
    const ticket = requests.next();
    error = "";
    loading = true;
    try {
      const next = await requestTimelineHistogram(
        () => !destroyed && open && requests.isCurrent(ticket),
        () => api.histogram({
        projectId: activeProjectId,
        column: activeColumn,
        buckets: BUCKET_COUNT,
        range: activeRange,
        focus,
        search: spec.search.trim() || undefined,
        searchColumns: [...spec.searchColumns],
        flagFilter: spec.flag,
        bookmarkedOnly: spec.bookmarkedOnly,
        condition: spec.condition,
      }));
      if (!next || !requests.isCurrent(ticket)) {
        return;
      }
      histogram = next;
    } catch (err) {
      if (!requests.isCurrent(ticket)) {
        return;
      }
      error = errorMessage(err);
    } finally {
      if (requests.isCurrent(ticket)) loading = false;
    }
  };

  const runQueued = async () => {
    if (inFlight || !queued || requestTimer !== null || !open || destroyed) return;
    const args = queued;
    queued = null;
    inFlight = true;
    try {
      await loadHistogram(...args);
    } finally {
      inFlight = false;
      if (queued && requestTimer === null && open && !destroyed) void runQueued();
    }
  };

  const scheduleHistogram = (...args: Parameters<typeof loadHistogram>) => {
    requests.invalidate();
    if (requestTimer !== null) clearTimeout(requestTimer);
    const spec = args[4];
    queued = [args[0], args[1], args[2], args[3] ? { ...args[3] } : null,
      { ...spec, searchColumns: [...spec.searchColumns], condition: structuredClone(spec.condition) }, args[5]];
    loading = true;
    requestTimer = setTimeout(() => {
      requestTimer = null;
      void runQueued();
    }, 120);
  };

  $: timeColumns = ($projectDetail?.columns ?? []).filter(
    (name) => $projectDetail?.column_kinds?.[name] === "datetime"
  );
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: if (projectId && !timeColumns.includes(column)) {
    column = timeColumns[0] ?? "";
  }
  $: filtersKey = `${$appliedFilters.search}::${$appliedFilters.flag}::${$appliedFilters.searchColumns.join("|")}::${$appliedFilters.bookmarkedOnly}::${JSON.stringify($appliedFilters.condition)}`;
  $: resetKey = `${projectId}::${column}::${filtersKey}`;
  $: if (resetKey !== lastResetKey) {
    lastResetKey = resetKey;
    zoomRange = null;
    histogram = null;
    autoFocus = true;
    requests.invalidate();
    error = "";
    dragStart = null;
    dragEnd = null;
    dragging = false;
  }

  $: if (!open) {
    // Ignore any response that was already in flight.
    requests.invalidate();
    if (requestTimer !== null) clearTimeout(requestTimer);
    requestTimer = null;
    queued = null;
    loading = false;
    lastRequestKey = "";
  }

  $: requestKey = `${resetKey}::${JSON.stringify(zoomRange)}::${autoFocus}::${$annotationVersions[projectId] ?? 0}`;
  $: if ($backend && shouldRequestPanel({ open, projectId, column }) && requestKey !== lastRequestKey) {
    lastRequestKey = requestKey;
    scheduleHistogram($backend, projectId, column, zoomRange, $appliedFilters, autoFocus);
  }

  $: buckets = histogram?.buckets ?? [];
  $: maxCount = buckets.reduce((max, bucket) => Math.max(max, bucket.count), 0);
  $: bucketTotal = buckets.reduce((sum, bucket) => sum + bucket.count, 0);
  $: totalRows = bucketTotal + (histogram?.outside ?? 0) + (histogram?.missing ?? 0);
  $: currentRange = zoomRange ?? visibleRange(histogram);
  $: fullRange = histogram?.full_range;
  $: fullMin = fullRange ? parseTimelineNanos(fullRange.min) : null;
  $: fullMax = fullRange ? parseTimelineNanos(fullRange.max) : null;
  $: currentMin = currentRange ? parseTimelineNanos(currentRange.min) : null;
  $: currentMax = currentRange ? parseTimelineNanos(currentRange.max) : null;
  $: canZoom = fullMin !== null && fullMax !== null && fullMax > fullMin;
  $: atFullRange = currentMin === fullMin && currentMax === fullMax;
  $: canPanLeft = currentMin !== null && fullMin !== null && currentMin > fullMin;
  $: canPanRight = currentMax !== null && fullMax !== null && currentMax < fullMax;
  $: slotWidth = viewWidth / Math.max(buckets.length, 1);
  $: selection =
    dragStart !== null && dragEnd !== null
      ? {
          start: Math.min(dragStart, dragEnd),
          end: Math.max(dragStart, dragEnd),
        }
      : null;
  $: axisTicks =
    buckets.length === 0
      ? []
      : (viewWidth < 680 ? [0, buckets.length - 1] : viewWidth < 950
          ? [0, Math.round(buckets.length / 2), buckets.length - 1]
          : [0, Math.round(buckets.length / 4), Math.round(buckets.length / 2), Math.round((buckets.length * 3) / 4), buckets.length - 1])
          .filter((value, index, list) => list.indexOf(value) === index)
          .map((index) => ({
            index,
            label:
              index === buckets.length - 1
                ? shortTimelineLabel(buckets[index].end)
                : shortTimelineLabel(buckets[index].start),
            anchor:
              index === 0 ? "start" : index === buckets.length - 1 ? "end" : "middle",
          }));
</script>

<svelte:window on:mousemove={moveDrag} on:mouseup={endDrag} />

{#if timeColumns.length > 0}
  <div class="stats-panel timeline-panel" aria-busy={loading}>
    <div class="condition-toolbar">
      <label class="condition-count">Column
        <select bind:value={column} aria-label="Timeline column">
          {#each timeColumns as name}
            <option value={name}>{$columnLabels.get(name) ?? name}</option>
          {/each}
        </select>
      </label>
      <button type="button" class="ghost" disabled={!canZoom} on:click={() => zoom(0.8)} title="Zoom in" aria-label="Zoom in">+</button>
      <button type="button" class="ghost" disabled={!canZoom} on:click={() => zoom(1.2)} title="Zoom out" aria-label="Zoom out">−</button>
      <button type="button" class="ghost" disabled={!canPanLeft} on:click={() => pan(-1)} title="Pan left" aria-label="Pan left">←</button>
      <button type="button" class="ghost" disabled={!canPanRight} on:click={() => pan(1)} title="Pan right" aria-label="Pan right">→</button>
      <button type="button" class="ghost" disabled={!histogram || atFullRange} on:click={showAll} title="Show the entire event period" aria-label="Show all events">Show all</button>
    </div>
    {#if open}
      {#if error}
        <p class="condition-count" role="alert">{error}</p>
      {:else if histogram && buckets.length > 0 && buckets[0].start}
        <p class="condition-count timeline-summary" data-in-window={bucketTotal} data-outside={histogram.outside}>
          In window {bucketTotal.toLocaleString()} / {totalRows.toLocaleString()}
          · Outside {histogram.outside.toLocaleString()}
          {#if histogram.missing > 0}· Missing {histogram.missing.toLocaleString()}{/if}
          {#if histogram.auto_focused}· Focused period{/if}
        </p>
        <div class="timeline-legend" aria-label="Timeline flag colors">
          <span class="flag-safe">Safe</span><span class="flag-suspicious">Suspicious</span>
          <span class="flag-critical">Critical</span><span class="flag-unflagged">Unflagged</span>
        </div>
        <!-- The SVG is pointer driven; the compact bucket list is the keyboard alternative. -->
        <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
        <div class="timeline-chart" bind:clientWidth={plotWidth}>
        <svg bind:this={svgEl} class="timeline-svg" viewBox={`0 0 ${viewWidth} ${VIEW_HEIGHT}`}
          role="application" aria-label={`Timeline histogram for ${$columnLabels.get(column) ?? column}`}
          on:mousedown={startDrag} on:wheel|nonpassive={handleWheel}>
          <line class="axis" x1="0" y1={PLOT_BOTTOM} x2={viewWidth} y2={PLOT_BOTTOM} />
          {#each buckets as bucket, index (`${index}:${bucket.start}`)}
            <g class="timeline-bucket" data-count={bucket.count} data-safe={bucket.flags.safe}
              data-suspicious={bucket.flags.suspicious} data-critical={bucket.flags.critical} data-unflagged={bucket.flags.unflagged}>
              <title>{bucketTitle(bucket)}</title>
              <rect aria-hidden="true" fill="transparent" pointer-events="all" x={index * slotWidth} y={PLOT_TOP}
                width={slotWidth} height={PLOT_BOTTOM - PLOT_TOP} />
              {#each FLAG_STACK as flag, flagIndex}
                {@const count = bucket.flags[flag]}
                {@const lower = FLAG_STACK.slice(0, flagIndex).reduce((sum, key) => sum + bucket.flags[key], 0)}
                <rect class={`bar flag-${flag}`} x={index * slotWidth} width={Math.max(1, slotWidth - 1)}
                  y={PLOT_BOTTOM - (maxCount > 0 ? ((lower + count) / maxCount) * (PLOT_BOTTOM - PLOT_TOP) : 0)}
                  height={maxCount > 0 ? (count / maxCount) * (PLOT_BOTTOM - PLOT_TOP) : 0} />
              {/each}
            </g>
          {/each}
          {#if selection}
            <rect class="selection" x={selection.start * slotWidth} y={PLOT_TOP}
              width={(selection.end - selection.start + 1) * slotWidth} height={PLOT_BOTTOM - PLOT_TOP} />
          {/if}
          {#each axisTicks as tick (tick.index)}
            <text class="axis-label" x={Math.min(viewWidth - 4, Math.max(4, tick.index * slotWidth + slotWidth / 2))}
              y={VIEW_HEIGHT - 8} text-anchor={tick.anchor}>{tick.label}</text>
          {/each}
        </svg>
        </div>
        <details>
          <summary class="condition-count">Buckets ({buckets.length})</summary>
          <ol class="bucket-list">
            {#each buckets as bucket, index (`${index}:${bucket.start}`)}
              <li><button type="button" class="ghost" title={bucketTitle(bucket)} disabled={bucket.count === 0}
                on:click={() => selectBuckets(index, index)}>
                <span>{shortTimelineLabel(bucket.start)}</span><strong>{bucket.count.toLocaleString()}</strong>
              </button></li>
            {/each}
          </ol>
        </details>
      {:else}
        <p class="condition-count">{loading ? "Loading timeline…" : "No datetime events."}</p>
      {/if}
    {/if}
  </div>
{/if}

<style>
  .timeline-panel { padding: 0; min-width: 0; }
  .timeline-chart { width: 100%; min-width: 0; }
  .timeline-svg { width: 100%; height: 170px; background: var(--surface-weak); border: 1px solid var(--border); cursor: crosshair; touch-action: none; user-select: none; }
  .timeline-legend { display: flex; flex-wrap: wrap; gap: 10px; font-size: 11px; margin: 6px 0; }
  .timeline-legend span::before { content: ""; display: inline-block; width: 8px; height: 8px; margin-right: 4px; background: currentColor; }
  .flag-safe { fill: var(--flag-safe-text); color: var(--flag-safe-text); }
  .flag-suspicious { fill: var(--flag-suspicious-text); color: var(--flag-suspicious-text); }
  .flag-critical { fill: var(--flag-critical-text); color: var(--flag-critical-text); }
  .flag-unflagged { fill: var(--muted); color: var(--muted); }
  .bar { opacity: .85; }
  .timeline-bucket:hover .bar { opacity: 1; }
  .selection { fill: var(--accent-soft); stroke: var(--accent); stroke-width: 1; pointer-events: none; }
  .axis { stroke: var(--border); stroke-width: 1; }
  .axis-label { fill: var(--muted); font-size: 11px; }
  .bucket-list { list-style: none; margin: 6px 0 0; padding: 0; max-height: 180px; overflow: auto; }
  .bucket-list button { display: flex; justify-content: space-between; width: 100%; gap: 8px; text-align: left; }
</style>
