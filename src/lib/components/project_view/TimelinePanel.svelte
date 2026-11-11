<script lang="ts">
  import { errorMessage } from "../../errors";
  import { onDestroy } from "svelte";
  import type { Backend } from "../../backend";
  import type { Histogram } from "../../types";
  import { filterByRange } from "../../stores/condition";
  import { shouldRequestPanel } from "../../panelRequest";
  import { appliedFilters, backend, columnLabels, projectDetail } from "./state";
  import type { AppliedFilters } from "./state";

  export let open = false;

  const BUCKET_COUNT = 60;
  const VIEW_WIDTH = 1000;
  const VIEW_HEIGHT = 190;
  const PLOT_TOP = 10;
  const PLOT_BOTTOM = 130;

  let column = "";
  let zoomRange: { min: string; max: string } | null = null;
  let histogram: Histogram | null = null;
  let fullHistogram: Histogram | null = null;
  let error = "";
  let request = 0;
  let alive = true;
  let dragging = false;
  let dragStart: number | null = null;
  let dragEnd: number | null = null;
  let svgEl: SVGSVGElement | null = null;
  let lastResetKey = "";

  onDestroy(() => {
    alive = false;
  });

  const boundaryList = (source: Histogram | null): string[] => {
    if (!source || source.buckets.length === 0) {
      return [];
    }
    const first = source.buckets[0].start;
    const last = source.buckets[source.buckets.length - 1].end;
    if (!first || !last) {
      return [];
    }
    return [...source.buckets.map((bucket) => bucket.start), last];
  };

  const windowIndices = (): [number, number] | null => {
    const grid = boundaryList(fullHistogram);
    const total = grid.length - 1;
    if (total < 1) {
      return null;
    }
    if (!zoomRange) {
      return [0, total];
    }
    const start = grid.indexOf(zoomRange.min);
    const end = grid.indexOf(zoomRange.max);
    if (start < 0 || end <= start) {
      return [0, total];
    }
    return [start, end];
  };

  const applyWindow = (startIndex: number, endIndex: number) => {
    const grid = boundaryList(fullHistogram);
    const total = grid.length - 1;
    if (total < 1) {
      return;
    }
    const start = Math.max(0, Math.min(total - 1, startIndex));
    const end = Math.max(start + 1, Math.min(total, endIndex));
    if (start === 0 && end === total) {
      zoomRange = null;
    } else {
      zoomRange = { min: grid[start], max: grid[end] };
    }
  };

  const zoom = (factor: number) => {
    const indices = windowIndices();
    if (!indices) {
      return;
    }
    const total = boundaryList(fullHistogram).length - 1;
    const [start, end] = indices;
    const width = end - start;
    const nextWidth =
      factor < 1
        ? Math.max(1, Math.floor(width * factor))
        : Math.min(total, Math.ceil(width * factor));
    if (nextWidth === width) {
      return;
    }
    const center = (start + end) / 2;
    let nextStart = Math.round(center - nextWidth / 2);
    let nextEnd = nextStart + nextWidth;
    if (nextStart < 0) {
      nextStart = 0;
      nextEnd = nextWidth;
    }
    if (nextEnd > total) {
      nextEnd = total;
      nextStart = total - nextWidth;
    }
    applyWindow(nextStart, nextEnd);
  };

  const pan = (direction: -1 | 1) => {
    const indices = windowIndices();
    if (!indices) {
      return;
    }
    const total = boundaryList(fullHistogram).length - 1;
    const [start, end] = indices;
    const width = end - start;
    const shift = Math.max(1, Math.round(width * 0.5));
    const nextStart = Math.max(0, Math.min(total - width, start + direction * shift));
    applyWindow(nextStart, nextStart + width);
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
    const ratio = local.x / VIEW_WIDTH;
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
    const first = histogram.buckets[low];
    const last = histogram.buckets[high];
    dragStart = null;
    dragEnd = null;
    if (first?.start && last?.end) {
      filterByRange(column, first.start, last.end);
    }
  };

  const handleWheel = (event: WheelEvent) => {
    if (!canZoom) {
      return;
    }
    event.preventDefault();
    zoom(event.deltaY < 0 ? 0.8 : 1.2);
  };

  const shortLabel = (value: string) =>
    value.length > 19 ? `${value.slice(0, 19)}…` : value;

  const loadHistogram = async (
    api: Backend,
    activeProjectId: string,
    activeColumn: string,
    activeRange: { min: string; max: string } | null,
    spec: AppliedFilters
  ) => {
    const ticket = ++request;
    error = "";
    try {
      const next = await api.histogram({
        projectId: activeProjectId,
        column: activeColumn,
        buckets: BUCKET_COUNT,
        range: activeRange,
        search: spec.search.trim() || undefined,
        searchColumns: [...spec.searchColumns],
        flagFilter: spec.flag,
        bookmarkedOnly: spec.bookmarkedOnly,
        condition: spec.condition,
      });
      if (!alive || ticket !== request) {
        return;
      }
      if (activeRange === null) {
        fullHistogram = next;
      }
      histogram = next;
    } catch (err) {
      if (!alive || ticket !== request) {
        return;
      }
      error = errorMessage(err);
    }
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
    fullHistogram = null;
    error = "";
    dragStart = null;
    dragEnd = null;
    dragging = false;
  }

  $: if (!open) {
    // Ignore any response that was already in flight.
    request += 1;
  }

  $: if ($backend && shouldRequestPanel({ open, projectId, column })) {
    void loadHistogram(
      $backend,
      projectId,
      column,
      zoomRange,
      $appliedFilters
    );
  }

  $: buckets = histogram?.buckets ?? [];
  $: maxCount = buckets.reduce((max, bucket) => Math.max(max, bucket.count), 0);
  $: bucketTotal = buckets.reduce((sum, bucket) => sum + bucket.count, 0);
  $: totalRows = bucketTotal + (histogram?.outside ?? 0) + (histogram?.missing ?? 0);
  $: canZoom = boundaryList(fullHistogram).length > 1;
  $: slotWidth = VIEW_WIDTH / Math.max(buckets.length, 1);
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
      : [0, Math.round(buckets.length / 4), Math.round(buckets.length / 2), Math.round((buckets.length * 3) / 4), buckets.length - 1]
          .filter((value, index, list) => list.indexOf(value) === index)
          .map((index) => ({
            index,
            label:
              index === buckets.length - 1
                ? shortLabel(buckets[index].end)
                : shortLabel(buckets[index].start),
            anchor:
              index === 0 ? "start" : index === buckets.length - 1 ? "end" : "middle",
          }));
</script>

<svelte:window on:mousemove={moveDrag} on:mouseup={endDrag} />

{#if timeColumns.length > 0}
  <div class="stats-panel timeline-panel">
    <div class="condition-toolbar">
      <span class="filter-label">Timeline</span>
      {#if open}
        <label class="condition-count">
          Column
          <select bind:value={column} aria-label="Timeline column">
            {#each timeColumns as name}
              <option value={name}>{$columnLabels.get(name) ?? name}</option>
            {/each}
          </select>
        </label>
        <button type="button" class="ghost" disabled={!canZoom} on:click={() => zoom(0.8)} title="Zoom in">
          +
        </button>
        <button type="button" class="ghost" disabled={!canZoom} on:click={() => zoom(1.2)} title="Zoom out">
          −
        </button>
        <button type="button" class="ghost" disabled={!canZoom} on:click={() => pan(-1)} title="Pan left">
          ←
        </button>
        <button type="button" class="ghost" disabled={!canZoom} on:click={() => pan(1)} title="Pan right">
          →
        </button>
        <button type="button" class="ghost" disabled={!zoomRange} on:click={() => (zoomRange = null)}>
          Reset
        </button>
      {/if}
    </div>
    {#if open}
      {#if error}
        <p class="condition-count">{error}</p>
      {:else if histogram && buckets.length > 0}
        <p class="condition-count">
          rows {totalRows} · in window {bucketTotal}
          {#if histogram.outside > 0}· outside {histogram.outside}{/if}
          {#if histogram.missing > 0}· missing {histogram.missing}{/if}
        </p>
        <!-- The SVG is a pointer-driven chart; the bucket buttons below are the keyboard alternative. -->
        <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
        <svg
          bind:this={svgEl}
          class="timeline-svg"
          viewBox={`0 0 ${VIEW_WIDTH} ${VIEW_HEIGHT}`}
          role="application"
          aria-label={`Timeline histogram for ${column}`}
          on:mousedown={startDrag}
          on:wheel|nonpassive={handleWheel}
        >
          <line class="axis" x1="0" y1={PLOT_BOTTOM} x2={VIEW_WIDTH} y2={PLOT_BOTTOM} />
          {#each buckets as bucket, index (`${index}:${bucket.start}`)}
            {@const barHeight =
              maxCount > 0 ? (bucket.count / maxCount) * (PLOT_BOTTOM - PLOT_TOP) : 0}
            <rect
              class="bar"
              x={index * slotWidth}
              y={PLOT_BOTTOM - barHeight}
              width={Math.max(1, slotWidth - 1)}
              height={barHeight}
            >
              <title>{bucket.start}–{bucket.end} ({bucket.count})</title>
            </rect>
          {/each}
          {#if selection}
            <rect
              class="selection"
              x={selection.start * slotWidth}
              y={PLOT_TOP}
              width={(selection.end - selection.start + 1) * slotWidth}
              height={PLOT_BOTTOM - PLOT_TOP}
            />
          {/if}
          {#each axisTicks as tick (tick.index)}
            <text
              class="axis-label"
              x={Math.min(VIEW_WIDTH - 4, Math.max(4, tick.index * slotWidth + slotWidth / 2))}
              y={VIEW_HEIGHT - 8}
              text-anchor={tick.anchor}
            >{tick.label}</text>
          {/each}
        </svg>
        <details>
          <summary class="condition-count">Buckets ({buckets.length})</summary>
          <div class="condition-toolbar">
            {#each buckets as bucket, index (`${index}:${bucket.start}`)}
              <button
                type="button"
                class="ghost"
                on:click={() => bucket.start && bucket.end && filterByRange(column, bucket.start, bucket.end)}
              >
                {bucket.start}–{bucket.end} ({bucket.count})
              </button>
            {/each}
          </div>
        </details>
      {:else}
        <p class="condition-count">No datetime buckets.</p>
      {/if}
    {/if}
  </div>
{/if}

<style>
  .timeline-panel {
    padding: 8px 12px;
  }

  .timeline-svg {
    width: 100%;
    height: 190px;
    background: var(--surface-weak);
    border: 1px solid var(--border);
    border-radius: 8px;
    cursor: crosshair;
    touch-action: none;
    user-select: none;
  }

  .bar {
    fill: var(--accent);
    opacity: 0.72;
  }

  .bar:hover {
    opacity: 1;
  }

  .selection {
    fill: var(--accent-soft);
    stroke: var(--accent);
    stroke-width: 1;
    pointer-events: none;
  }

  .axis {
    stroke: var(--border);
    stroke-width: 1;
  }

  .axis-label {
    fill: var(--muted);
    font-size: 11px;
  }
</style>
