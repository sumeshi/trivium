<script lang="ts">
  import type { TimeSpan } from "../../types";
  import { compactTimestamp, durationLabel, durationNanos, rankedSpans } from "../../panelPresentation";
  import { filterByRange } from "../../stores/condition";

  export let column: string;
  export let spans: TimeSpan[] = [];
  export let kind: "gap" | "session" | "burst";

  let order: "count" | "duration" | "time" = kind === "gap" ? "duration" : "count";
  let visible = 20;
  let previous: TimeSpan[] | null = null;
  $: sorted = rankedSpans(spans, order);
  $: if (spans !== previous) { previous = spans; visible = 20; }
</script>

<div class="span-list-toolbar">
  <span>{spans.length.toLocaleString()} {kind === "gap" ? "gaps" : kind === "session" ? "sessions" : "bursts"}</span>
  <select bind:value={order} aria-label={`Sort ${kind}s`}>
    {#if kind !== "gap"}<option value="count">Most events</option>{/if}
    <option value="duration">Longest duration</option>
    <option value="time">Time order</option>
  </select>
</div>
{#if spans.length === 0}
  <p class="empty-spans">None in this result.</p>
{:else}
  <ol class="time-spans">
    {#each sorted.slice(0, visible) as span, index (`${span.start}:${span.end}:${index}`)}
      <li>
        <button
          type="button"
          class="span-row"
          title={`Filter ${column}: ${span.start} – ${span.end}; ${kind === "gap" ? `${span.count.toLocaleString()} elapsed seconds` : `${span.count.toLocaleString()} events`}`}
          on:click={() => filterByRange(column, span.start, span.end)}
        >
          <span class="span-dates"><span>{compactTimestamp(span.start)}</span><span>{compactTimestamp(span.end)}</span></span>
          <span class="span-measures">
            {#if kind !== "gap"}<strong>{span.count.toLocaleString()} <small>events</small></strong>{/if}
            <span>{durationLabel(durationNanos(span.start, span.end))}</span>
          </span>
        </button>
      </li>
    {/each}
  </ol>
  {#if visible < sorted.length || visible > 20}
    <div class="span-pagination">
      <span>Showing {Math.min(visible, sorted.length).toLocaleString()} of {sorted.length.toLocaleString()}</span>
      {#if visible < sorted.length}<button type="button" on:click={() => visible += 50}>Show more</button>{/if}
      {#if visible > 20}<button type="button" on:click={() => visible = 20}>Show less</button>{/if}
    </div>
  {/if}
{/if}

<style>
  .span-list-toolbar { display: flex; align-items: center; justify-content: space-between; gap: 8px; font-size: 10px; color: var(--muted); margin: 6px 0; }
  .span-list-toolbar select { max-width: 145px; min-width: 0; height: 27px; font-size: 10px; }
  .time-spans { list-style: none; padding: 0; margin: 0; display: grid; gap: 2px; }
  .span-row { width: 100%; display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 6px; border: 1px solid transparent; border-bottom-color: var(--border); border-radius: 2px; color: var(--fg); background: transparent; text-align: left; }
  .span-row:hover, .span-row:focus-visible { outline: none; border-color: var(--accent-border); background: var(--accent-soft); }
  .span-dates { display: grid; gap: 2px; min-width: 0; font-size: 10px; font-family: var(--font-mono); }
  .span-dates span { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .span-dates span:last-child { color: var(--muted); }
  .span-measures { display: grid; gap: 2px; flex: 0 0 auto; text-align: right; font-size: 10px; font-variant-numeric: tabular-nums; color: var(--muted); }
  .span-measures strong { color: var(--fg); font-size: 11px; font-weight: 500; }
  .span-measures small { font-weight: 400; font-size: 9px; color: var(--muted); }
  .span-pagination { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; font-size: 10px; color: var(--muted); margin-top: 7px; }
  .span-pagination button { border: 0; padding: 3px 0; color: var(--accent); background: transparent; font-size: 10px; }
  .span-pagination button:focus-visible { outline: 1px solid var(--accent-border); }
  .empty-spans { color: var(--muted); font-size: 11px; margin: 5px 0; }
</style>
