<script lang="ts">
  import { onDestroy } from "svelte";
  import type { Backend } from "../../backend";
  import type { ConditionNode, TimeAnalysis } from "../../types";
  import { conditionPayload, filterByRange } from "../../stores/condition";
  import { shouldRequestPanel } from "../../panelRequest";
  import { backend, columnLabels, flagFilter, projectDetail, search, visibleColumns } from "./state";
  import type { FlagFilterValue } from "./state";

  export let open = false;

  let column = "";
  let gapSeconds: number | null = 60;
  let sessionMinutes: number | null = 30;
  let burstWindowSeconds: number | null = 60;
  let burstMin: number | null = 10;
  let analysis: TimeAnalysis | null = null;
  let error = "";
  let request = 0;
  let alive = true;

  onDestroy(() => {
    alive = false;
  });

  $: timeColumns = ($projectDetail?.columns ?? []).filter(
    (name) => $projectDetail?.column_kinds?.[name] === "datetime"
  );
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: if (projectId && !timeColumns.includes(column)) {
    column = timeColumns[0] ?? "";
  }

  const ready = (value: number | null) => Number.isInteger(value) && (value ?? 0) >= 0;

  const loadAnalysis = async (
    api: Backend,
    activeProjectId: string,
    activeColumn: string,
    activeGap: number | null,
    activeSession: number | null,
    activeWindow: number | null,
    activeBurstMin: number | null,
    activeSearch: string,
    activeFlag: FlagFilterValue,
    activeColumns: string[],
    activeCondition: ConditionNode | null
  ) => {
    if (!ready(activeGap) || !ready(activeSession) || !ready(activeWindow) || !ready(activeBurstMin)) {
      return;
    }
    const ticket = ++request;
    analysis = null;
    error = "";
    try {
      const next = await api.timeAnalysis({
        projectId: activeProjectId,
        column: activeColumn,
        gapSeconds: activeGap ?? 60,
        sessionMinutes: activeSession ?? 30,
        burstWindowSeconds: activeWindow ?? 60,
        burstMin: activeBurstMin ?? 10,
        search: activeSearch.trim() || undefined,
        flagFilter: activeFlag,
        columns: activeColumns,
        condition: activeCondition,
      });
      if (!alive || ticket !== request) {
        return;
      }
      analysis = next;
    } catch (err) {
      if (!alive || ticket !== request) {
        return;
      }
      error = err instanceof Error ? err.message : String(err);
    }
  };

  $: if (!open) {
    // Ignore any response that was already in flight.
    request += 1;
  }

  $: if ($backend && shouldRequestPanel({ open, projectId, column })) {
    void loadAnalysis(
      $backend,
      projectId,
      column,
      gapSeconds,
      sessionMinutes,
      burstWindowSeconds,
      burstMin,
      $search,
      $flagFilter,
      $visibleColumns,
      $conditionPayload
    );
  }
</script>

{#if timeColumns.length > 0}
  <div class="stats-panel">
    <div class="condition-toolbar">
      <span class="filter-label">Time</span>
      <select bind:value={column} aria-label="Time column">
        {#each timeColumns as name}
          <option value={name}>{$columnLabels.get(name) ?? name}</option>
        {/each}
      </select>
      <label class="condition-count">
        Gap seconds
        <input type="number" min="0" step="1" bind:value={gapSeconds} aria-label="Gap seconds" />
      </label>
      <label class="condition-count">
        Session minutes
        <input type="number" min="0" step="1" bind:value={sessionMinutes} aria-label="Session minutes" />
      </label>
      <label class="condition-count">
        Burst window
        <input type="number" min="0" step="1" bind:value={burstWindowSeconds} aria-label="Burst window seconds" />
      </label>
      <label class="condition-count">
        Burst over
        <input type="number" min="0" step="1" bind:value={burstMin} aria-label="Burst minimum count" />
      </label>
    </div>
    {#if error}
      <p class="condition-count">{error}</p>
    {:else if analysis}
      {@const columnName = analysis.column}
      <div class="condition-toolbar">
        <span class="condition-count">Gaps</span>
        {#each analysis.gaps as span, index (`gap:${span.start}:${span.end}:${index}`)}
          <button type="button" class="ghost" on:click={() => filterByRange(columnName, span.start, span.end)}>
            {span.start}–{span.end} ({span.count}s)
          </button>
        {/each}
      </div>
      <div class="condition-toolbar">
        <span class="condition-count">Sessions</span>
        {#each analysis.sessions as span, index (`session:${span.start}:${span.end}:${index}`)}
          <button type="button" class="ghost" on:click={() => filterByRange(columnName, span.start, span.end)}>
            {span.start}–{span.end} ({span.count})
          </button>
        {/each}
      </div>
      <div class="condition-toolbar">
        <span class="condition-count">Bursts</span>
        {#each analysis.bursts as span, index (`burst:${span.start}:${span.end}:${index}`)}
          <button type="button" class="ghost" on:click={() => filterByRange(columnName, span.start, span.end)}>
            {span.start}–{span.end} ({span.count})
          </button>
        {/each}
      </div>
    {/if}
  </div>
{/if}
