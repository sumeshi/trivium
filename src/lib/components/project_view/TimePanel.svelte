<script lang="ts">
  import { errorMessage } from "../../errors";
  import { onDestroy } from "svelte";
  import { createLatestRequest } from "../../latestRequest";
  import type { Backend } from "../../backend";
  import type { TimeAnalysis } from "../../types";
  import { shouldRequestPanel } from "../../panelRequest";
  import TimeSpanList from "./TimeSpanList.svelte";
  import { annotationVersions } from './state';
  import type { AppliedFilters } from "./state";
  import { useWorkspace } from "../../workspaceContext";
  const { appliedFilters, backend, columnLabels, projectDetail, createReadScope } = useWorkspace();
  const reads = createReadScope();

  export let open = false;

  let column = "";
  let gapSeconds: number | null = 60;
  let sessionMinutes: number | null = 30;
  let burstWindowSeconds: number | null = 60;
  let burstMin: number | null = 10;
  let analysis: TimeAnalysis | null = null;
  let error = "";
  let loading = false;
  let gapsOpen = true;
  let sessionsOpen = false;
  let burstsOpen = false;
  let lastAnalysisKey = "";
  const requests = createLatestRequest();

  onDestroy(() => {
    reads.dispose();
    requests.dispose();
  });

  $: timeColumns = ($projectDetail?.columns ?? []).filter(
    (name) => $projectDetail?.column_kinds?.[name] === "datetime"
  );
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: annotationRevision = $annotationVersions[projectId] ?? 0;
  $: scope = { projectId, annotationRevision };
  $: if (projectId && !timeColumns.includes(column)) {
    column = timeColumns[0] ?? "";
  }

  const ready = (value: number | null) => Number.isSafeInteger(value) && (value ?? 0) >= 0;

  const loadAnalysis = async (
    api: Backend,
    activeScope: { projectId: string; annotationRevision: number },
    activeColumn: string,
    activeGap: number | null,
    activeSession: number | null,
    activeWindow: number | null,
    activeBurstMin: number | null,
    spec: AppliedFilters
  ) => {
    reads.cancelAll();
    const ticket = requests.next();
    analysis = null;
    error = "";
    if (!ready(activeGap) || !ready(activeSession) || !ready(activeWindow) || !ready(activeBurstMin)) {
      loading = false;
      error = "Time analysis settings must be non-negative whole numbers.";
      return;
    }
    loading = true;
    try {
      const next = await reads.run("analysis", api, (requestId) => api.timeAnalysis({
        requestId,
        projectId: activeScope.projectId,
        column: activeColumn,
        gapSeconds: activeGap ?? 60,
        sessionMinutes: activeSession ?? 30,
        burstWindowSeconds: activeWindow ?? 60,
        burstMin: activeBurstMin ?? 10,
        search: spec.search.trim() || undefined,
        searchColumns: [...spec.searchColumns],
        flagFilter: spec.flag,
        bookmarkedOnly: spec.bookmarkedOnly,
        condition: spec.condition,
      }));
      if (!requests.isCurrent(ticket)) {
        return;
      }
      analysis = next;
    } catch (err) {
      if (!requests.isCurrent(ticket)) {
        return;
      }
      error = errorMessage(err);
    } finally {
      if (requests.isCurrent(ticket)) loading = false;
    }
  };

  $: if (!open) {
    reads.cancelAll();
    // Ignore any response that was already in flight.
    requests.invalidate();
    loading = false;
    lastAnalysisKey = "";
  }

  $: analysisKey = JSON.stringify([scope, column, gapSeconds, sessionMinutes, burstWindowSeconds, burstMin, $appliedFilters]);
  $: if ($backend && shouldRequestPanel({ open, projectId, column }) && analysisKey !== lastAnalysisKey) {
    lastAnalysisKey = analysisKey;
    void loadAnalysis(
      $backend,
      scope,
      column,
      gapSeconds,
      sessionMinutes,
      burstWindowSeconds,
      burstMin,
      $appliedFilters
    );
  }
</script>

<div class="stats-panel time-panel">
  {#if timeColumns.length > 0}
    <div class="time-column">
      <label for="time-column">Column</label>
      <select id="time-column" bind:value={column} aria-label="Time column">
        {#each timeColumns as name}
          <option value={name}>{$columnLabels.get(name) ?? name}</option>
        {/each}
      </select>
    </div>
    <div class="time-settings">
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
      <p class="panel-error" role="alert">{error}</p>
    {:else if loading}
      <p class="condition-count" role="status">Analyzing time…</p>
    {:else if analysis}
      <p class="time-help">Click an interval to filter. Hover for the original timestamps.</p>
      <details class="time-section" bind:open={gapsOpen}>
        <summary>Gaps <span>{analysis.gaps.length.toLocaleString()}</span></summary>
        {#if gapsOpen}<TimeSpanList column={analysis.column} spans={analysis.gaps} kind="gap" />{/if}
      </details>
      <details class="time-section" bind:open={sessionsOpen}>
        <summary>Sessions <span>{analysis.sessions.length.toLocaleString()}</span></summary>
        {#if sessionsOpen}<TimeSpanList column={analysis.column} spans={analysis.sessions} kind="session" />{/if}
      </details>
      <details class="time-section" bind:open={burstsOpen}>
        <summary>Bursts <span>{analysis.bursts.length.toLocaleString()}</span></summary>
        {#if burstsOpen}<TimeSpanList column={analysis.column} spans={analysis.bursts} kind="burst" />{/if}
      </details>
    {/if}
  {:else}
    <p class="condition-count">This project has no datetime columns.</p>
  {/if}
</div>

<style>
  .time-panel { gap: 10px; min-width: 0; }
  .time-column { display: flex; gap: 8px; align-items: center; font-size: 11px; color: var(--muted); }
  .time-column select { flex: 1; min-width: 0; }
  .time-settings { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 7px 10px; }
  .time-settings label { display: flex; flex-direction: column; gap: 3px; font-size: 10px; color: var(--muted); }
  .time-settings input { width: 100%; min-width: 0; }
  .time-help { margin: 0; color: var(--muted); font-size: 10px; }
  .time-section { border-top: 1px solid var(--border); padding-top: 8px; }
  summary { cursor: pointer; font-size: 11px; color: var(--fg); }
  summary span { color: var(--muted); margin-left: 5px; font-variant-numeric: tabular-nums; }
  .panel-error { margin: 0; font-size: 11px; color: var(--critical); }
</style>
