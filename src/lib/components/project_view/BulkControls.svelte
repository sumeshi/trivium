<script lang="ts">
  import { createEventDispatcher, onDestroy } from "svelte";
  import type { AnnotationPatch, BulkAnnotateArgs } from "../../backend";
  import { conditionPayload } from "../../stores/condition";
  import {
    backend,
    flagFilter,
    projectDetail,
    search,
    visibleColumns,
    FLAG_OPTIONS,
  } from "./state";

  let target: "filter" | "all" = "filter";
  let flag: NonNullable<AnnotationPatch["flag"]> = FLAG_OPTIONS[0].value;
  let memo = "";
  let count: number | null = null;
  let error = "";
  let busy = false;
  let alive = true;

  const dispatch = createEventDispatcher<{ refresh: void }>();

  onDestroy(() => {
    alive = false;
  });

  $: projectId = $projectDetail?.project.meta.id ?? "";

  const buildArgs = (): BulkAnnotateArgs => {
    const base: BulkAnnotateArgs = {
      projectId,
      target,
      flag,
      memo: memo.trim() ? memo : null,
    };
    if (target === "all") {
      return base;
    }
    return {
      ...base,
      search: $search.trim() || undefined,
      flagFilter: $flagFilter,
      columns: $visibleColumns,
      condition: $conditionPayload,
    };
  };

  const run = async (operation: "preview" | "apply" | "undo") => {
    if (!$backend || !projectId || busy) {
      return;
    }
    busy = true;
    error = "";
    try {
      if (operation === "preview") {
        const result = await $backend.bulkPreview(buildArgs());
        if (!alive) return;
        count = result.count;
        return;
      }
      if (operation === "apply") {
        const result = await $backend.bulkUpdate(buildArgs());
        if (!alive) return;
        count = result.updated;
        dispatch("refresh");
        return;
      }
      const result = await $backend.bulkUndo({ projectId });
      if (!alive) return;
      count = result.updated;
      dispatch("refresh");
    } catch (err) {
      if (!alive) return;
      error = err instanceof Error ? err.message : String(err);
    } finally {
      if (alive) {
        busy = false;
      }
    }
  };
</script>

<div class="stats-panel">
  <div class="condition-toolbar">
    <span class="filter-label">Bulk annotate</span>
    <select bind:value={target} aria-label="Bulk target">
      <option value="filter">Filtered rows</option>
      <option value="all">All rows</option>
    </select>
    <select bind:value={flag} aria-label="Bulk flag">
      {#each FLAG_OPTIONS as option}
        <option value={option.value}>{option.hint} {option.label}</option>
      {/each}
    </select>
    <label class="condition-count">
      Memo
      <input bind:value={memo} placeholder="Optional" aria-label="Bulk memo" />
    </label>
    <button
      type="button"
      class="ghost"
      on:click={() => void run("preview")}
      disabled={busy}
    >
      Preview
    </button>
    <button
      type="button"
      class="primary"
      on:click={() => void run("apply")}
      disabled={busy}
    >
      Apply
    </button>
    <button
      type="button"
      class="ghost"
      on:click={() => void run("undo")}
      disabled={busy}
    >
      Undo last bulk
    </button>
    {#if count !== null}
      <span class="condition-count">{count} rows</span>
    {/if}
  </div>
  {#if error}
    <p class="condition-count">{error}</p>
  {/if}
</div>
