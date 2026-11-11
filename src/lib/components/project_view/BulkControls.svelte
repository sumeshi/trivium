<script lang="ts">
  import { createEventDispatcher, onDestroy } from "svelte";
  import type { AnnotationPatch, BulkAnnotateArgs } from "../../backend";
  import {
    appliedFilters,
    backend,
    projectDetail,
    selectedCount,
    selectedRowIds,
    sortDirection,
    sortKey,
    totalFilteredRows,
    FLAG_OPTIONS,
  } from "./state";

  let target: "selection" | "filtered" | "all" = "filtered";
  let flag: NonNullable<AnnotationPatch["flag"]> = FLAG_OPTIONS[0].value;
  let memo = "";
  let tagInput = "";
  let count: number | null = null;
  let previewToken: string | null = null;
  let error = "";
  let busy = false;
  let alive = true;

  const dispatch = createEventDispatcher<{ refresh: void }>();

  onDestroy(() => {
    alive = false;
  });

  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: allRowCount = $projectDetail?.project.meta.total_records ?? 0;
  // A zero-row selection is not a request for the filtered or all scope.
  $: selectedTargetEmpty = target === "selection" && $selectedCount === 0;
  $: canApplyTag = tagInput.trim().length > 0 && !selectedTargetEmpty;

  const buildArgs = (patch: AnnotationPatch): BulkAnnotateArgs => ({
    projectId,
    target,
    ...(target === "selection" ? { rowIndices: Array.from($selectedRowIds) } : {}),
    // Use the applied QuerySpec, not the live inputs. The search box may still
    // be waiting for its debounce while the table shows the previous result.
    search: $appliedFilters.search.trim() || undefined,
    searchColumns: $appliedFilters.searchColumns,
    flagFilter: $appliedFilters.flag,
    bookmarkedOnly: $appliedFilters.bookmarkedOnly,
    sortKey: $sortKey,
    sortDirection: $sortDirection,
    condition: $appliedFilters.condition,
    ...(previewToken ? { previewToken } : {}),
    ...patch,
  });

  const flagMemoPatch = (): AnnotationPatch => ({
    flag,
    memo: memo.trim() ? memo : null,
  });

  const run = async (operation: "preview" | "apply" | "undo") => {
    if (!$backend || !projectId || busy || selectedTargetEmpty) {
      return;
    }
    busy = true;
    error = "";
    try {
      if (operation === "preview") {
        const result = await $backend.bulkPreview(buildArgs(flagMemoPatch()));
        if (!alive) return;
        count = result.count;
        previewToken = result.previewToken;
        return;
      }
      if (operation === "apply") {
        if (!previewToken) {
          error = "Preview again before applying.";
          return;
        }
        const result = await $backend.bulkUpdate(buildArgs(flagMemoPatch()));
        if (!alive) return;
        count = result.updated;
        previewToken = null;
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

  /**
   * Tags are one-directional: Add appends and Remove deletes. Neither reads a
   * mixed row's current tags to toggle them. The preview token is fetched in
   * the same call as the update so the target cannot change in between.
   */
  const runTag = async (operation: "add" | "remove") => {
    const tag = tagInput.trim();
    if (!$backend || !projectId || busy || !tag || selectedTargetEmpty) {
      return;
    }
    busy = true;
    error = "";
    const patch: AnnotationPatch =
      operation === "add" ? { add_tags: [tag] } : { remove_tags: [tag] };
    try {
      const args = buildArgs(patch);
      const preview = await $backend.bulkPreview(args);
      if (!alive) return;
      count = preview.count;
      previewToken = null;
      if (preview.count === 0) {
        return;
      }
      const result = await $backend.bulkUpdate({
        ...args,
        previewToken: preview.previewToken,
      });
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
      <option value="selection" disabled={$selectedCount === 0}>
        Selected ({$selectedCount})
      </option>
      <option value="filtered">Current results ({$totalFilteredRows})</option>
      <option value="all">All rows ({allRowCount})</option>
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
      disabled={busy || selectedTargetEmpty}
    >
      Preview
    </button>
    <button
      type="button"
      class="primary"
      on:click={() => void run("apply")}
      disabled={busy || selectedTargetEmpty}
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
    <label class="condition-count">
      Tag
      <input bind:value={tagInput} placeholder="tag" aria-label="Bulk tag" />
    </label>
    <button
      type="button"
      class="ghost"
      on:click={() => void runTag("add")}
      disabled={busy || !canApplyTag}
      title="Preview and add this tag to the target rows"
    >
      Add tag
    </button>
    <button
      type="button"
      class="ghost"
      on:click={() => void runTag("remove")}
      disabled={busy || !canApplyTag}
      title="Preview and remove this tag from the target rows"
    >
      Remove tag
    </button>
    {#if count !== null}
      <span class="condition-count">{count} rows</span>
    {/if}
    {#if selectedTargetEmpty}
      <span class="condition-count">Select rows to use the selection target.</span>
    {/if}
  </div>
  {#if error}
    <p class="condition-count">{error}</p>
  {/if}
</div>
