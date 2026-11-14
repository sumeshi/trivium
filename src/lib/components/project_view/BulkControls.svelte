<script lang="ts">
  import { createEventDispatcher, onDestroy } from "svelte";
  import { get } from "svelte/store";
  import { errorMessage } from "../../errors";
  import type { BulkCount } from "../../types";
  import { applyBulkAnnotation, buildBulkAnnotateArgs, buildBulkPatch } from "../../bulkAnnotation";
  import type { BulkFlag, BulkTagOperation, BulkTarget } from "../../bulkAnnotation";
  import { FLAG_OPTIONS } from './state';
  import { useWorkspace } from "../../workspaceContext";

  const workspace = useWorkspace();
  const { appliedResultLoading, backend, projectDetail, selectedCount, totalFilteredRows } = workspace;
  const reads = workspace.createReadScope();
  const dispatch = createEventDispatcher<{ refresh: void }>();
  let target: BulkTarget = "filtered";
  let flag: BulkFlag = "keep";
  let memo = "";
  let clearMemo = false;
  let tagInput = "";
  let tagOperation: BulkTagOperation = "add";
  let preview: BulkCount | null = null;
  let count: number | null = null;
  let resultKind: "preview" | "applied" | "restored" = "preview";
  let error = "";
  let busy = false;
  let alive = true;
  let generation = 0;

  const invalidate = () => {
    generation += 1;
    preview = null; if (resultKind === "preview") count = null; error = "";
    reads.cancelAll();
  };
  // Query changes can arrive while a preview is pending, independently of
  // this form. They must not turn its old response into a new Apply token.
  const subscriptions = [workspace.currentProjectId, workspace.appliedFilters,
    workspace.search, workspace.searchColumns, workspace.flagFilter,
    workspace.bookmarkedOnly, workspace.conditionPayload,
    workspace.sortKey, workspace.sortDirection, workspace.selectedRowIds].map(store => {
    let initial = true;
    return store.subscribe(() => { if (initial) initial = false; else invalidate(); });
  });
  const cleanup = () => {
    if (!alive) return;
    alive = false; reads.dispose(); subscriptions.forEach(stop => stop());
  };
  // Workspace teardown clears stores before child components unmount. Stop
  // observing those clears so a captured Apply can finish for its old case.
  const unregister = workspace.onDispose(cleanup);
  onDestroy(() => { unregister(); cleanup(); });

  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: allRowCount = $projectDetail?.project.meta.total_records ?? 0;
  $: selectedTargetEmpty = target === "selection" && $selectedCount === 0;
  $: filteredResultLoading = target === "filtered" && $appliedResultLoading;
  $: targetEmpty = selectedTargetEmpty || (target === "filtered" && $totalFilteredRows === 0) || (target === "all" && allRowCount === 0);
  $: patch = buildBulkPatch({ flag, memo, clearMemo, tag: tagInput, tagOperation });
  $: hasPatch = Object.keys(patch).length > 0;

  const captureArgs = () => buildBulkAnnotateArgs({
    projectId: get(workspace.currentProjectId) ?? "", target,
    selectedRowIds: Array.from(get(workspace.selectedRowIds)),
    // The displayed result's applied query remains authoritative while the
    // main search input is still waiting for its debounce.
    filters: get(workspace.appliedFilters), sortKey: get(workspace.sortKey),
    sortDirection: get(workspace.sortDirection), previewToken: null,
    patch: buildBulkPatch({ flag, memo, clearMemo, tag: tagInput, tagOperation }),
  });

  const run = async (operation: "preview" | "apply" | "undo") => {
    const api = get(backend), owner = get(workspace.currentProjectId);
    if (!api || !owner || busy) return;
    if (operation !== "undo" && (selectedTargetEmpty || filteredResultLoading || (operation === "apply" && (!hasPatch || targetEmpty)))) return;
    const ticket = generation;
    const owned = () => alive && get(workspace.currentProjectId) === owner;
    const current = () => owned() && generation === ticket && !(target === "filtered" && get(workspace.appliedResultLoading));
    busy = true; error = ""; count = null;
    try {
      if (operation === "undo") {
        const result = await api.bulkUndo({ projectId: owner });
        if (!owned()) return;
        preview = null; count = result.updated; resultKind = "restored";
        dispatch("refresh");
      } else if (operation === "preview") {
        const args = structuredClone(captureArgs());
        const result = await reads.run("preview", api, requestId => api.bulkPreview({ ...args, requestId }));
        if (!current()) return;
        preview = result; count = result.count; resultKind = "preview";
      } else {
        const result = await applyBulkAnnotation(api, captureArgs(), {
          preview,
          // A requested mutation survives closing its panel. While mounted,
          // changed form/query inputs cancel the pre-commit preview instead.
          isCurrent: () => generation === ticket && (!alive || current()),
        });
        if (!result) { if (owned()) error = "Target changed. Apply again."; return; }
        if (!owned()) return;
        preview = null; count = result.updated; resultKind = "applied";
        dispatch("refresh");
      }
    } catch (err) {
      if (current()) {
        preview = null;
        const message = errorMessage(err);
        error = message === "Target changed; preview again." ? "Target changed. Apply again." : message;
      }
    } finally { if (alive) busy = false; }
  };
</script>

<div class="stats-panel bulk-form">
  <label class="bulk-field">Rows
    <select bind:value={target} aria-label="Bulk target" disabled={busy} on:change={invalidate}>
      <option value="selection" disabled={$selectedCount === 0}>Selected ({$selectedCount})</option>
      <option value="filtered">Current results ({filteredResultLoading ? '…' : $totalFilteredRows})</option>
      <option value="all">All rows ({allRowCount})</option>
    </select>
  </label>
  <label class="bulk-field">Flag
    <select bind:value={flag} aria-label="Bulk flag" disabled={busy} on:change={invalidate}>
      <option value="keep">Keep</option>
      <option value="">Unflagged</option>
      {#each FLAG_OPTIONS as option}<option value={option.value}>{option.hint} {option.label}</option>{/each}
    </select>
  </label>
  <div class="bulk-field bulk-wide">
    <label for="bulk-note">Note</label>
    <input id="bulk-note" bind:value={memo} placeholder="Keep" aria-label="Bulk note" disabled={busy || clearMemo} on:input={invalidate} />
    <label class="bulk-clear" title="Remove notes from matching rows">
      <input type="checkbox" bind:checked={clearMemo} disabled={busy} on:change={event => { if (event.currentTarget.checked) memo = ""; invalidate(); }} />
      Clear note
    </label>
  </div>
  <div class="bulk-field bulk-wide">
    <label for="bulk-tag">Tag</label>
    <div class="bulk-tag">
      <select bind:value={tagOperation} aria-label="Bulk tag action" disabled={busy} on:change={invalidate}>
        <option value="add">Add</option><option value="remove">Remove</option>
      </select>
      <input id="bulk-tag" bind:value={tagInput} placeholder="Keep" aria-label="Bulk tag" title="One exact label; leave blank to keep tags" disabled={busy} on:input={invalidate} />
    </div>
    <span class="condition-count">Labels for related rows. Click tags to search.</span>
  </div>
  <div class="bulk-actions bulk-wide">
    <button type="button" class="ghost" on:click={() => void run("preview")} disabled={busy || selectedTargetEmpty || filteredResultLoading}>Preview</button>
    <button type="button" class="primary" on:click={() => void run("apply")} disabled={busy || !hasPatch || targetEmpty || filteredResultLoading}>Apply</button>
    <button type="button" class="ghost" on:click={() => void run("undo")} disabled={busy} title="Undo the last bulk annotation">Undo</button>
    {#if count !== null}<span class="condition-count" role="status">{count.toLocaleString()} rows{resultKind === "applied" ? " applied" : resultKind === "restored" ? " restored" : ""}</span>{/if}
    {#if selectedTargetEmpty}<span class="condition-count">No rows selected</span>{/if}
  </div>
  {#if error}<p class="condition-count bulk-wide" role="alert">{error}</p>{/if}
</div>

<style>
  .bulk-form { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 8px; }
  .bulk-field { display: flex; flex-direction: column; align-items: stretch; gap: 4px; min-width: 0; color: var(--muted); font-size: 11px; }
  .bulk-field > label { color: var(--muted); }
  .bulk-wide { grid-column: 1 / -1; }
  .bulk-field input:not([type=checkbox]), .bulk-field select { min-width: 0; width: 100%; }
  .bulk-clear { display: inline-flex; align-items: center; gap: 5px; }
  .bulk-tag { display: flex; gap: 5px; }
  .bulk-tag select { flex: 0 0 84px; }
  .bulk-tag input { flex: 1; }
  .bulk-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 5px; }
  .bulk-actions button { padding: 4px 8px; }
  .bulk-form > p { margin: 0; }
</style>
