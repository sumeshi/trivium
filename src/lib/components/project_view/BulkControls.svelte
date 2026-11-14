<script lang="ts">
  import { createEventDispatcher, onDestroy } from "svelte";
  import { get } from "svelte/store";
  import { errorMessage } from "../../errors";
  import { applyBulkAnnotation, buildBulkAnnotateArgs, buildBulkPatch } from "../../bulkAnnotation";
  import type { BulkFlag, BulkTagOperation, BulkTarget } from "../../bulkAnnotation";
  import { FLAG_OPTIONS } from './state';
  import { useWorkspace } from "../../workspaceContext";

  const workspace = useWorkspace();
  const { appliedResultLoading, backend, projectDetail, selectedCount, totalFilteredRows } = workspace;
  const dispatch = createEventDispatcher<{ refresh: void }>();
  let target: BulkTarget = "filtered";
  let flag: BulkFlag = "keep";
  let memo = "";
  let clearMemo = false;
  let tagInput = "";
  let tagOperation: BulkTagOperation = "add";
  let count: number | null = null;
  let resultKind: "applied" | "restored" = "applied";
  let error = "";
  let busy = false;
  let alive = true;
  let generation = 0;

  const invalidate = () => {
    generation += 1;
    error = "";
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
    alive = false; subscriptions.forEach(stop => stop());
  };
  // Workspace teardown clears stores before child components unmount. Stop
  // observing those clears so a captured Apply can finish for its old case.
  const unregister = workspace.onDispose(cleanup);
  onDestroy(() => { unregister(); cleanup(); });

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
    sortDirection: get(workspace.sortDirection),
    patch: buildBulkPatch({ flag, memo, clearMemo, tag: tagInput, tagOperation }),
  });

  const chooseFlag = (value: BulkFlag) => {
    flag = flag === value ? "keep" : value;
    invalidate();
  };

  const run = async (operation: "apply" | "undo") => {
    const api = get(backend), owner = get(workspace.currentProjectId);
    if (!api || !owner || busy) return;
    if (operation === "apply" && (filteredResultLoading || !hasPatch || targetEmpty)) return;
    const ticket = generation;
    const owned = () => alive && get(workspace.currentProjectId) === owner;
    const current = () => owned() && generation === ticket && !(target === "filtered" && get(workspace.appliedResultLoading));
    busy = true; error = ""; count = null;
    try {
      if (operation === "undo") {
        const result = await api.bulkUndo({ projectId: owner });
        if (!owned()) return;
        count = result.updated; resultKind = "restored";
        dispatch("refresh");
      } else {
        const result = await applyBulkAnnotation(api, captureArgs(), {
          // A requested mutation survives closing its panel. While mounted,
          // changed form/query inputs cancel the pre-commit preview instead.
          isCurrent: () => generation === ticket && (!alive || current()),
        });
        if (!result) { if (owned()) error = "Target changed. Apply again."; return; }
        if (!owned()) return;
        count = result.updated; resultKind = "applied";
        dispatch("refresh");
      }
    } catch (err) {
      if (current()) {
        error = errorMessage(err);
      }
    } finally { if (alive) busy = false; }
  };
</script>

<div class="stats-panel bulk-form">
  <p class="condition-count bulk-wide bulk-intro">Apply flags, notes and tags to multiple rows. Unselected flags and blank fields stay unchanged.</p>
  <label class="bulk-field">Rows
    <select bind:value={target} aria-label="Bulk target" disabled={busy} on:change={invalidate}>
      <option value="selection" disabled={$selectedCount === 0}>Selected ({$selectedCount})</option>
      <option value="filtered">Current results ({filteredResultLoading ? '…' : $totalFilteredRows})</option>
      <option value="all">All rows ({allRowCount})</option>
    </select>
  </label>
  <div class="bulk-field">
    <span>Flag</span>
    <div class="bulk-flag-buttons" role="group" aria-label="Bulk flag">
      {#each FLAG_OPTIONS as option}
        <button type="button" class={`bulk-flag bulk-flag-${option.value}`} class:selected={flag === option.value}
          aria-label={`Set bulk flag ${option.label}`} aria-pressed={flag === option.value}
          title={`Set ${option.label}; click again to leave flags unchanged`} disabled={busy}
          on:click={() => chooseFlag(option.value)}>{option.hint} {option.label}</button>
      {/each}
      <button type="button" class="bulk-flag bulk-unflag" class:selected={flag === ''}
        aria-label="Clear bulk flags" aria-pressed={flag === ''}
        title="Remove flags from target rows; click again to leave flags unchanged" disabled={busy}
        on:click={() => chooseFlag('')}>–</button>
    </div>
  </div>
  <div class="bulk-field bulk-wide">
    <label for="bulk-note">Note</label>
    <input id="bulk-note" bind:value={memo} placeholder="Add or replace note" aria-label="Bulk note" disabled={busy || clearMemo} on:input={invalidate} />
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
      <input id="bulk-tag" bind:value={tagInput} placeholder="Tag name" aria-label="Bulk tag" title="One exact tag name; blank leaves tags unchanged" disabled={busy} on:input={invalidate} />
    </div>
    <span class="condition-count">Labels for related rows. Click tags to search.</span>
  </div>
  <div class="bulk-actions bulk-wide">
    <button type="button" class="primary" on:click={() => void run("apply")} disabled={busy || !hasPatch || targetEmpty || filteredResultLoading}>Apply</button>
    <button type="button" class="ghost" on:click={() => void run("undo")} disabled={busy} title="Undo the last bulk annotation">Undo</button>
    {#if count !== null}<span class="condition-count" role="status">{count.toLocaleString()} rows {resultKind}</span>{/if}
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
  .bulk-intro { line-height: 1.45; }
  .bulk-flag-buttons { display: flex; align-items: center; flex-wrap: wrap; gap: 4px; min-height: 31px; }
  .bulk-flag { padding: 4px 5px; border: 1px solid var(--border); border-radius: 4px; background: transparent; font: inherit; white-space: nowrap; }
  .bulk-flag-safe { color: var(--positive); }
  .bulk-flag-suspicious { color: var(--warning); }
  .bulk-flag-critical { color: var(--danger); }
  .bulk-unflag { min-width: 25px; color: var(--muted); }
  .bulk-flag:hover, .bulk-unflag.selected { background: var(--accent-soft); border-color: var(--accent-border); }
  .bulk-flag-safe.selected { background: var(--flag-safe-selected-bg, var(--positive)); color: var(--flag-safe-selected-fg, var(--flag-selected-text)); border-color: var(--flag-safe-selected-border, var(--positive)); }
  .bulk-flag-suspicious.selected { background: var(--flag-suspicious-selected-bg, var(--warning)); color: var(--flag-suspicious-selected-fg, var(--flag-selected-text)); border-color: var(--flag-suspicious-selected-border, var(--warning)); }
  .bulk-flag-critical.selected { background: var(--flag-critical-selected-bg, var(--danger)); color: var(--flag-critical-selected-fg, var(--flag-selected-text)); border-color: var(--flag-critical-selected-border, var(--danger)); }
  .bulk-flag:focus-visible { outline: 2px solid var(--accent); outline-offset: 1px; }
  .bulk-clear { display: inline-flex; align-items: center; gap: 5px; }
  .bulk-tag { display: flex; gap: 5px; }
  .bulk-tag select { flex: 0 0 84px; }
  .bulk-tag input { flex: 1; }
  .bulk-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 5px; }
  .bulk-actions button { padding: 4px 8px; }
  .bulk-form > p { margin: 0; }
</style>
