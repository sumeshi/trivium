<script lang="ts">
  import { onDestroy, tick } from "svelte";
  import { errorMessage } from "../../errors";
  import { createLatestRequest } from "../../latestRequest";
  import { appendGroupPath, GROUP_GRANULARITIES, GROUP_PAGE_SIZE, groupDisplayValue, groupRowsPage, isMissingGroup, MAX_GROUP_LEVELS, selectedGroupGranularities, sortGroupTree } from "../../grouping";
  import type { GroupGranularity, GroupSortDirection, GroupSortKey } from "../../grouping";
  import type { Backend } from "../../backend";
  import type { GroupTreeNode, GroupTreeResponse } from "../../types";
  import type { AppliedFilters } from "./state";
  import { annotationVersions } from "./state";
  import { useWorkspace } from "../../workspaceContext";

  const { backend, columnLabels, projectDetail, appliedFilters, conditionTree, createReadScope, goBack, canGoBack } = useWorkspace();
  const reads = createReadScope();
  const requests = createLatestRequest();
  export let open = true;
  export let groupColumns: string[] = [];
  export let granularities: Record<string, GroupGranularity> = {};
  export let sortBy: GroupSortKey = "name";
  export let sortDirection: GroupSortDirection = "asc";
  let panel: HTMLElement | null = null;
  let pendingColumn = "";
  let tree: GroupTreeResponse | null = null;
  let expanded = new Set<string>();
  let page = 0;
  let error = "";
  let loading = false;
  let lastKey = "";
  let refreshRevision = 0;

  onDestroy(() => { reads.dispose(); requests.dispose(); });

  $: columns = $projectDetail?.columns ?? [];
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: availableColumns = columns.filter((name) => !groupColumns.includes(name));
  $: if (!availableColumns.includes(pendingColumn)) pendingColumn = availableColumns[0] ?? "";
  $: annotationRevision = $annotationVersions[projectId] ?? 0;
  $: filterKey = JSON.stringify([
    $appliedFilters.search.trim(), $appliedFilters.search.trim() ? $appliedFilters.searchColumns : [],
    $appliedFilters.flag, $appliedFilters.bookmarkedOnly, $appliedFilters.condition,
  ]);
  $: selectedGranularities = selectedGroupGranularities(groupColumns, granularities);
  $: requestKey = JSON.stringify([projectId, groupColumns, selectedGranularities, annotationRevision, filterKey, refreshRevision]);
  $: if (!open || !$backend || !projectId || groupColumns.length === 0) {
    reads.cancelAll();
    requests.invalidate();
    lastKey = "";
    tree = null;
    loading = false;
  }
  $: if (open && $backend && projectId && groupColumns.length > 0 && requestKey !== lastKey) {
    lastKey = requestKey;
    void loadTree($backend, projectId, [...groupColumns], [...selectedGranularities], $appliedFilters);
  }
  $: sortedNodes = sortGroupTree(tree?.nodes ?? [], sortBy, sortDirection);
  $: presentation = groupRowsPage(sortedNodes, expanded, page);

  export async function groupByColumn(columnId: string): Promise<void> {
    if (!columns.includes(columnId) || groupColumns.includes(columnId)) return;
    if (groupColumns.length >= MAX_GROUP_LEVELS) {
      error = `Use up to ${MAX_GROUP_LEVELS} group levels. Remove a level before adding another.`;
      return;
    }
    groupColumns = [...groupColumns, columnId];
    await tick();
  }

  function moveColumn(index: number, delta: -1 | 1) {
    const next = index + delta;
    if (next < 0 || next >= groupColumns.length) return;
    const copy = [...groupColumns];
    [copy[index], copy[next]] = [copy[next], copy[index]];
    groupColumns = copy;
  }

  async function replaceColumn(index: number, column: string) {
    if (groupColumns[index] === column || !columns.includes(column) || groupColumns.some((name, position) => position !== index && name === column)) return;
    groupColumns = groupColumns.map((name, position) => position === index ? column : name);
    await tick();
    if (panel?.isConnected) panel.querySelector<HTMLElement>(`#group-level-${index}`)?.focus({ preventScroll: true });
  }

  function clearGrouping() {
    groupColumns = [];
    granularities = {};
    expanded = new Set();
    page = 0;
    error = "";
  }

  function changeGranularity(name: string, event: Event) {
    const value = (event.currentTarget as HTMLSelectElement).value as GroupGranularity;
    granularities = { ...granularities, [name]: value };
  }

  function changeSort(event: Event) {
    sortBy = (event.currentTarget as HTMLSelectElement).value as GroupSortKey;
    sortDirection = sortBy === "name" ? "asc" : "desc";
    page = 0;
  }

  function toggleSortDirection() {
    sortDirection = sortDirection === "asc" ? "desc" : "asc";
    page = 0;
  }

  function toggle(key: string) {
    const next = new Set(expanded);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    expanded = next;
    page = 0;
  }

  async function loadTree(api: Backend, activeProjectId: string, selected: string[], selectedGranularities: GroupGranularity[], spec: AppliedFilters) {
    reads.cancel("groups");
    const ticket = requests.next();
    tree = null;
    error = "";
    loading = true;
    page = 0;
    expanded = new Set();
    try {
      const next = await reads.run("groups", api, (requestId) => api.groupTree({
        requestId, projectId: activeProjectId, groupColumns: selected,
        groupGranularities: selectedGranularities,
        search: spec.search.trim() || undefined,
        searchColumns: [...spec.searchColumns], flagFilter: spec.flag,
        bookmarkedOnly: spec.bookmarkedOnly, condition: spec.condition,
      }));
      if (requests.isCurrent(ticket)) tree = next;
    } catch (err) {
      if (requests.isCurrent(ticket)) error = errorMessage(err);
    } finally {
      if (requests.isCurrent(ticket)) loading = false;
    }
  }

  function applyPath(path: GroupTreeNode[]) {
    if (!tree || loading) return;
    const columns = tree.columns;
    conditionTree.update((root) => appendGroupPath(root, columns, path));
  }
</script>

<section class="grouping-panel" aria-label="Nested grouping" bind:this={panel}>
  <div class="group-setup-heading">
    <span class="section-label">Grouping</span>
    <button type="button" class="text-button" on:click={goBack} disabled={!$canGoBack}
      aria-label="Back to previous filter" title="Undo the last Rules change">← Back</button>
    <button type="button" class="text-button" on:click={clearGrouping} disabled={groupColumns.length === 0}
      aria-label="Clear grouping" title="Remove grouping levels; keep Rules">Ungroup</button>
    <button type="button" class="icon-button" on:click={() => refreshRevision += 1}
      disabled={loading || groupColumns.length === 0} title="Recalculate groups" aria-label="Recalculate groups">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M20 7v5h-5M4 17v-5h5" /><path d="M6.1 7a7 7 0 0 1 11.6-2L20 8M4 16l2.3 3A7 7 0 0 0 17.9 17" /></svg>
    </button>
  </div>
  {#if groupColumns.length > 0}
    <ol class="group-levels" aria-label="Group levels">
      {#each groupColumns as name, index (name)}
        <li data-group-column={name} style={`--level-depth: ${index}`}>
          <label class="level-label" for={`group-level-${index}`}>{index === 0 ? "Group by" : "Then by"}</label>
          <select class="level-column" id={`group-level-${index}`} aria-label={`Group level ${index + 1} column`}
            value={name} title={$columnLabels.get(name) ?? name}
            on:change={event => replaceColumn(index, event.currentTarget.value)}>
            {#each columns.filter(column => column === name || !groupColumns.includes(column)) as column}
              <option value={column}>{$columnLabels.get(column) ?? column}</option>
            {/each}
          </select>
          {#if $projectDetail?.column_kinds[name] === "datetime" || selectedGranularities[index] !== "exact"}
            <select class="group-granularity" aria-label={`Group ${$columnLabels.get(name) ?? name} by`} title="Datetime groups use UTC"
              value={selectedGranularities[index]} on:change={(event) => changeGranularity(name, event)}>
              {#each GROUP_GRANULARITIES as [value, label]}<option {value}>{label}</option>{/each}
            </select>
          {/if}
          <button type="button" class="icon-button" on:click={() => moveColumn(index, -1)} disabled={index === 0}
            aria-label={`Move group ${$columnLabels.get(name) ?? name} up`} title="Move level up">↑</button>
          <button type="button" class="icon-button" on:click={() => moveColumn(index, 1)} disabled={index === groupColumns.length - 1}
            aria-label={`Move group ${$columnLabels.get(name) ?? name} down`} title="Move level down">↓</button>
          <button type="button" class="icon-button" on:click={() => groupColumns = groupColumns.filter((_, position) => position !== index)}
            aria-label={`Remove group ${$columnLabels.get(name) ?? name}`} title="Remove group level">×</button>
        </li>
      {/each}
    </ol>
  {/if}
  <div class="add-level">
    <label class="level-label" for="group-next-column">{groupColumns.length ? "Then by" : "Group by"}</label>
    <select id="group-next-column" bind:value={pendingColumn} aria-label="Group column" disabled={!pendingColumn || groupColumns.length >= MAX_GROUP_LEVELS}>
      {#each availableColumns as name}
        <option value={name}>{$columnLabels.get(name) ?? name}</option>
      {/each}
    </select>
    <button type="button" class="ghost" on:click={() => groupByColumn(pendingColumn)}
      disabled={!pendingColumn || groupColumns.length >= MAX_GROUP_LEVELS} title="Add a nested group level">Add level</button>
  </div>
  {#if error}<p class="panel-error" role="alert">{error}</p>{/if}
  {#if loading}
    <p class="group-summary" role="status">Grouping current results…</p>
  {:else if tree}
    <div class="group-summary">
      <span><strong class="section-label">Values</strong> · {tree.total.toLocaleString()} rows · {tree.nodes.length.toLocaleString()} groups</span>
      <div class="group-sort">
        <select aria-label="Sort groups by" title="Sort each group level" value={sortBy} on:change={changeSort}>
          <option value="name">Name</option><option value="count">Count</option>
        </select>
        <button type="button" class="icon-button" on:click={toggleSortDirection}
          aria-label={sortDirection === "asc" ? "Sort groups descending" : "Sort groups ascending"}
          title={sortDirection === "asc" ? "Ascending — switch to descending" : "Descending — switch to ascending"}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d={sortDirection === "asc" ? "M7 19V5m-4 4 4-4 4 4M14 7h7m-7 5h5m-5 5h3" : "M7 5v14m-4-4 4 4 4-4M14 7h3m-3 5h5m-5 5h7"} /></svg>
        </button>
        <button type="button" class="text-button" on:click={() => { expanded = new Set(); page = 0; }} disabled={expanded.size === 0}>Collapse all</button>
      </div>
    </div>
    {#if tree.nodes.length === 0}<p class="group-summary">No groups in the current result.</p>{/if}
    <div class="group-tree" aria-label="Group results">
      {#if presentation.rows.length}
        <div class="group-result-heading" aria-hidden="true"><span>Value</span><span>Count</span></div>
      {/if}
      {#each presentation.rows as row (row.key)}
        <div class="group-row" data-depth={row.depth} data-group-label={row.node.label} data-group-missing={isMissingGroup(row.node)} style={`--group-depth: ${row.depth}`}>
          {#if row.node.children.length > 0}
            <button type="button" class="expand-group" on:click={() => toggle(row.key)}
              aria-expanded={expanded.has(row.key)} aria-label={`${expanded.has(row.key) ? "Collapse" : "Expand"} group ${groupDisplayValue(row.node)}`}
              title={expanded.has(row.key) ? "Collapse nested groups" : "Expand nested groups"}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" class:expanded={expanded.has(row.key)}><path d="m9 5 7 7-7 7" /></svg>
            </button>
          {:else}<span class="leaf-mark" aria-hidden="true">·</span>{/if}
          <button type="button" class="group-value" on:click={() => applyPath(row.path)}
            title={`Filter by ${row.path.map(groupDisplayValue).join(" / ")} (${row.node.count.toLocaleString()} rows)`}>
            <span class="group-value-label">{groupDisplayValue(row.node)}</span><span class="group-count">{row.node.count.toLocaleString()}</span>
          </button>
        </div>
      {/each}
    </div>
    {#if presentation.pages > 1}
      <nav class="group-pages" aria-label="Group result pages">
        <button type="button" class="ghost" on:click={() => page = presentation.page - 1} disabled={presentation.page === 0} aria-label="Previous group page">Previous</button>
        <span>{presentation.page + 1} / {presentation.pages} · {GROUP_PAGE_SIZE} per page</span>
        <button type="button" class="ghost" on:click={() => page = presentation.page + 1} disabled={presentation.page + 1 >= presentation.pages} aria-label="Next group page">Next</button>
      </nav>
    {/if}
  {:else if !loading && !error}
    <p class="group-summary">Choose a column to group.</p>
  {/if}
</section>

<style>
  .grouping-panel { display: grid; gap: 9px; min-width: 0; }
  .group-setup-heading { display: flex; align-items: center; gap: 5px; }
  .section-label { font-weight: 600; color: var(--fg); font-size: 10px; }
  .group-setup-heading > .section-label { flex: 1; }
  .add-level { display: flex; align-items: center; gap: 5px; }
  .add-level select { flex: 1; min-width: 0; }
  .add-level .ghost { flex: 0 0 auto; font-size: 11px; }
  .icon-button { display: inline-flex; align-items: center; justify-content: center; flex: 0 0 24px; height: 24px; padding: 0; border: 1px solid transparent; border-radius: 3px; background: transparent; color: var(--muted); }
  .icon-button:hover:not(:disabled), .text-button:hover:not(:disabled) { color: var(--fg); background: var(--accent-soft); }
  .icon-button:focus-visible, .expand-group:focus-visible, .group-value:focus-visible, .text-button:focus-visible { outline: 1px solid var(--accent); outline-offset: 1px; }
  button:disabled { opacity: .4; }
  .group-levels { list-style: none; padding: 0; margin: 0; display: grid; gap: 3px; }
  .group-levels li { display: flex; align-items: center; gap: 5px; min-width: 0; padding-left: calc(5px + min(var(--level-depth), 3) * 5px); border-left: 2px solid var(--border); }
  .level-label { flex: 0 0 47px; color: var(--muted); font-size: 10px; white-space: nowrap; }
  .level-column { flex: 1; min-width: 0; width: 0; height: 27px; padding: 0 5px; font-size: 11px; }
  .group-granularity { flex: 0 0 102px; width: 102px; height: 25px; padding: 0 3px; font-size: 10px; }
  .group-summary { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px; margin: 0; padding-top: 8px; border-top: 1px solid var(--border); font-size: 10px; color: var(--muted); }
  .group-sort { display: flex; align-items: center; gap: 3px; }
  .group-sort select { flex: 0 0 auto; width: auto; min-width: 65px; height: 24px; padding: 2px 4px; font-size: 10px; }
  .group-sort .text-button { white-space: nowrap; }
  .text-button { background: transparent; border: 0; padding: 2px 4px; border-radius: 3px; color: var(--muted); font-size: 10px; }
  .panel-error { margin: 0; color: var(--critical); font-size: 11px; }
  .group-tree { display: grid; gap: 2px; min-width: 0; }
  .group-result-heading { display: flex; justify-content: space-between; padding: 0 6px 3px 22px; color: var(--muted); font-size: 10px; }
  .group-row { display: flex; align-items: center; gap: 3px; padding-left: calc(var(--group-depth) * 13px); min-width: 0; }
  .expand-group, .leaf-mark { display: inline-flex; align-items: center; justify-content: center; width: 19px; height: 25px; flex: 0 0 19px; background: transparent; border: 0; padding: 0; color: var(--muted); }
  .expand-group:hover { color: var(--fg); }
  .expand-group svg.expanded { transform: rotate(90deg); }
  .group-value { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex: 1; min-width: 0; min-height: 25px; padding: 3px 6px; background: var(--accent-soft); border: 1px solid transparent; border-radius: 3px; color: var(--fg); font-size: 11px; text-align: left; }
  .group-value:hover { border-color: var(--accent-border); }
  .group-value-label { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .group-count { flex: 0 0 auto; color: var(--muted); font-variant-numeric: tabular-nums; }
  .group-pages { display: flex; align-items: center; justify-content: space-between; gap: 5px; font-size: 10px; color: var(--muted); }
  .group-pages .ghost { font-size: 10px; }
</style>
