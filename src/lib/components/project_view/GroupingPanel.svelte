<script lang="ts">
  import { onDestroy } from "svelte";
  import type { Backend } from "../../backend";
  import type { ConditionNode, GroupTreeNode, GroupTreeResponse } from "../../types";
  import { conditionPayload, filterByValues } from "../../stores/condition";
  import { backend, flagFilter, projectDetail, search, visibleColumns } from "./state";
  import type { FlagFilterValue } from "./state";

  type FlatNode = {
    node: GroupTreeNode;
    depth: number;
    path: GroupTreeNode[];
    key: string;
  };

  let pendingColumn = "";
  let selectedColumns: string[] = [];
  let tree: GroupTreeResponse | null = null;
  let expanded = new Set<string>();
  let error = "";
  let loading = false;
  let request = 0;
  let alive = true;
  let loadedProjectId = "";

  onDestroy(() => {
    alive = false;
  });

  $: columns = $projectDetail?.columns ?? [];
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: if (projectId && !columns.includes(pendingColumn)) {
    pendingColumn = columns[0] ?? "";
  }
  $: if (projectId && projectId !== loadedProjectId) {
    loadedProjectId = projectId;
    selectedColumns = [];
    tree = null;
    expanded = new Set<string>();
    error = "";
  }
  $: if (selectedColumns.some((name) => !columns.includes(name))) {
    selectedColumns = selectedColumns.filter((name) => columns.includes(name));
    tree = null;
  }

  const nodeKey = (path: GroupTreeNode[]) => path.map((node) => node.label).join("\u0001");

  const flatten = (
    nodes: GroupTreeNode[],
    depth: number,
    parents: GroupTreeNode[],
    out: FlatNode[],
    open: Set<string>
  ) => {
    for (const node of nodes) {
      const path = [...parents, node];
      const key = nodeKey(path);
      out.push({ node, depth, path, key });
      if (node.children.length > 0 && open.has(key)) {
        flatten(node.children, depth + 1, path, out, open);
      }
    }
  };

  $: rows = tree
    ? (() => {
        const out: FlatNode[] = [];
        flatten(tree.nodes, 0, [], out, expanded);
        return out;
      })()
    : [];

  const addColumn = () => {
    if (!pendingColumn || selectedColumns.includes(pendingColumn)) {
      return;
    }
    selectedColumns = [...selectedColumns, pendingColumn];
    tree = null;
  };

  const removeColumn = (index: number) => {
    selectedColumns = selectedColumns.filter((_, position) => position !== index);
    tree = null;
  };

  const moveColumn = (index: number, delta: -1 | 1) => {
    const next = index + delta;
    if (next < 0 || next >= selectedColumns.length) {
      return;
    }
    const copy = [...selectedColumns];
    [copy[index], copy[next]] = [copy[next], copy[index]];
    selectedColumns = copy;
    tree = null;
  };

  const toggle = (key: string) => {
    const next = new Set(expanded);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
    }
    expanded = next;
  };

  const loadTree = async (
    api: Backend,
    activeProjectId: string,
    groupColumns: string[],
    activeSearch: string,
    activeFlag: FlagFilterValue,
    activeColumns: string[],
    activeCondition: ConditionNode | null
  ) => {
    if (groupColumns.length === 0) {
      tree = null;
      error = "Add at least one group column.";
      return;
    }
    const ticket = ++request;
    loading = true;
    error = "";
    try {
      const next = await api.groupTree({
        projectId: activeProjectId,
        groupColumns,
        search: activeSearch.trim() || undefined,
        flagFilter: activeFlag,
        columns: activeColumns,
        condition: activeCondition,
      });
      if (!alive || ticket !== request) {
        return;
      }
      tree = next;
      expanded = new Set(next.nodes.map((node) => nodeKey([node])));
    } catch (err) {
      if (!alive || ticket !== request) {
        return;
      }
      tree = null;
      error = err instanceof Error ? err.message : String(err);
    } finally {
      if (alive && ticket === request) {
        loading = false;
      }
    }
  };

  const refresh = () => {
    if (!$backend || !projectId) {
      return;
    }
    void loadTree(
      $backend,
      projectId,
      selectedColumns,
      $search,
      $flagFilter,
      $visibleColumns,
      $conditionPayload
    );
  };

  const applyPath = (path: GroupTreeNode[]) => {
    if (!tree) {
      return;
    }
    const filters = path
      .map((node, depth) => ({
        column: tree?.columns[depth] ?? selectedColumns[depth] ?? "",
        value: node.label,
      }))
      .filter((filter) => filter.column.length > 0);
    if (filters.length > 0) {
      filterByValues(filters);
    }
  };
</script>

<div class="stats-panel">
  <div class="condition-toolbar">
    <span class="filter-label">Grouping</span>
    <select bind:value={pendingColumn} aria-label="Group column">
      {#each columns as name}
        <option value={name}>{name}</option>
      {/each}
    </select>
    <button type="button" class="ghost" on:click={addColumn} disabled={!pendingColumn}>
      Add
    </button>
    <button
      type="button"
      class="primary"
      on:click={refresh}
      disabled={loading || selectedColumns.length === 0}
    >
      {loading ? "Loading…" : "Refresh"}
    </button>
    {#if tree}
      <span class="condition-count">{tree.total} rows</span>
    {/if}
  </div>
  {#if selectedColumns.length > 0}
    <div class="condition-toolbar">
      <span class="condition-count">Levels</span>
      {#each selectedColumns as name, index (name)}
        <span class="condition-count">{index + 1}. {name}</span>
        <button
          type="button"
          class="ghost"
          on:click={() => moveColumn(index, -1)}
          disabled={index === 0}
          aria-label={`Move ${name} up`}
        >
          ↑
        </button>
        <button
          type="button"
          class="ghost"
          on:click={() => moveColumn(index, 1)}
          disabled={index === selectedColumns.length - 1}
          aria-label={`Move ${name} down`}
        >
          ↓
        </button>
        <button
          type="button"
          class="ghost"
          on:click={() => removeColumn(index)}
          aria-label={`Remove ${name}`}
        >
          ×
        </button>
      {/each}
    </div>
  {/if}
  {#if error}
    <p class="condition-count">{error}</p>
  {:else if tree}
    <div class="grouping-tree">
      {#each rows as row (row.key)}
        <div class="condition-toolbar" style={`padding-left: ${row.depth * 16}px;`}>
          {#if row.node.children.length > 0}
            <button
              type="button"
              class="ghost"
              on:click={() => toggle(row.key)}
              aria-label={expanded.has(row.key) ? "Collapse group" : "Expand group"}
            >
              {expanded.has(row.key) ? "−" : "+"}
            </button>
          {/if}
          <button type="button" class="ghost" on:click={() => applyPath(row.path)}>
            {row.node.label || "(empty)"} ({row.node.count})
          </button>
        </div>
      {/each}
    </div>
  {/if}
</div>
