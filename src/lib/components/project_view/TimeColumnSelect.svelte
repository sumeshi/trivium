<script lang="ts">
  import { datetimeAnalysisColumns, initialDatetimeColumn } from "../../analysisColumns";
  import { useWorkspace } from "../../workspaceContext";
  const { columnLabels, projectDetail } = useWorkspace();

  export let column = "";
  export let label = "Datetime column";
  let previousProject = "";

  $: columns = $projectDetail?.columns ?? [];
  $: kinds = $projectDetail?.column_kinds ?? {};
  $: choices = datetimeAnalysisColumns(columns, kinds);
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: if (projectId !== previousProject || (column && !columns.includes(column))) {
    previousProject = projectId;
    column = initialDatetimeColumn(columns, kinds);
  }
</script>

<label title="Analyze this column as datetime. Missing and unparseable values are counted and skipped; source values stay unchanged.">
  Column
  <select bind:value={column} aria-label={label}>
    <option value="">Choose a datetime column</option>
    {#if choices.inferred.length > 0}
      <optgroup label="Datetime columns">
        {#each choices.inferred as name}<option value={name}>{$columnLabels.get(name) ?? name}</option>{/each}
      </optgroup>
    {/if}
    {#if choices.other.length > 0}
      <optgroup label="Other columns — interpret as datetime">
        {#each choices.other as name}<option value={name}>{$columnLabels.get(name) ?? name}</option>{/each}
      </optgroup>
    {/if}
  </select>
</label>

<style>
  label { display: flex; align-items: center; gap: 8px; color: var(--muted); font-size: 11px; min-width: 0; }
  select { flex: 1; min-width: 0; max-width: 100%; }
</style>
