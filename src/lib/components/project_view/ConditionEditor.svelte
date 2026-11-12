<script lang="ts">

  import ConditionBranch from "./ConditionBranch.svelte";
  import { useWorkspace } from "../../workspaceContext";
  const { totalFilteredRows, projectDetail, conditionTree } = useWorkspace();

  $: columns = $projectDetail?.columns ?? [];
</script>

<div class="condition-editor">
  <div class="condition-toolbar">
    <span class="filter-label">Conditions</span>
    <span class="condition-count">{$totalFilteredRows.toLocaleString()} rows</span>
  </div>
  <ConditionBranch node={$conditionTree} {columns} isRoot />
</div>

<style>
  .condition-editor { gap: 10px; min-width: 0; }
  .condition-toolbar { gap: 7px; }
</style>
