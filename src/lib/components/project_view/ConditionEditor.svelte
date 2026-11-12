<script lang="ts">

  import ConditionBranch from "./ConditionBranch.svelte";
  import { useWorkspace } from "../../workspaceContext";
  const { totalFilteredRows, projectDetail, conditionTree } = useWorkspace();

  $: columns = $projectDetail?.columns ?? [];
</script>

<div class="condition-editor">
  <div class="condition-toolbar">
    <span class="filter-label">Rules</span>
    <span class="condition-count">{$totalFilteredRows.toLocaleString()} rows</span>
  </div>
  <p class="rules-help">AND matches every child; OR matches any child. Changes apply automatically; value checks are advisory. Empty text rules start applying when you enter a value.</p>
  <ConditionBranch node={$conditionTree} {columns} isRoot />
</div>

<style>
  .condition-editor { gap: 10px; min-width: 0; }
  .condition-toolbar { gap: 7px; }
  .rules-help { margin: 0; font-size: 10px; line-height: 1.45; color: var(--muted); }
</style>
