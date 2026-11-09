<script lang="ts">
  import { totalFilteredRows, projectDetail } from "./state";
  import { conditionTree } from "../../stores/condition";
  import ConditionBranch from "./ConditionBranch.svelte";

  $: columns = $projectDetail?.columns ?? [];
</script>

<div class="condition-editor">
  <div class="condition-toolbar">
    <span class="filter-label">Conditions</span>
    <span class="condition-count">{$totalFilteredRows} rows</span>
  </div>
  <ConditionBranch node={$conditionTree} {columns} isRoot />
</div>
