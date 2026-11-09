<script lang="ts">
  import { totalFilteredRows, projectDetail } from "./state";
  import { conditionTree } from "../../stores/condition";
  import { canGoBack, canGoForward, goBack, goForward } from "../../stores/viewHistory";
  import ConditionBranch from "./ConditionBranch.svelte";

  $: columns = $projectDetail?.columns ?? [];
</script>

<div class="condition-editor">
  <div class="condition-toolbar">
    <span class="filter-label">Conditions</span>
    <span class="condition-count">{$totalFilteredRows} rows</span>
    <button type="button" class="ghost" disabled={!$canGoBack} on:click={goBack}>Back</button>
    <button type="button" class="ghost" disabled={!$canGoForward} on:click={goForward}>Forward</button>
  </div>
  <ConditionBranch node={$conditionTree} {columns} isRoot />
</div>
