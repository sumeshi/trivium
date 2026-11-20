<script lang="ts">

  import { createEventDispatcher } from "svelte";
  import ConditionBranch from "./ConditionBranch.svelte";
  import { useWorkspace } from "../../workspaceContext";
  const { projectDetail, conditionTree, groupRuleIds, makeGroupRulesEditable } = useWorkspace();
  const dispatch = createEventDispatcher<{ openGroups: void }>();

  $: columns = $projectDetail?.columns ?? [];
</script>

<div class="condition-editor">
  <p class="rules-help" title="AND requires every condition; OR accepts any condition. Blank text rules wait for a value.">Changes apply automatically: show rows matching these rules.</p>
  {#if $groupRuleIds.length}
    <div class="managed-group-actions" aria-label="Group rules">
      <span>From Groups</span>
      <button type="button" on:click={() => dispatch('openGroups')} title="Edit the selected group path">Open Groups</button>
      <button type="button" on:click={makeGroupRulesEditable} title="Keep this path as editable rules, independent of Groups">Make rule</button>
    </div>
  {/if}
  <ConditionBranch node={$conditionTree} {columns} isRoot />
</div>

<style>
  .condition-editor { gap: 7px; min-width: 0; }
  .rules-help { margin: 0; font-size: 10px; line-height: 1.45; color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .managed-group-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 5px; font-size: 11px; }
  .managed-group-actions span { margin-right: auto; color: var(--muted); }
  .managed-group-actions button { min-height: 25px; padding: 2px 6px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); font-size: 11px; }
  .managed-group-actions button:hover { border-color: var(--border-strong); background: var(--hover); }
  .managed-group-actions button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
</style>
