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
  <div class="rules-actions" class:managed-group-actions={$groupRuleIds.length > 0} aria-label="Rule tools">
    {#if $groupRuleIds.length}
      <span>From Groups</span>
      <button type="button" on:click={makeGroupRulesEditable} title="Keep this path as editable rules, independent of Groups">Make rule</button>
    {/if}
    <button type="button" class="open-groups" on:click={() => dispatch('openGroups')} title="Open grouping tools">Open Groups</button>
  </div>
  <ConditionBranch node={$conditionTree} {columns} isRoot />
</div>

<style>
  .condition-editor { gap: 7px; min-width: 0; }
  .rules-help { margin: 0; font-size: 10px; line-height: 1.45; color: var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .rules-actions { display: flex; align-items: center; flex-wrap: wrap; gap: 5px; font-size: 11px; }
  .rules-actions span { color: var(--muted); }
  .rules-actions button { min-height: 25px; padding: 2px 6px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); font-size: 11px; }
  .rules-actions .open-groups { margin-left: auto; }
  .rules-actions button:hover { border-color: var(--border-strong); background: var(--hover); }
  .rules-actions button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
</style>
