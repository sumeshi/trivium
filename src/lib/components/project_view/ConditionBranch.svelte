<script lang="ts">
  import { get } from 'svelte/store';
  import { onMount } from 'svelte';
  import type { CompareOp } from "../../types";
  import type { EditorNode } from "../../stores/condition";
  import ConditionValueInput from "./ConditionValueInput.svelte";
  import FormatConditionFields from './FormatConditionFields.svelte';
  import { formatCatalogError, ensureFormatsLoaded } from '../../patternCatalog';

  import { COMPARE_OPS } from '../../stores/condition';
  import { useWorkspace } from "../../workspaceContext";
  const {
    columnLabels, addCondition, addGroup, moveNode, patchCondition, removeNode,
    setGroupKind, toggleCollapsed, backend, groupRuleIds,
  } = useWorkspace();

  export let node: EditorNode;
  export let columns: string[] = [];
  export let depth = 0;
  export let index = 0;
  export let siblingCount = 1;
  export let isRoot = false;
  $: managed = node.type === 'condition' && $groupRuleIds.includes(node.id);
  onMount(() => {
    const service = get(backend);
    if (isRoot && service) void ensureFormatsLoaded(service).catch(() => {});
  });

  const setOp = (id: string, value: string) => {
    patchCondition(id, { op: value as CompareOp });
  };

  const setKind = (id: string, value: string) => {
    if (value === "and" || value === "or") {
      setGroupKind(id, value);
    }
  };


</script>

<div class="condition-branch" data-depth={depth}>
  {#if node.type === "group"}
    <div class="condition-group" data-kind={node.kind}>
      <div class="group-toolbar">
        <button type="button" class="branch-icon" on:click={() => toggleCollapsed(node.id)} aria-expanded={!node.collapsed} aria-label={`${node.collapsed ? "Expand" : "Collapse"} ${node.kind.toUpperCase()} group`} title={`${node.collapsed ? "Expand" : "Collapse"} group`}>
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d={node.collapsed ? "m9 5 7 7-7 7" : "m5 9 7 7 7-7"} /></svg>
        </button>
        <select
          class="group-kind"
          value={node.kind}
          on:change={(event) => setKind(node.id, event.currentTarget.value)}
          aria-label="Combine conditions"
          title={node.kind === "and" ? "All children must match" : "Any child may match"}
        >
          <option value="and">AND</option>
          <option value="or">OR</option>
        </select>
        <span class="group-size" title={`Conditions and groups: ${node.children.length}`}>{node.children.length}</span>
        <button type="button" class="group-add" on:click={() => addCondition(node.id, columns[0] ?? "")} aria-label="Add condition" title="Add condition">
          <span aria-hidden="true">＋</span><span>Condition</span>
        </button>
        <button type="button" class="group-add" on:click={() => addGroup(node.id)} aria-label="Add group" title="Add nested AND/OR group">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M8 4H4v16h4m8-16h4v16h-4M8 12h8m-4-4v8" /></svg><span>Group</span>
        </button>
        {#if !isRoot}
          <button type="button" class="branch-icon" disabled={index === 0} on:click={() => moveNode(node.id, -1)} aria-label="Move group up" title="Move group up">
            <span aria-hidden="true">↑</span>
          </button>
          <button
            type="button"
            class="branch-icon"
            disabled={index >= siblingCount - 1}
            on:click={() => moveNode(node.id, 1)}
            aria-label="Move group down"
            title="Move group down"
          >
            <span aria-hidden="true">↓</span>
          </button>
          <button type="button" class="branch-icon" on:click={() => removeNode(node.id)} aria-label="Remove group" title="Remove group"><span aria-hidden="true">×</span></button>
        {/if}
      </div>
      {#if isRoot && $formatCatalogError}<p class="format-load-error" role="alert">{$formatCatalogError}</p>{/if}
      {#if !node.collapsed}
        <div class="group-children" role="group" aria-label={node.kind === "and" ? "All conditions must match" : "Any condition may match"}>
        {#each node.children as child, childIndex (child.id)}
          {#if childIndex > 0}<div class="condition-connector" data-kind={node.kind} aria-hidden="true"><span>{node.kind.toUpperCase()}</span></div>{/if}
          <svelte:self
            node={child}
            {columns}
            depth={depth + 1}
            index={childIndex}
            siblingCount={node.children.length}
          />
        {/each}
        {#if node.children.length === 0}<p class="empty-group">Add a condition or nested group.</p>{/if}
        </div>
      {/if}
    </div>
  {:else if node.type === 'format'}
    <div class="condition-row">
      <FormatConditionFields {node} {columns} />
      <div class="condition-actions">
        <button type="button" class="branch-icon" disabled={index === 0} on:click={() => moveNode(node.id, -1)} aria-label="Move condition up" title="Move condition up"><span aria-hidden="true">↑</span></button>
        <button type="button" class="branch-icon" disabled={index >= siblingCount - 1} on:click={() => moveNode(node.id, 1)} aria-label="Move condition down" title="Move condition down"><span aria-hidden="true">↓</span></button>
        <button type="button" class="branch-icon" on:click={() => removeNode(node.id)} aria-label="Remove condition" title="Remove condition"><span aria-hidden="true">×</span></button>
      </div>
    </div>
  {:else if managed}
    <div class="managed-rule" role="group" aria-label="Rule from Groups" data-condition-id={node.id}>
      <span class="managed-source" title="Managed by Groups. Use Make rule to edit.">Group</span>
      <span class="managed-column" title={$columnLabels.get(node.column) ?? node.column}>{$columnLabels.get(node.column) ?? node.column}</span>
      <span class="managed-operator">{COMPARE_OPS.find(op => op.value === node.op)?.label ?? node.op}</span>
      <span class="managed-value" title={node.value}>{node.value === '' ? '(empty string)' : node.value}</span>
    </div>
  {:else}
    <div class="condition-row">
      <select
        class="condition-column"
        value={node.column}
        on:change={(event) => patchCondition(node.id, { column: event.currentTarget.value })}
        aria-label="Column"
      >
        {#each columns as column}
          <option value={column}>{$columnLabels.get(column) ?? column}</option>
        {/each}
      </select>
      <select
        class="condition-operator"
        value={node.op}
        on:change={(event) => setOp(node.id, event.currentTarget.value)}
        aria-label="Operator"
      >
        {#each COMPARE_OPS as op}
          <option value={op.value}>{op.label}</option>
        {/each}
      </select>
      <label class="condition-not">
        <input
          type="checkbox"
          checked={node.negated}
          on:change={(event) => patchCondition(node.id, { negated: event.currentTarget.checked })}
        />
        Not
      </label>
      <ConditionValueInput {node} />
      <div class="condition-actions">
      <button type="button" class="branch-icon" disabled={index === 0} on:click={() => moveNode(node.id, -1)} aria-label="Move condition up" title="Move condition up">
        <span aria-hidden="true">↑</span>
      </button>
      <button
        type="button"
        class="branch-icon"
        disabled={index >= siblingCount - 1}
        on:click={() => moveNode(node.id, 1)}
        aria-label="Move condition down"
        title="Move condition down"
      >
        <span aria-hidden="true">↓</span>
      </button>
      <button type="button" class="branch-icon" on:click={() => removeNode(node.id)} aria-label="Remove condition" title="Remove condition"><span aria-hidden="true">×</span></button>
      </div>
    </div>
  {/if}
</div>

<style>
  .format-load-error { margin: 4px 0; color: var(--danger); font-size: 10px; overflow-wrap: anywhere; }
  .condition-branch { gap: 0; min-width: 0; }
  .condition-group { gap: 5px; min-width: 0; border: 0; padding: 0; }
  .group-toolbar { display: flex; align-items: center; flex-wrap: wrap; gap: 3px; padding: 2px 0; border-bottom: 1px solid var(--border); background: var(--surface-weak); }
  .group-toolbar .group-kind { width: 60px; min-height: 25px; height: 25px; padding: 0 3px; font-size: 11px; line-height: 1.2; font-weight: 500; color: var(--fg); border: 1px solid var(--border); border-radius: 3px; background: var(--surface); }
  .group-size { margin-right: auto; min-width: 16px; padding: 0 3px; text-align: center; color: var(--muted); font-size: 10px; line-height: 16px; font-variant-numeric: tabular-nums; }
  .group-children { display: flex; flex-direction: column; gap: 3px; margin-left: 4px; padding: 4px 0 4px 8px; border-left: 2px solid var(--border-strong); min-width: 0; }
  .condition-group[data-kind="or"] > .group-children { border-left-style: dashed; }
  .condition-connector { display: flex; align-items: center; gap: 6px; margin: 1px 0; color: var(--muted); font-size: 10px; line-height: 12px; font-weight: 400; }
  .condition-connector span { min-width: 24px; padding: 0 2px; text-align: center; }
  .condition-connector::after { content: ''; flex: 1; height: 1px; background: var(--border); }
  .condition-row { display: grid; grid-template-columns: minmax(0, 1fr) 72px 77px; align-items: center; gap: 4px; min-width: 0; }
  .condition-row select, .condition-row input { min-width: 0; width: 100%; height: 27px; padding: 0 6px; font-size: 11px; }
  .condition-row :global(.condition-value-input) { grid-column: 1 / 3; min-width: 0; }
  .condition-not { display: flex; flex-direction: row; align-items: center; justify-content: flex-end; gap: 3px; padding-right: 4px; font-size: 10px; color: var(--muted); }
  .condition-not input { width: 12px; height: 12px; }
  .managed-rule { display: grid; grid-template-columns: auto minmax(45px, .7fr) auto minmax(0, 1fr); align-items: center; gap: 6px; min-height: 27px; padding: 3px 5px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface-weak); font-size: 11px; }
  .managed-source { padding: 1px 4px; border: 1px solid var(--accent-border); border-radius: 3px; color: var(--accent); font-size: 10px; }
  .managed-column, .managed-value { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .managed-operator { color: var(--muted); }
  .condition-actions { display: flex; align-self: start; gap: 1px; justify-content: flex-end; }
  .branch-icon, .group-add { display: inline-flex; align-items: center; justify-content: center; gap: 3px; height: 25px; padding: 0; border: 1px solid transparent; border-radius: 3px; color: var(--muted); background: transparent; font-size: 13px; }
  .branch-icon { flex: 0 0 24px; width: 24px; }
  .group-add { flex: 0 0 auto; gap: 4px; padding: 0 6px; border-color: var(--border); background: var(--surface); color: var(--fg); font-size: 10px; white-space: nowrap; }
  .branch-icon:hover:not(:disabled) { border-color: var(--border); color: var(--fg); background: var(--surface-weak); }
  .group-add:hover { border-color: var(--border-strong); background: var(--hover); }
  .branch-icon:focus-visible, .group-add:focus-visible { outline: 1px solid var(--accent-border); outline-offset: 1px; }
  .group-add:focus-visible { outline-width: 2px; }
  .branch-icon:disabled { opacity: .35; }
  .empty-group { margin: 3px 0; font-size: 10px; color: var(--muted); }
</style>
