<script lang="ts">
  import type { CompareOp } from "../../types";
  import type { EditorNode } from "../../stores/condition";
  import { columnLabels, projectDetail } from "./state";
  import {
    COMPARE_OPS,
    addCondition,
    addGroup,
    moveNode,
    patchCondition,
    removeNode,
    setGroupKind,
    toggleCollapsed,
  } from "../../stores/condition";

  export let node: EditorNode;
  export let columns: string[] = [];
  export let depth = 0;
  export let index = 0;
  export let siblingCount = 1;
  export let isRoot = false;

  const setOp = (id: string, value: string) => {
    patchCondition(id, { op: value as CompareOp });
  };

  const setKind = (id: string, value: string) => {
    if (value === "and" || value === "or") {
      setGroupKind(id, value);
    }
  };

  const valueInput = (column: string): { type: string; inputmode?: "numeric" | "decimal"; step?: string } => {
    const kind = $projectDetail?.column_kinds?.[column];
    // Integer text stays a string. type=number would round values above 2^53.
    if (kind === "int") {
      return { type: "text", inputmode: "numeric" };
    }
    if (kind === "float") {
      return { type: "number", inputmode: "decimal", step: "any" };
    }
    if (kind === "datetime") {
      return { type: "datetime-local", step: "1" };
    }
    return { type: "text" };
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
          value={node.kind}
          on:change={(event) => setKind(node.id, event.currentTarget.value)}
          aria-label="Combine conditions"
          title={node.kind === "and" ? "All children must match" : "Any child may match"}
        >
          <option value="and">AND</option>
          <option value="or">OR</option>
        </select>
        <span class="group-description">{node.kind === "and" ? "Match all" : "Match any"}<span class="group-size">{node.children.length}</span></span>
        <button type="button" class="group-add" on:click={() => addCondition(node.id, columns[0] ?? "")} aria-label="Add condition" title="Add condition">
          <span aria-hidden="true">＋</span><span>Condition</span>
        </button>
        <button type="button" class="branch-icon" on:click={() => addGroup(node.id)} aria-label="Add group" title="Add nested AND/OR group"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M8 4H4v16h4m8-16h4v16h-4M8 12h8m-4-4v8" /></svg></button>
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
      {#if !node.collapsed}
        <div class="group-children">
        {#each node.children as child, childIndex (child.id)}
          {#if childIndex > 0}<span class="condition-connector">{node.kind.toUpperCase()}</span>{/if}
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
      <input
        class="condition-value"
        type={node.exact ? "text" : valueInput(node.column).type}
        inputmode={node.exact ? undefined : valueInput(node.column).inputmode}
        step={node.exact ? undefined : valueInput(node.column).step}
        value={node.value}
        placeholder={node.op === "in" ? "a, b, c" : "Value"}
        on:input={(event) => patchCondition(node.id, { value: event.currentTarget.value, exact: false })}
        aria-label="Value"
      />
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
  .condition-branch { gap: 0; min-width: 0; }
  .condition-group { gap: 5px; min-width: 0; border: 0; padding: 0; }
  .group-toolbar { display: flex; align-items: center; flex-wrap: wrap; gap: 3px; }
  .group-toolbar select { width: 59px; height: 26px; padding: 0 4px; font-size: 10px; font-weight: 600; color: var(--accent); }
  .group-description { display: inline-flex; align-items: center; gap: 5px; flex: 1; min-width: 65px; font-size: 10px; color: var(--muted); }
  .group-size { font-size: 9px; font-variant-numeric: tabular-nums; }
  .group-children { display: flex; flex-direction: column; gap: 3px; margin-left: 10px; padding: 3px 0 4px 10px; border-left: 1px solid var(--border-strong); min-width: 0; }
  .condition-connector { margin: 2px 0; color: var(--accent); font-size: 9px; line-height: 1.3; font-weight: 600; letter-spacing: .04em; }
  .condition-row { display: grid; grid-template-columns: minmax(0, 1fr) 72px 77px; align-items: center; gap: 4px; min-width: 0; }
  .condition-row select, .condition-row input { min-width: 0; width: 100%; height: 27px; padding: 0 6px; font-size: 11px; }
  .condition-row .condition-value { grid-column: 1 / 3; }
  .condition-not { display: flex; flex-direction: row; align-items: center; justify-content: flex-end; gap: 3px; padding-right: 4px; font-size: 10px; color: var(--muted); }
  .condition-not input { width: 12px; height: 12px; }
  .condition-actions { display: flex; gap: 1px; justify-content: flex-end; }
  .branch-icon, .group-add { display: inline-flex; align-items: center; justify-content: center; gap: 3px; height: 25px; padding: 0; border: 1px solid transparent; border-radius: 3px; color: var(--muted); background: transparent; font-size: 13px; }
  .branch-icon { flex: 0 0 24px; width: 24px; }
  .group-add { padding: 0 4px; font-size: 10px; }
  .branch-icon:hover:not(:disabled), .group-add:hover { border-color: var(--border); color: var(--fg); background: var(--surface-weak); }
  .branch-icon:focus-visible, .group-add:focus-visible { outline: 1px solid var(--accent-border); outline-offset: 1px; }
  .branch-icon:disabled { opacity: .35; }
  .empty-group { margin: 3px 0; font-size: 10px; color: var(--muted); }
</style>
