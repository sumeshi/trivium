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

<div class="condition-branch" style={`padding-left: ${depth === 0 ? 0 : 12}px`}>
  {#if node.type === "group"}
    <div class="condition-group">
      <div class="condition-toolbar">
        <button type="button" class="ghost" on:click={() => toggleCollapsed(node.id)}>
          {node.collapsed ? "Expand" : "Collapse"}
        </button>
        <select
          value={node.kind}
          on:change={(event) => setKind(node.id, event.currentTarget.value)}
          aria-label="Combine conditions"
        >
          <option value="and">AND</option>
          <option value="or">OR</option>
        </select>
        <button type="button" class="ghost" on:click={() => addCondition(node.id, columns[0] ?? "")}>
          Add
        </button>
        <button type="button" class="ghost" on:click={() => addGroup(node.id)}>Add group</button>
        {#if !isRoot}
          <button type="button" class="ghost" disabled={index === 0} on:click={() => moveNode(node.id, -1)}>
            Up
          </button>
          <button
            type="button"
            class="ghost"
            disabled={index >= siblingCount - 1}
            on:click={() => moveNode(node.id, 1)}
          >
            Down
          </button>
          <button type="button" class="ghost" on:click={() => removeNode(node.id)}>Remove</button>
        {/if}
      </div>
      {#if !node.collapsed}
        {#each node.children as child, childIndex (child.id)}
          <svelte:self
            node={child}
            {columns}
            depth={depth + 1}
            index={childIndex}
            siblingCount={node.children.length}
          />
        {/each}
      {/if}
    </div>
  {:else}
    <div class="condition-row">
      <select
        value={node.column}
        on:change={(event) => patchCondition(node.id, { column: event.currentTarget.value })}
        aria-label="Column"
      >
        {#each columns as column}
          <option value={column}>{$columnLabels.get(column) ?? column}</option>
        {/each}
      </select>
      <select
        value={node.op}
        on:change={(event) => setOp(node.id, event.currentTarget.value)}
        aria-label="Operator"
      >
        {#each COMPARE_OPS as op}
          <option value={op.value}>{op.label}</option>
        {/each}
      </select>
      <input
        type={node.exact ? "text" : valueInput(node.column).type}
        inputmode={node.exact ? undefined : valueInput(node.column).inputmode}
        step={node.exact ? undefined : valueInput(node.column).step}
        value={node.value}
        placeholder={node.op === "in" ? "a, b, c" : "Value"}
        on:input={(event) => patchCondition(node.id, { value: event.currentTarget.value, exact: false })}
        aria-label="Value"
      />
      <label class="condition-not">
        <input
          type="checkbox"
          checked={node.negated}
          on:change={(event) => patchCondition(node.id, { negated: event.currentTarget.checked })}
        />
        Not
      </label>
      <button type="button" class="ghost" disabled={index === 0} on:click={() => moveNode(node.id, -1)}>
        Up
      </button>
      <button
        type="button"
        class="ghost"
        disabled={index >= siblingCount - 1}
        on:click={() => moveNode(node.id, 1)}
      >
        Down
      </button>
      <button type="button" class="ghost" on:click={() => removeNode(node.id)}>Remove</button>
    </div>
  {/if}
</div>
