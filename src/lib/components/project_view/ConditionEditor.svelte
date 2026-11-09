<script lang="ts">
  import type { CompareOp } from "../../types";
  import { totalFilteredRows, projectDetail } from "./state";
  import {
    COMPARE_OPS,
    addCondition,
    conditionKind,
    conditionRows,
    patchCondition,
    removeCondition,
  } from "../../stores/condition";

  $: columns = $projectDetail?.columns ?? [];

  const addRow = () => {
    addCondition(columns[0] ?? "");
  };

  const setOp = (id: string, value: string) => {
    patchCondition(id, { op: value as CompareOp });
  };
</script>

<div class="condition-editor">
  <div class="condition-toolbar">
    <span class="filter-label">Conditions</span>
    {#if $conditionRows.length > 1}
      <select bind:value={$conditionKind} aria-label="Combine conditions">
        <option value="and">AND</option>
        <option value="or">OR</option>
      </select>
    {/if}
    <button type="button" class="ghost" on:click={addRow}>Add</button>
    <span class="condition-count">{$totalFilteredRows} rows</span>
  </div>
  {#each $conditionRows as row (row.id)}
    <div class="condition-row">
      <select
        value={row.column}
        on:change={(event) =>
          patchCondition(row.id, { column: event.currentTarget.value })}
        aria-label="Column"
      >
        {#each columns as column}
          <option value={column}>{column}</option>
        {/each}
      </select>
      <select
        value={row.op}
        on:change={(event) => setOp(row.id, event.currentTarget.value)}
        aria-label="Operator"
      >
        {#each COMPARE_OPS as op}
          <option value={op.value}>{op.label}</option>
        {/each}
      </select>
      <input
        type="text"
        value={row.value}
        placeholder={row.op === "in" ? "a, b, c" : "Value"}
        on:input={(event) =>
          patchCondition(row.id, { value: event.currentTarget.value })}
        aria-label="Value"
      />
      <label class="condition-not">
        <input
          type="checkbox"
          checked={row.negated}
          on:change={(event) =>
            patchCondition(row.id, { negated: event.currentTarget.checked })}
        />
        Not
      </label>
      <button type="button" class="ghost" on:click={() => removeCondition(row.id)}>
        Remove
      </button>
    </div>
  {/each}
</div>
