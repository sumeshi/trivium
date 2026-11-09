<script lang="ts">
  import { onDestroy } from "svelte";
  import type { ColorRule, CompareOp, ConditionNode } from "../../types";
  import { COMPARE_OPS } from "../../stores/condition";
  import { backend, projectDetail } from "./state";

  let rules: ColorRule[] = [];
  let column = "";
  let op: CompareOp = "contains";
  let value = "";
  let color = "#ff0000";
  let message = "";
  let error = "";
  let saving = false;
  let loadedProjectId = "";
  let alive = true;

  onDestroy(() => {
    alive = false;
  });

  $: columns = $projectDetail?.columns ?? [];
  $: projectId = $projectDetail?.project.meta.id ?? "";
  $: if (projectId && !columns.includes(column)) {
    column = columns[0] ?? "";
  }
  $: if (projectId && projectId !== loadedProjectId) {
    loadedProjectId = projectId;
    rules = [...($projectDetail?.color_rules ?? [])];
    message = "";
    error = "";
  }

  const describe = (node: ConditionNode): string => {
    if ("kind" in node) {
      return `${node.kind.toUpperCase()}(${node.children.map(describe).join(", ")})`;
    }
    const label = COMPARE_OPS.find((item) => item.value === node.op)?.label ?? node.op;
    return `${node.column} ${label} ${node.values.join(", ")}`;
  };

  const addRule = () => {
    const trimmed = value.trim();
    if (!column || !trimmed) {
      error = "Column and value are required.";
      return;
    }
    const condition: ConditionNode = {
      column,
      op,
      values: [trimmed],
      negated: false,
    };
    rules = [
      ...rules,
      {
        name: `${column} ${op} ${trimmed}`,
        condition,
        color,
      },
    ];
    value = "";
    message = "";
    error = "";
  };

  const removeRule = (index: number) => {
    rules = rules.filter((_, position) => position !== index);
    message = "";
  };

  const save = async () => {
    if (!$backend || !projectId) {
      return;
    }
    saving = true;
    message = "";
    error = "";
    try {
      const response = await $backend.saveColorRules({ projectId, rules });
      if (!alive) {
        return;
      }
      rules = response.rules;
      message = "Rules saved.";
    } catch (err) {
      if (!alive) {
        return;
      }
      error = err instanceof Error ? err.message : String(err);
    } finally {
      if (alive) {
        saving = false;
      }
    }
  };
</script>

<div class="stats-panel">
  <div class="condition-toolbar">
    <span class="filter-label">Row colors</span>
    <button type="button" class="primary" on:click={save} disabled={saving || !projectId}>
      {saving ? "Saving…" : "Save"}
    </button>
    {#if message}
      <span class="condition-count">{message}</span>
    {/if}
  </div>
  {#if error}
    <p class="condition-count">{error}</p>
  {/if}
  <div class="condition-toolbar">
    <select bind:value={column} aria-label="Color rule column">
      {#each columns as name}
        <option value={name}>{name}</option>
      {/each}
    </select>
    <select bind:value={op} aria-label="Color rule operator">
      {#each COMPARE_OPS as option}
        <option value={option.value}>{option.label}</option>
      {/each}
    </select>
    <input
      type="text"
      bind:value={value}
      placeholder="Value"
      aria-label="Color rule value"
    />
    <input type="color" bind:value={color} aria-label="Row color" />
    <button
      type="button"
      class="ghost"
      on:click={addRule}
      disabled={!column || value.trim().length === 0}
    >
      Add
    </button>
  </div>
  {#if rules.length === 0}
    <p class="condition-count">No color rules.</p>
  {:else}
    {#each rules as rule, index (index)}
      <div class="condition-toolbar">
        <span class="color-swatch" style={`background-color: ${rule.color};`}></span>
        <span class="condition-count">{rule.name} — {describe(rule.condition)}</span>
        <button
          type="button"
          class="ghost"
          on:click={() => removeRule(index)}
          aria-label={`Delete ${rule.name}`}
        >
          ×
        </button>
      </div>
    {/each}
  {/if}
</div>

<style>
  .color-swatch {
    display: inline-block;
    width: 1rem;
    height: 1rem;
    border-radius: 4px;
    border: 1px solid var(--border);
  }

  input[type="color"] {
    width: 2.75rem;
    height: 2.25rem;
    padding: 2px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface);
  }
</style>
