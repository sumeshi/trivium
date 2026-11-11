<script lang="ts">
  import { errorMessage } from "../../errors";
  import { onDestroy } from "svelte";
  import { get } from "svelte/store";
  import type { ColorRule, CompareOp, ConditionNode } from "../../types";
  import { COMPARE_OPS } from "../../stores/condition";
  import { theme } from "../../theme";
  import {
    CONDITION_COLORS,
    displayRowColor,
    resolveConditionColor,
  } from "../../skin";
  import { backend, columnLabels, projectDetail } from "./state";

  let rules: ColorRule[] = [];
  let column = "";
  let op: CompareOp = "contains";
  let value = "";
  let color =
    resolveConditionColor(CONDITION_COLORS[0].name, get(theme)) ?? "#ff0000";
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
    const column = $columnLabels.get(node.column) ?? node.column;
    return `${column} ${label} ${node.values.join(", ")}`;
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
        name: `${$columnLabels.get(column) ?? column} ${op} ${trimmed}`,
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
      error = errorMessage(err);
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
  <div class="color-palette" role="group" aria-label="Color palette">
    {#each CONDITION_COLORS as entry}
      <button
        type="button"
        class="palette-swatch"
        class:selected={`var(--rule-${entry.name})` ===
          displayRowColor(color, $theme)}
        style={`background-color: var(--rule-${entry.name});`}
        title={entry.name}
        aria-label={`Use ${entry.name}`}
        on:click={() => (color = resolveConditionColor(entry.name, $theme) ?? color)}
      ></button>
    {/each}
  </div>
  <div class="condition-toolbar">
    <select bind:value={column} aria-label="Color rule column">
      {#each columns as name}
        <option value={name}>{$columnLabels.get(name) ?? name}</option>
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
        <span
          class="color-swatch"
          style={`background-color: ${displayRowColor(rule.color, $theme)};`}
        ></span>
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
  .color-palette {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-bottom: 8px;
  }

  .palette-swatch {
    width: 1.5rem;
    height: 1.5rem;
    padding: 0;
    border: 1px solid var(--border);
    border-radius: 6px;
    cursor: pointer;
  }

  .palette-swatch.selected {
    outline: 2px solid var(--accent);
    outline-offset: 1px;
  }

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
