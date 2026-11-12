<script lang="ts">
  import type { ValueCount } from "../../types";
  import { useWorkspace } from "../../workspaceContext";
  const { filterByValue } = useWorkspace();

  export let column: string;
  export let items: ValueCount[] = [];

  $: maximum = Math.max(1, ...items.map(item => item.count));
</script>

{#if items.length === 0}
  <p class="empty-values">No values in this result.</p>
{:else}
  <ul class="value-count-list">
    {#each items as item (item.value)}
      <li>
        <button
          type="button"
          class="value-count-row"
          title={`Filter by ${item.value} (${item.count.toLocaleString()} rows)`}
          on:click={() => filterByValue(column, item.value)}
        >
          <span class="count-bar" style={`width: ${(item.count / maximum) * 100}%`} aria-hidden="true"></span>
          <span class="value-label">{item.value || "(empty string)"}</span>
          <span class="value-count">{item.count.toLocaleString()}</span>
        </button>
      </li>
    {/each}
  </ul>
{/if}

<style>
  .value-count-list { list-style: none; margin: 0; padding: 0; display: grid; gap: 3px; }
  .value-count-row { position: relative; display: flex; align-items: center; gap: 10px; width: 100%; min-height: 27px; padding: 4px 7px; border: 1px solid transparent; border-radius: 3px; background: transparent; color: var(--fg); font-size: 11px; text-align: left; overflow: hidden; }
  .value-count-row:hover, .value-count-row:focus-visible { border-color: var(--accent-border); outline: none; }
  .count-bar { position: absolute; inset: 0 auto 0 0; background: var(--accent-soft); pointer-events: none; }
  .value-label { position: relative; flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .value-count { position: relative; flex: 0 0 auto; font-variant-numeric: tabular-nums; color: var(--muted); }
  .empty-values { margin: 4px 0; color: var(--muted); font-size: 11px; }
</style>
