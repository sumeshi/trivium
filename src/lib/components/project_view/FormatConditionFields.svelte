<script lang="ts">
  import type { EditorFormatCondition } from '../../stores/condition';
  import { useWorkspace } from '../../workspaceContext';
  import { formats } from '../../patternCatalog';
  import { formatDefinitionKey } from '../../formats';
  export let node: EditorFormatCondition;
  export let columns: string[] = [];
  const { patchFormatCondition, columnLabels } = useWorkspace();
  let find = '';
  $: options = columns.filter(column => ($columnLabels.get(column) ?? column).toLocaleLowerCase().includes(find.trim().toLocaleLowerCase()));
  $: snapshot = formatDefinitionKey(node.format);
  $: selected = $formats.find(entry => formatDefinitionKey(entry.definition) === snapshot)?.definition.id ?? '__saved';
  const choose = (id: string) => {
    const entry = $formats.find(entry => entry.definition.id === id);
    if (entry) patchFormatCondition(node.id, { format: entry.definition, equals: undefined });
  };
  const toggle = (column: string) => patchFormatCondition(node.id, {
    columns: node.columns.includes(column) ? node.columns.filter(id => id !== column) : [...node.columns, column],
  });
</script>

<div class="format-fields">
  <details class="format-columns">
    <summary title={node.columns.map(column => $columnLabels.get(column) ?? column).join(', ')} aria-label="Choose format columns">
      {node.columns.length === columns.length && node.columns.length ? 'All columns' : `${node.columns.length} columns`} <span aria-hidden="true">▾</span>
    </summary>
    <div class="format-column-menu" role="group" aria-label="Format columns">
      <input type="search" aria-label="Find a format column" placeholder="Find column" bind:value={find} />
      <div class="format-column-actions">
        <button type="button" on:click={() => patchFormatCondition(node.id, { columns: [...columns] })}>Select all</button>
        <button type="button" on:click={() => patchFormatCondition(node.id, { columns: [] })}>Deselect all</button>
      </div>
      <div class="format-column-list">
        {#each options as column}
          <label><input type="checkbox" checked={node.columns.includes(column)} on:change={() => toggle(column)} /><span>{$columnLabels.get(column) ?? column}</span></label>
        {/each}
      </div>
    </div>
  </details>
  <select class="format-choice" value={selected} aria-label="Format" title={node.format.pattern} on:change={event => choose(event.currentTarget.value)}>
    {#if selected === '__saved'}<option value="__saved">{node.format.name} (saved)</option>{/if}
    {#each $formats as entry (entry.definition.id)}<option value={entry.definition.id}>{entry.definition.name}</option>{/each}
  </select>
  <label class="format-not"><input type="checkbox" checked={node.negated} on:change={event => patchFormatCondition(node.id, { negated: event.currentTarget.checked })} />Not</label>
  {#if node.equals !== undefined}
    <label class="format-equals">Value<input value={node.equals} aria-label="Exact extracted value" on:input={event => patchFormatCondition(node.id, { equals: event.currentTarget.value })} /></label>
  {/if}
  {#if !node.columns.length}<span class="format-error">Choose at least one column.</span>{/if}
</div>

<style>
  .format-fields { display: grid; grid-template-columns: minmax(95px, 1fr) minmax(85px, 1fr) auto; align-items: start; gap: 5px; min-width: 0; flex: 1; }
  select, summary, input[type="search"], .format-equals input { min-width: 0; width: 100%; min-height: 27px; padding: 4px 6px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); font-size: 11px; }
  summary { display: flex; justify-content: space-between; gap: 4px; cursor: pointer; list-style: none; }
  summary::-webkit-details-marker { display: none; }
  .format-columns { position: relative; min-width: 0; }
  .format-column-menu { position: absolute; z-index: 46; top: calc(100% + 3px); left: 0; width: min(280px, 70vw); padding: 7px; border: 1px solid var(--border-strong); border-radius: 4px; background: var(--surface); box-shadow: var(--shadow-soft); }
  .format-column-actions { display: flex; gap: 5px; margin: 5px 0; }
  .format-column-actions button { padding: 3px 5px; border: 1px solid var(--border); border-radius: 4px; color: var(--fg); background: var(--surface); font-size: 10px; }
  .format-column-list { max-height: 200px; overflow: auto; }
  .format-column-list label { display: flex; align-items: center; gap: 6px; padding: 3px; font-size: 11px; }
  .format-column-list span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .format-not { display: inline-flex; align-items: center; gap: 4px; padding-top: 5px; color: var(--muted); font-size: 10px; }
  .format-equals { grid-column: 1 / -1; display: grid; grid-template-columns: auto 1fr; align-items: center; gap: 5px; color: var(--muted); font-size: 10px; }
  .format-error { grid-column: 1 / -1; color: var(--danger); font-size: 10px; }
  button:focus-visible, select:focus-visible, summary:focus-visible, input:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
</style>
