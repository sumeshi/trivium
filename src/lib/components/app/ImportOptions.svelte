<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ImportOptions, TextEncoding } from '../../types';

  export let options: ImportOptions;
  export let csv = false;
  export let disabled = false;
  const dispatch = createEventDispatcher<{ change: ImportOptions }>();
  const update = (change: Partial<ImportOptions>) => dispatch('change', { ...options, ...change });
  const changeEncoding = (event: Event) => update({ encoding: (event.currentTarget as HTMLSelectElement).value as TextEncoding });
</script>

<details class="import-options">
  <summary>Import options <span>{options.recover ? 'Recovery' : 'Strict'}</span></summary>
  <div class="options-fields">
    <label class="encoding-field">
      <span>Encoding</span>
      <select aria-label="Import encoding" value={options.encoding} {disabled}
        on:change={changeEncoding}>
        <option value="auto">Auto (Unicode BOM / UTF-8)</option>
        <option value="utf-8">UTF-8</option>
        <option value="utf-16le">UTF-16 LE</option>
        <option value="utf-16be">UTF-16 BE</option>
        <option value="utf-32le">UTF-32 LE</option>
        <option value="utf-32be">UTF-32 BE</option>
        <option value="windows-1252">Windows-1252</option>
        <option value="shift-jis">Shift JIS</option>
      </select>
    </label>
    {#if csv}
      <label class="check-option" title="Turn off for CSV files without a header row. Columns receive generated names.">
        <input type="checkbox" aria-label="First row contains column names" checked={options.hasHeaders} {disabled}
          on:change={(event) => update({ hasHeaders: event.currentTarget.checked })} />
        <span>First row contains column names</span>
      </label>
    {/if}
    <label class="check-option">
      <input type="checkbox" aria-label="Recover readable records" checked={options.recover} {disabled}
        on:change={(event) => update({ recover: event.currentTarget.checked })} />
      <span>Recover readable records</span>
    </label>
    <p>Strict stops on errors. Recovery records repairs and skipped records in the import report.</p>
  </div>
</details>

<style>
  .import-options { margin: 10px 0; font-size: 11px; }
  summary { cursor: pointer; padding: 4px 0; }
  summary span { color: var(--muted); margin-left: 6px; font-size: 10px; }
  .options-fields { display: grid; gap: 8px; padding-top: 8px; }
  .encoding-field { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; gap: 10px; }
  select { width: 100%; min-width: 0; padding: 5px 7px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); font-size: 11px; }
  .check-option { display: flex; align-items: center; gap: 7px; }
  p { margin: 0; color: var(--muted); font-size: 10px; line-height: 1.4; }
</style>
