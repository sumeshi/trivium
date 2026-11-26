<script lang="ts">
  import { createEventDispatcher, onDestroy } from 'svelte';
  import type { Backend } from '../../backend';
  import type { ImportOptions } from '../../types';
  import { createImportPreview } from '../../importPreview';

  export let backend: Backend;
  export let path: string | null = null;
  export let delimiter = '';
  export let options: ImportOptions;
  export let disabled = false;
  export let multiple = false;
  const dispatch = createEventDispatcher<{ delimiter: string }>();
  const preview = createImportPreview(backend);
  const { state } = preview;
  let expanded = false;
  $: preview.configure(expanded && !disabled && path ? { path, delimiter, importOptions: options } : null);
  $: fileName = path?.split(/[/\\]/).at(-1) ?? '';
  $: suggestion = $state.result?.suggestedDelimiter;
  const delimiterNames: Record<string, string> = { ',': 'comma', '\t': 'tab', ';': 'semicolon', '|': 'pipe' };
  const delimiterName = (value: string) => delimiterNames[value] ?? value;
  const applySuggestion = () => { if (suggestion !== null && suggestion !== undefined) dispatch('delimiter', suggestion); };
  onDestroy(() => preview.dispose());
</script>

<details class="import-preview" bind:open={expanded}>
  <summary aria-label="Preview import" aria-disabled={disabled || !path}
    on:click={(event) => { if (disabled || !path) event.preventDefault(); }}>Preview</summary>
  {#if expanded && path}
    <section aria-label="Import preview" aria-busy={$state.loading}>
      <div class="preview-heading">
        <span title={path}>{multiple ? `First file: ${fileName}` : fileName}</span>
        {#if $state.loading}<span role="status">Loading…</span>{/if}
      </div>
      {#if $state.error}<p class="preview-error" role="status">{$state.error}</p>{/if}
      {#if $state.result}
        {#if suggestion !== null && suggestion !== undefined && suggestion !== (delimiter || ',')}
          <button type="button" class="delimiter-suggestion" disabled={disabled}
            on:click={applySuggestion}>
            Use {delimiterName(suggestion)} delimiter
          </button>
        {/if}
        <!-- Keyboard focus allows the bounded preview to scroll without a mouse. -->
        <!-- svelte-ignore a11y-no-noninteractive-tabindex -->
        <div class="preview-table" tabindex="0" role="region" aria-label="Preview rows">
          <table>
            <thead><tr>{#each $state.result.columns as column}<th>{column}</th>{/each}</tr></thead>
            <tbody>
              {#each $state.result.rows as row}
                <tr>{#each row as value}<td class:missing={value === null}>{value ?? 'null'}</td>{/each}</tr>
              {/each}
            </tbody>
          </table>
        </div>
        <div class="preview-caption">
          <span>{$state.result.encoding}{$state.result.bom ? ' · BOM' : ''}</span>
          {#if $state.result.truncated}<span>Limited sample</span>{/if}
        </div>
        {#each $state.result.warnings as warning}<p class="preview-warning">{warning}</p>{/each}
      {/if}
    </section>
  {/if}
</details>

<style>
  .import-preview { margin: 10px 0; font-size: 11px; }
  summary { cursor: pointer; padding: 4px 0; }
  summary[aria-disabled="true"] { color: var(--muted); cursor: default; }
  section { margin-top: 7px; min-width: 0; }
  .preview-heading, .preview-caption { display: flex; align-items: center; justify-content: space-between; gap: 8px; color: var(--muted); font-size: 10px; }
  .preview-heading { margin-bottom: 5px; }
  .preview-heading > span:first-child { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .preview-table { max-height: 180px; overflow: auto; border: 1px solid var(--border); border-radius: 4px; }
  table { border-collapse: collapse; width: max-content; min-width: 100%; text-align: left; }
  th, td { padding: 4px 6px; max-width: 240px; white-space: pre-wrap; overflow-wrap: anywhere; border-bottom: 1px solid var(--border); vertical-align: top; }
  th { position: sticky; top: 0; z-index: 1; background: var(--surface); color: var(--muted); font-size: 10px; font-weight: 500; }
  tr:last-child td { border-bottom: 0; }
  .missing, .preview-caption { color: var(--muted); }
  .preview-caption { margin-top: 4px; }
  .preview-error, .preview-warning { margin: 5px 0; font-size: 10px; line-height: 1.4; overflow-wrap: anywhere; }
  .preview-error { color: var(--critical); }
  .preview-warning { color: var(--muted); }
  .delimiter-suggestion { margin-bottom: 5px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); padding: 4px 7px; font-size: 10px; }
</style>
