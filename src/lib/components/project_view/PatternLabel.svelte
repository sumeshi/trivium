<script lang="ts">
  import type { FormatCatalogEntry, FormatDefinition } from '../../formats';
  export let entry: FormatCatalogEntry;
  export let showExample = false;
  export let descriptionId: string | undefined = undefined;

  function firstExample(definition: FormatDefinition): string {
    for (const example of definition.examples ?? []) {
      const value = example.values.find(value => value.trim().length > 0);
      if (value !== undefined) return value;
    }
    return '';
  }
  $: rawExample = showExample ? firstExample(entry.definition) : '';
  $: example = rawExample.replace(/\s+/gu, ' ').trim();
</script>

<span class="pattern-label" id={descriptionId}>
  {#if descriptionId && entry.builtin}<span class="visually-hidden">Built-in pattern</span>{/if}
  <span class="pattern-title">
    <strong title={entry.definition.name}>{entry.definition.name}</strong>
    {#if !entry.builtin}<span class="pattern-custom" title="User-defined pattern">Custom</span>{/if}
  </span>
  {#if example}<span class="pattern-example" title={rawExample}>e.g. {example}</span>{/if}
</span>

<style>
  .pattern-label { display: flex; flex: 1; flex-direction: column; gap: 2px; width: 100%; min-width: 0; }
  .pattern-title { display: flex; align-items: center; gap: 5px; min-width: 0; }
  strong { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--fg); font-size: 11px; font-weight: 500; }
  .pattern-custom { flex: 0 0 auto; padding: 0 3px; border: 1px solid var(--border); border-radius: 3px; color: var(--muted); background: var(--surface-weak); font: 400 9px/14px var(--font-ui); }
  .pattern-example { display: block; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--muted); font: 400 10px/14px var(--font-mono); }
  .visually-hidden { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0,0,0,0); white-space: nowrap; }
</style>
