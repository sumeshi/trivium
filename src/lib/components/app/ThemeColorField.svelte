<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { normalizeThemeHex } from '../../themeModel';
  export let name: string;
  export let label: string;
  export let value: string;
  export let fallback: string;
  const dispatch = createEventDispatcher<{ change: string }>();
  $: color = normalizeThemeHex(value);
</script>

<div class="color-field">
  <label for={`theme-color-${name}`}>{label}</label>
  <input class="color-swatch" type="color" value={color ?? normalizeThemeHex(fallback) ?? '#000000'} aria-label={`Choose ${label} color`}
    on:input={event => dispatch('change', event.currentTarget.value)} />
  <input id={`theme-color-${name}`} class="color-code" class:invalid={!color} value={value} type="text"
    aria-label={`${label} color code`} aria-invalid={!color} maxlength="7" autocomplete="off" spellcheck="false"
    title="Use #RGB or #RRGGBB" on:input={event => dispatch('change', event.currentTarget.value)} />
</div>

<style>
  .color-field { display: grid; grid-template-columns: minmax(0, 1fr) 28px 86px; align-items: center; gap: 7px; min-height: 28px; }
  label { color: var(--fg); font-size: 11px; }
  input { min-width: 0; height: 25px; border: 1px solid var(--border); border-radius: 4px; color: var(--fg); background: var(--surface); }
  .color-swatch { width: 28px; padding: 2px; cursor: pointer; }
  .color-code { width: 86px; padding: 3px 6px; font: 11px var(--font-mono); }
  .color-code.invalid { border-color: var(--danger); }
  input:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
</style>
