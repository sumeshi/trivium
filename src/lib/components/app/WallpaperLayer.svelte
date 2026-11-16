<script lang="ts">
  import { onDestroy } from 'svelte';
  import { wallpaper } from '../../appearance';
  import { wallpaperVisible } from '../../appearanceModel';
  // A slider update must never rebuild/reparse the multi-megabyte image URL.
  $: dataUrl = $wallpaper.image?.dataUrl ?? '';
  $: backgroundImage = dataUrl ? `url("${dataUrl}")` : null;
  $: visible = wallpaperVisible($wallpaper.settings, $wallpaper.image);
  $: if (typeof document !== 'undefined') {
    document.documentElement.toggleAttribute('data-wallpaper-table', visible && $wallpaper.settings.behindTable);
  }
  onDestroy(() => {
    if (typeof document !== 'undefined') document.documentElement.removeAttribute('data-wallpaper-table');
  });
</script>

{#if visible && $wallpaper.image}
  <div class="wallpaper-layer" aria-hidden="true"
    style:background-image={backgroundImage}
    style:background-size={$wallpaper.settings.fit}
    style:opacity={$wallpaper.settings.strength / 100}></div>
{/if}

<style>
  .wallpaper-layer {
    position: fixed; inset: 0; z-index: -1; pointer-events: none;
    background-position: center; background-repeat: no-repeat; contain: paint;
  }
</style>
