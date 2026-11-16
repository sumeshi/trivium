<script lang="ts">
  import { wallpaper } from '../../appearance';
  import { wallpaperVisible } from '../../appearanceModel';
  export let pinnedWidth = 0;
  // These primitives keep strength/scroll updates from assigning the image URL.
  $: dataUrl = $wallpaper.image?.dataUrl ?? '';
  $: backgroundImage = dataUrl ? `url("${dataUrl}")` : null;
  $: visible = wallpaperVisible($wallpaper.settings, $wallpaper.image);
  $: strength = $wallpaper.settings.strength / 100;
  $: edge = Number.isFinite(pinnedWidth) ? Math.max(0, pinnedWidth) : 0;
</script>

<div class="table-body-surface" class:has-wallpaper={visible}
  style:--wallpaper-image={backgroundImage}
  style:--wallpaper-strength={strength}
  style:--wallpaper-edge={`${edge}px`}>
  {#if visible}
    <div class="wallpaper-layer wallpaper-base" aria-hidden="true"></div>
    {#if edge > 0}
      <div class="wallpaper-layer wallpaper-pinned-mask" aria-hidden="true"></div>
    {/if}
  {/if}
  <slot />
</div>

<style>
  .table-body-surface {
    position: relative; isolation: isolate; overflow: clip;
    display: flex; flex: 1 1 auto; min-height: 0; width: 100%;
    background: var(--surface);
  }
  .wallpaper-layer {
    position: absolute; inset: 0; pointer-events: none;
    background: var(--surface); contain: paint;
  }
  .wallpaper-layer::before {
    content: ''; position: absolute; inset: 0;
    background-image: var(--wallpaper-image);
    background-size: cover; background-position: center; background-repeat: no-repeat;
    opacity: var(--wallpaper-strength);
  }
  .wallpaper-base { z-index: -1; }
  .wallpaper-pinned-mask {
    /* Both planes cover the full body, so clipping never changes image scale.
       The opaque theme backing hides scrolling text, even at zero image alpha. */
    z-index: 1;
    clip-path: inset(0 max(0px, calc(100% - var(--wallpaper-edge))) 0 0);
  }
</style>
