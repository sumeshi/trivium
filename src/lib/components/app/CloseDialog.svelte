<script lang="ts">
  import { createEventDispatcher, onMount } from 'svelte';
  import type { AppCloseState } from '../../appClose';

  export let phase: AppCloseState['phase'];
  export let message: string;
  export let startupError = false;
  const dispatch = createEventDispatcher<{ keepOpen: void; retry: void }>();
  let dialog: HTMLElement;

  onMount(() => (dialog.querySelector<HTMLButtonElement>('button') ?? dialog).focus());
  const keyboard = (event: KeyboardEvent) => {
    event.stopPropagation();
    if (event.key !== 'Tab') return;
    const items = [...dialog.querySelectorAll<HTMLButtonElement>('button:not(:disabled)')];
    const first = items[0], last = items.at(-1);
    if (event.shiftKey && document.activeElement === first && last) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && document.activeElement === last && first) { event.preventDefault(); first.focus(); }
  };
</script>

<div class="close-backdrop">
  <!-- The modal owns its keyboard focus loop. -->
  <div class="close-dialog" bind:this={dialog} role="alertdialog" aria-modal="true"
    aria-labelledby="app-close-title" aria-describedby="app-close-message" tabindex="-1" on:keydown={keyboard}>
    <h3 id="app-close-title">{startupError ? 'Could not prepare the app' : phase === 'failed' || phase === 'recovering' ? 'Could not close safely' : 'Closing Trivium'}</h3>
    <p id="app-close-message">{message}</p>
    {#if phase === 'failed' && !startupError}
      <p>Keep the app open to check your changes before trying again.</p>
    {/if}
    <div class="close-actions dialog-actions">
      {#if !startupError && phase === 'failed'}
        <button type="button" class="ghost" on:click={() => dispatch('keepOpen')}>Keep open</button>
      {/if}
      {#if phase === 'failed' || startupError}
        <button type="button" class="primary" on:click={() => dispatch('retry')}>{startupError ? 'Retry' : 'Try closing again'}</button>
      {/if}
    </div>
  </div>
</div>

<style>
  .close-backdrop { position: fixed; inset: 0; z-index: 260; display: grid; place-items: center; background: color-mix(in srgb, var(--bg) 65%, transparent); }
  .close-dialog { width: min(440px, calc(100vw - 32px)); padding: 18px; border: 1px solid var(--border-strong); border-radius: 7px; background: var(--surface); color: var(--fg); box-shadow: var(--shadow-strong); }
  h3 { margin: 0 0 10px; font-size: 14px; }
  p { margin: 8px 0; font-size: 12px; line-height: 1.5; }
  .close-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 14px; }
</style>
