<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { isTabKey } from '../../keyboard';

  export let name: string;
  const dispatch = createEventDispatcher<{ cancel: void; confirm: void }>();
  let dialog: HTMLElement;
  let cancelButton: HTMLButtonElement;
  let previousFocus: HTMLElement | null = null;

  onMount(() => {
    previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    cancelButton.focus();
  });
  onDestroy(() => { if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true }); });

  const keyboard = (event: KeyboardEvent) => {
    event.stopPropagation();
    if (event.key === 'Escape') {
      event.preventDefault();
      dispatch('cancel');
    } else if (isTabKey(event)) {
      const buttons = [...dialog.querySelectorAll<HTMLButtonElement>('button')];
      const first = buttons[0], last = buttons.at(-1);
      if (event.shiftKey && document.activeElement === first && last) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last && first) { event.preventDefault(); first.focus(); }
    }
  };
</script>

<div class="delete-project-backdrop">
  <!-- The modal owns its keyboard focus loop. -->
  <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
  <section class="delete-project-dialog" bind:this={dialog} role="alertdialog" aria-modal="true"
    aria-labelledby="delete-project-title" aria-describedby="delete-project-name delete-project-message" tabindex="-1" on:keydown={keyboard}>
    <h3 id="delete-project-title">Delete project?</h3>
    <p id="delete-project-name" class="delete-project-name" title={name}>{name}</p>
    <p id="delete-project-message">Saved data and annotations will be deleted. The source file stays unchanged.</p>
    <div class="delete-project-actions dialog-actions">
      <button type="button" class="cancel" bind:this={cancelButton} on:click={() => dispatch('cancel')}>Cancel</button>
      <button type="button" class="delete" on:click={() => dispatch('confirm')}>Delete</button>
    </div>
  </section>
</div>

<style>
  .delete-project-backdrop { position: fixed; inset: 0; z-index: 220; display: grid; place-items: center; background: color-mix(in srgb, var(--bg) 65%, transparent); }
  .delete-project-dialog { width: min(390px, calc(100vw - 32px)); padding: 16px; border: 1px solid var(--border-strong); border-radius: 6px; background: var(--surface); color: var(--fg); }
  h3 { margin: 0 0 10px; font-size: 14px; }
  p { margin: 0 0 10px; font-size: 11px; line-height: 1.45; }
  .delete-project-name { overflow-wrap: anywhere; font-weight: 600; }
  #delete-project-message { color: var(--muted); }
  .delete-project-actions { display: flex; justify-content: flex-end; gap: 6px; margin-top: 12px; }
  button { border: 1px solid var(--border); }
  .cancel { background: var(--surface); color: var(--fg); }
  .delete { background: var(--danger-soft); border-color: var(--danger); color: var(--danger); font-weight: 600; }
  button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 2px; }
</style>
