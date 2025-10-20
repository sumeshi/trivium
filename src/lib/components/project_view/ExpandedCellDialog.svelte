<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { expandedCell } from './state';

  const dispatch = createEventDispatcher();

  const closeCell = () => {
    expandedCell.set(null);
  };

  const handleBackdropKey = (event: KeyboardEvent) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      closeCell();
    }
  };

  const handleBackdropClick = (event: MouseEvent) => {
    if (event.target === event.currentTarget) {
      closeCell();
    }
  };

  const copyExpandedCell = async () => {
    if (!$expandedCell) return;
    const text = $expandedCell.value ?? '';
    try {
      if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
        await navigator.clipboard.writeText(text);
      } else {
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
      }
      dispatch('notify', { message: 'Copied cell value.', tone: 'success' });
    } catch (error) {
      console.error(error);
      dispatch('notify', { message: 'Failed to copy cell value.', tone: 'error' });
    }
  };
</script>

{#if $expandedCell}
  <div
    class="cell-dialog-backdrop"
    role="button"
    tabindex="0"
    on:click={handleBackdropClick}
    on:keydown={handleBackdropKey}
  >
    <div
      class="cell-dialog"
      role="dialog"
      aria-modal="true"
      aria-label={`Cell value for ${$expandedCell.column}`}
    >
      <div class="cell-dialog-header">
        <h3>{$expandedCell.column}</h3>
        <div class="cell-dialog-actions">
          <button type="button" class="ghost" on:click={copyExpandedCell}>Copy</button>
          <button type="button" class="ghost close-dialog" on:click={closeCell}>Close</button>
        </div>
      </div>
      <pre class="cell-dialog-body">{$expandedCell.value || '—'}</pre>
    </div>
  </div>
{/if}
