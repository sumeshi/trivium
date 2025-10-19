<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { memoEditor, backend, projectDetail, sanitizeMemoInput } from './state';
  import type { CachedRow } from './state';

  const dispatch = createEventDispatcher();

  let memoDraft = '';
  let memoSaving = false;
  let memoError: string | null = null;

  const closeMemoEditor = () => {
    memoEditor.set(null);
    memoDraft = '';
    memoError = null;
    memoSaving = false;
  };

  const saveMemo = async () => {
    if (!$memoEditor || memoSaving || !$projectDetail) return;
    const sanitized = sanitizeMemoInput(memoDraft).trim();
    memoSaving = true;
    memoError = null;
    try {
      await $backend.updateFlag({
        projectId: $projectDetail.project.meta.id,
        rowIndex: $memoEditor.row.row_index,
        flag: $memoEditor.row.flag,
        memo: sanitized.length ? sanitized : null
      });
      dispatch('notify', { message: 'Memo updated.', tone: 'success' });
      closeMemoEditor();
      dispatch('refresh');
    } catch (error) {
      console.error(error);
      memoError = 'Failed to update memo.';
      dispatch('notify', { message: 'Failed to update memo.', tone: 'error' });
    } finally {
      memoSaving = false;
    }
  };

  const handleMemoBackdropClick = (event: MouseEvent) => {
    if (event.target === event.currentTarget && !memoSaving) {
      closeMemoEditor();
    }
  };

  const handleMemoBackdropKey = (event: KeyboardEvent) => {
    if (!memoSaving && (event.key === 'Escape' || event.key === 'Enter')) {
      event.preventDefault();
      closeMemoEditor();
    }
  };

  memoEditor.subscribe(editor => {
    if (editor) {
      memoDraft = editor.row.memo ?? '';
      memoError = null;
      memoSaving = false;
    }
  });
</script>

{#if $memoEditor}
  <!-- svelte-ignore a11y-click-events-have-key-events -->
  <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
  <div
    class="cell-dialog-backdrop"
    role="dialog"
    aria-modal="true"
    aria-label={`Edit memo for row ${$memoEditor.row.row_index + 1}`}
    on:click={handleMemoBackdropClick}
    tabindex="-1"
    on:keydown={handleMemoBackdropKey}
  >
    <div class="cell-dialog memo-dialog">
      <div class="cell-dialog-header">
        <h3>Edit memo</h3>
        <div class="cell-dialog-actions">
          <button type="button" class="ghost" on:click={closeMemoEditor} disabled={memoSaving}>
            Cancel
          </button>
          <button
            type="button"
            class="primary"
            on:click={saveMemo}
            disabled={memoSaving}
          >
            {memoSaving ? 'Saving…' : 'Save'}
          </button>
        </div>
      </div>
      <label class="memo-editor-label">
        <span>Memo</span>
        <textarea
          bind:value={memoDraft}
          rows="8"
          placeholder="Add memo"
          spellcheck="true"
          disabled={memoSaving}
        />
      </label>
      {#if memoError}
        <p class="memo-error">{memoError}</p>
      {/if}
    </div>
  </div>
{/if}
