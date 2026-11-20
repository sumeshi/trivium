<script lang="ts">
  import { createEventDispatcher, onDestroy, tick } from 'svelte';
  import { get } from 'svelte/store';
  import { createCompositionGuard, isTabKey } from '../../keyboard';
  import { buildMemoPatch } from '../../memoAnnotation';
  import { useWorkspace } from "../../workspaceContext";
  const {
    memoEditor, backend, saveAnnotation, currentProjectId,
  } = useWorkspace();

  const dispatch = createEventDispatcher();

  let memoDraft = '';
  let memoSaving = false;
  let memoError: string | null = null;
  let textareaEl: HTMLTextAreaElement | null = null;
  let backdropEl: HTMLDivElement | null = null;
  let tagDraft = '';
  let tagList: string[] = [];
  let bookmarkDraft = false;
  const memoComposition = createCompositionGuard();
  const tagComposition = createCompositionGuard();

  export function hasUnsavedChanges(): boolean {
    const editor = get(memoEditor);
    if (!editor || memoSaving) return false;
    return tagDraft.trim().length > 0 || Object.keys(buildMemoPatch(editor.row, {
      memo: memoDraft, tags: tagList, bookmarked: bookmarkDraft,
    })).length > 0;
  }

  // Automatic tab changes must retain the editor until a pending save has
  // succeeded; on failure this component still owns the draft for retry.
  export function hasUncommittedChanges(): boolean {
    return !!get(memoEditor) && (memoSaving || hasUnsavedChanges());
  }

  export function focusDraft(): void {
    textareaEl?.focus();
  }

  const closeMemoEditor = () => {
    memoEditor.set(null);
    memoDraft = '';
    memoError = null;
    memoSaving = false;
    tagDraft = '';
    tagList = [];
    bookmarkDraft = false;
    memoComposition.reset();
    tagComposition.reset();
  };

  const addTags = () => {
    const parts = tagDraft
      .split(',')
      .map((tag) => tag.trim())
      .filter((tag) => tag.length > 0);
    if (parts.length === 0) {
      tagDraft = '';
      return;
    }
    const merged = [...tagList];
    for (const tag of parts) {
      if (!merged.some((existing) => existing.toLowerCase() === tag.toLowerCase())) {
        merged.push(tag);
      }
    }
    tagList = merged;
    tagDraft = '';
  };

  const removeTag = (tag: string) => {
    tagList = tagList.filter((existing) => existing !== tag);
  };

  const saveMemo = async () => {
    const editor = get(memoEditor);
    if (!editor || memoSaving) return;
    addTags();
    const patch = buildMemoPatch(editor.row, { memo: memoDraft, tags: tagList, bookmarked: bookmarkDraft });
    if (Object.keys(patch).length === 0) {
      closeMemoEditor();
      return;
    }
    const backendInstance = get(backend);
    if (!backendInstance) {
      memoError = 'Backend is not available.';
      return;
    }

    memoSaving = true;
    memoError = null;
    // Disabled controls leave the tab order. Keep keyboard events in the
    // dialog while the accepted save is pending instead of focusing the table.
    backdropEl?.focus();

    try {
      const saved = await saveAnnotation(editor.projectId, editor.row, patch);
      if (!saved || get(currentProjectId) !== editor.projectId) return;

      dispatch('notify', { message: 'Note updated.', tone: 'success' });
      if (get(memoEditor) === editor) {
        closeMemoEditor();
      }
    } catch (error) {
      console.error('saveMemo: Backend updateFlag failed:', error);
      if (get(currentProjectId) !== editor.projectId) return;
      // The saved baseline is restored by the workspace; keep the draft for retry.
      memoError = 'Failed to update note.';
      dispatch('notify', { message: 'Failed to update note.', tone: 'error' });
    } finally {
      memoSaving = false;
      if (get(memoEditor) === editor && memoError) {
        await tick();
        if (get(memoEditor) === editor) textareaEl?.focus();
      }
    }
  };

  const handleMemoBackdropClick = (event: MouseEvent) => {
    if (event.target === event.currentTarget && !memoSaving) {
      closeMemoEditor();
    }
  };

  const handleMemoBackdropKey = (event: KeyboardEvent) => {
    if (isTabKey(event) && !event.altKey && !event.ctrlKey && !event.metaKey) {
      event.preventDefault();
      event.stopPropagation();
      const controls = [...(backdropEl?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), textarea:not(:disabled)') ?? [])]
        .filter(control => control.tabIndex >= 0 && control.getClientRects().length);
      if (!controls.length) { backdropEl?.focus(); return; }
      const index = controls.indexOf(document.activeElement as HTMLElement);
      const next = event.shiftKey ? (index <= 0 ? controls.length - 1 : index - 1)
        : (index + 1) % controls.length;
      (controls[next] ?? backdropEl)?.focus();
      return;
    }
    if (event.key !== 'Escape') return;
    // Escape may cancel the IME candidate window without closing the editor.
    event.stopPropagation();
    if (memoComposition.blocksShortcut(event) || tagComposition.blocksShortcut(event)) return;
    // Keep the dialog (and the draft) open while a save is in flight; the
    // window-level Escape handler must not close it either.
    event.preventDefault();
    if (!memoSaving) {
      closeMemoEditor();
    }
  };

  const stopMemoEditorSubscription = memoEditor.subscribe(async (editor) => {
    if (!editor) return;
    memoDraft = editor.row.memo ?? '';
    tagList = [...(editor.row.tags ?? [])];
    tagDraft = '';
    bookmarkDraft = editor.row.bookmarked ?? false;
    memoError = null;
    memoSaving = false;
    memoComposition.reset();
    tagComposition.reset();
    await tick();
    // The dialog may have been closed or retargeted while tick was pending.
    if (get(memoEditor) !== editor) return;
    if (textareaEl) {
      textareaEl.focus();
      // Place caret at end
      const length = textareaEl.value.length;
      textareaEl.setSelectionRange(length, length);
    }
  });

  onDestroy(stopMemoEditorSubscription);
</script>

{#if $memoEditor}
  <!-- svelte-ignore a11y-click-events-have-key-events -->
  <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
  <div
    class="cell-dialog-backdrop"
    bind:this={backdropEl}
    role="dialog"
    aria-modal="true"
    aria-label={`Edit note for row ${$memoEditor.rowIndex + 1}`}
    on:click={handleMemoBackdropClick}
    tabindex="-1"
    on:keydown={handleMemoBackdropKey}
  >
    <div class="cell-dialog memo-dialog">
      <div class="cell-dialog-header">
        <h3>Note · row {$memoEditor.rowIndex + 1}</h3>
        <button type="button" class="ghost dialog-close" aria-label="Close note editor" title="Close note editor"
          on:click={closeMemoEditor} disabled={memoSaving}>
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><path d="m6 6 12 12M18 6 6 18" /></svg>
        </button>
      </div>
      <label class="memo-editor-label">
        <span>Note</span>
        <textarea
          aria-label="Note"
          bind:this={textareaEl}
          bind:value={memoDraft}
          rows="8"
          placeholder="Add note"
          spellcheck="true"
          disabled={memoSaving}
          on:compositionstart={memoComposition.start}
          on:compositionend={memoComposition.end}
          on:keydown={(event) => {
            if (memoComposition.blocksShortcut(event)) return;
            if (event.key === 'Enter' && !event.shiftKey && !memoSaving) {
              event.preventDefault();
              event.stopPropagation();
              saveMemo();
            }
          }}
        />
      </label>
      <div class="memo-tags">
        <span class="memo-editor-caption">Tags</span>
        <div class="tag-chips">
          {#each tagList as tag}
            <span class="tag-chip">
              {tag}
              <button
                type="button"
                class="tag-chip-remove"
                aria-label={`Remove tag ${tag}`}
                on:click={() => removeTag(tag)}
                disabled={memoSaving}
              >
                ×
              </button>
            </span>
          {/each}
        </div>
        <input
          class="tag-input"
          bind:value={tagDraft}
          placeholder="Add tags (comma separated)"
          disabled={memoSaving}
          on:compositionstart={tagComposition.start}
          on:compositionend={tagComposition.end}
          on:blur={addTags}
          on:keydown={(event) => {
            if (tagComposition.blocksShortcut(event)) return;
            if (event.key === 'Enter' || event.key === ',') {
              event.preventDefault();
              addTags();
            }
          }}
        />
      </div>
      <label class="bookmark-toggle">
        <input
          type="checkbox"
          bind:checked={bookmarkDraft}
          disabled={memoSaving}
        />
        <span>★ Bookmark</span>
      </label>
      {#if memoError}
        <p class="memo-error">{memoError}</p>
      {/if}
      <div class="cell-dialog-actions note-actions dialog-actions">
        <button type="button" class="ghost" on:click={closeMemoEditor} disabled={memoSaving}>Cancel</button>
        <button type="button" class="primary" on:click={saveMemo} disabled={memoSaving}>
          {memoSaving ? 'Saving…' : 'Save'}
        </button>
      </div>
    </div>
  </div>
{/if}

<style>
  .note-actions { justify-content: flex-end; }
</style>
