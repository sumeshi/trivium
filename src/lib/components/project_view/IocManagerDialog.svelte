<script lang="ts">
  import { createEventDispatcher, onMount, onDestroy, tick } from 'svelte';
  import { open, save } from '@tauri-apps/api/dialog';
  import {
    iocManagerOpen,
    iocDraft,
    backend,
    projectDetail,
    normalizeIocFlag,
    FLAG_OPTIONS,
    escapeCsvValue,
    buildIocCsv,
    parseIocCsvText
  } from './state';
  import type { IocEntry } from '../../types';

  const dispatch = createEventDispatcher();

  let iocError: string | null = null;
  let isSavingIocs = false;
  let iocImportInput: HTMLInputElement | null = null;
  let dialogEl: HTMLDivElement | null = null;

  const closeIocManager = () => {
    iocManagerOpen.set(false);
    iocError = null;
    isSavingIocs = false;
  };

  const handleClickOutside = (event: MouseEvent) => {
    const target = event.target as HTMLElement;
    
    // IOC rulesボタンがクリックされた場合は閉じない
    if (target.textContent === 'IOC Rules') {
      return;
    }
    
    // IOC Managerダイアログの外部クリック
    if ($iocManagerOpen && dialogEl && !dialogEl.contains(target)) {
      closeIocManager();
    }
  };

  onMount(() => {
    document.addEventListener('click', handleClickOutside);
    
    // iocManagerOpenの変更を監視
    const unsubscribe = iocManagerOpen.subscribe(value => {
      console.log('iocManagerOpen changed to:', value);
    });
    
    return () => {
      unsubscribe();
    };
  });

  onDestroy(() => {
    document.removeEventListener('click', handleClickOutside);
  });

  const addIocEntry = async () => {
    iocDraft.update((d: IocEntry[]) => [...d, { flag: 'critical', tag: '', query: '' }]);
    
    // 新しいルールのTagフィールドにフォーカス
    await tick();
    const tagInputs = dialogEl?.querySelectorAll('.ioc-row input[placeholder="Tag name"]');
    if (tagInputs && tagInputs.length > 0) {
      const lastTagInput = tagInputs[tagInputs.length - 1] as HTMLInputElement;
      lastTagInput.focus();
    }
  };

  const updateIocEntry = (index: number, field: keyof IocEntry, value: string) => {
    iocDraft.update((d: IocEntry[]) => d.map((entry: IocEntry, current: number) => {
      if (current !== index) return entry;
      if (field === 'flag') {
        return { ...entry, flag: normalizeIocFlag(value) };
      }
      return { ...entry, [field]: value };
    }));
  };

  const removeIocEntry = (index: number) => {
    iocDraft.update((d: IocEntry[]) => d.filter((_: IocEntry, current: number) => current !== index));
  };

  const handleIocFieldChange = (index: number, field: keyof IocEntry, event: Event) => {
    const target = event.currentTarget as HTMLInputElement | HTMLSelectElement;
    updateIocEntry(index, field, target.value);
  };

  const sanitizeIocEntries = (): IocEntry[] =>
    $iocDraft
      .map((entry: IocEntry) => ({
        flag: normalizeIocFlag(entry.flag),
        tag: entry.tag.trim(),
        query: entry.query.trim()
      }))
      .filter((entry: IocEntry) => entry.query.length > 0)
      .sort((a: IocEntry, b: IocEntry) => a.tag.localeCompare(b.tag));

  const saveIocEntries = async () => {
    if (!$projectDetail) return;
    isSavingIocs = true;
    iocError = null;
    try {
      const sanitized = sanitizeIocEntries();
      await $backend.saveIocs({
        projectId: $projectDetail.project.meta.id,
        entries: sanitized
      });
      dispatch('notify', { message: 'IOC rules updated.', tone: 'success' });
      closeIocManager();
      dispatch('refresh');
    } catch (error) {
      console.error(error);
      iocError =
        error instanceof Error ? error.message : 'Failed to save IOC rules.';
    } finally {
      isSavingIocs = false;
    }
  }

  const importIocEntries = async () => {
    if (!$projectDetail) return;
    iocError = null;
    if ($backend.isNative) {
      try {
        const selected = await open({
          multiple: false,
          filters: [{ name: 'IOC CSV', extensions: ['csv'] }]
        });
        if (!selected) {
          return;
        }
        isSavingIocs = true;
        const path = Array.isArray(selected) ? selected[0] : selected;
        await $backend.importIocs({
          projectId: $projectDetail.project.meta.id,
          path
        });
        dispatch('notify', { message: 'Imported IOC rules.', tone: 'success' });
        closeIocManager();
        dispatch('refresh');
      } catch (error) {
        console.error(error);
        iocError =
          error instanceof Error ? error.message : 'Failed to import IOC rules.';
      } finally {
        isSavingIocs = false;
      }
    } else if (iocImportInput) {
      iocImportInput.value = '';
      iocImportInput.click();
    }
  };

  const exportIocEntries = async () => {
    if (!$projectDetail) return;
    try {
      if ($backend.isNative) {
        const destination = await save({
          filters: [{ name: 'IOC CSV', extensions: ['csv'] }],
          defaultPath: `${$projectDetail.project.meta.name.replace(/\.[^.]+$/, '')}-iocs.csv`
        });
        if (!destination) {
          return;
        }
        await $backend.exportIocs({
          projectId: $projectDetail.project.meta.id,
          destination
        });
      } else {
        const csv = buildIocCsv(sanitizeIocEntries());
        const blob = new Blob([csv], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = `${$projectDetail.project.meta.name.replace(/\.[^.]+$/, '')}-iocs.csv`;
        document.body.appendChild(anchor);
        anchor.click();
        document.body.removeChild(anchor);
        URL.revokeObjectURL(url);
      }
      dispatch('notify', { message: 'Exported IOC rules.', tone: 'success' });
    } catch (error) {
      console.error(error);
      iocError =
        error instanceof Error ? error.message : 'Failed to export IOC rules.';
    }
  };

  const handleIocFileUpload = async (event: Event) => {
    if (!$projectDetail) return;
    const target = event.currentTarget as HTMLInputElement | null;
    const file = target?.files?.[0];
    if (!file) return;
    isSavingIocs = true;
    try {
      const text = await file.text();
      const imported = parseIocCsvText(text);
      if (!imported.length) {
        iocError = 'No IOC entries found in selected file.';
        return;
      }
      await $backend.saveIocs({
        projectId: $projectDetail.project.meta.id,
        entries: imported
      });
      dispatch('notify', { message: 'Imported IOC rules.', tone: 'success' });
      closeIocManager();
      dispatch('refresh');
    } catch (error) {
      console.error(error);
      iocError =
        error instanceof Error ? error.message : 'Failed to import IOC rules.';
    } finally {
      if (iocImportInput) {
        iocImportInput.value = '';
      }
      isSavingIocs = false;
    }
  };
</script>

{#if $iocManagerOpen}
  <!-- DEBUG: IOC Dialog should be visible -->
  <div class="cell-dialog-backdrop" role="dialog" aria-modal="true" aria-label="IOC rules">
    <div class="cell-dialog ioc-dialog" bind:this={dialogEl}>
      <div class="cell-dialog-header">
        <h3>IOC Rules</h3>
      </div>
      <div class="ioc-controls">
        <button type="button" class="ghost" on:click={addIocEntry}>
          Add rule
        </button>
        <div class="ioc-spacer" />
        <button type="button" class="ghost" on:click={importIocEntries} disabled={isSavingIocs}>
          Import…
        </button>
        <button type="button" class="ghost" on:click={exportIocEntries} disabled={isSavingIocs}>
          Export…
        </button>
      </div>
      <div class="ioc-table">
        <div class="ioc-header">
          <span>Flag</span>
          <span>Tag</span>
          <span>Query</span>
          <span></span>
        </div>
        {#if $iocDraft.length === 0}
          <p class="ioc-empty">No IOC rules configured.</p>
        {:else}
          {#each $iocDraft as entry, index (index)}
            <div class="ioc-row">
              <select
                bind:value={entry.flag}
                on:change={(event) => handleIocFieldChange(index, 'flag', event)}
                disabled={isSavingIocs}
              >
                {#each FLAG_OPTIONS as option}
                  <option value={option.value}>{option.label}</option>
                {/each}
              </select>
              <input
                bind:value={entry.tag}
                placeholder="Tag name"
                on:input={(event) => handleIocFieldChange(index, 'tag', event)}
                disabled={isSavingIocs}
              />
              <input
                bind:value={entry.query}
                placeholder="Search string"
                on:input={(event) => handleIocFieldChange(index, 'query', event)}
                disabled={isSavingIocs}
              />
              <button
                type="button"
                class="ghost danger"
                on:click={() => removeIocEntry(index)}
                disabled={isSavingIocs}
              >
                Delete
              </button>
            </div>
          {/each}
        {/if}
      </div>
      {#if iocError}
        <p class="memo-error">{iocError}</p>
      {/if}
      <div class="ioc-footer">
        <button type="button" class="ghost" on:click={closeIocManager} disabled={isSavingIocs}>
          Cancel
        </button>
        <button type="button" class="primary" on:click={saveIocEntries} disabled={isSavingIocs}>
          {isSavingIocs ? 'Saving…' : 'Save'}
        </button>
      </div>
      {#if !$backend.isNative}
        <input
          type="file"
          accept=".csv"
          class="hidden-input"
          bind:this={iocImportInput}
          on:change={handleIocFileUpload}
        />
      {/if}
    </div>
  </div>
{/if}