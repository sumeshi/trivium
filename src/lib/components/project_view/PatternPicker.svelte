<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { get } from 'svelte/store';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import { readTextFile, writeTextFile, stat } from '@tauri-apps/plugin-fs';
  import { useWorkspace } from '../../workspaceContext';
  import { formats, formatCatalogError, ensureFormatsLoaded, saveCustomFormat, deleteCustomFormat } from '../../patternCatalog';
  import { formatFileName, parseFormatDefinition } from '../../formats';
  import type { FormatCatalogEntry, FormatDefinition } from '../../formats';
  import { acceptSave } from '../../saveDrain';
  import { errorMessage } from '../../errors';

  const { backend } = useWorkspace();
  const dispatch = createEventDispatcher<{ choose: FormatDefinition }>();
  let find = '', name = '', pattern = '', valueGroup = '', json = '', error = '';
  let editing: 'new' | 'json' | null = null;
  let busy = false, loading = true, destroyed = false;
  $: visible = $formats.filter(entry => entry.definition.name.toLocaleLowerCase().includes(find.trim().toLocaleLowerCase()));
  onDestroy(() => { destroyed = true; });
  onMount(() => { void reload(); });

  async function reload() {
    const service = get(backend);
    if (!service) return;
    loading = true;
    try { await ensureFormatsLoaded(service, Boolean($formatCatalogError)); }
    catch { /* The shared catalog displays the load error. */ }
    finally { if (!destroyed) loading = false; }
  }
  function newPattern() {
    name = pattern = valueGroup = error = ''; editing = 'new';
  }
  function edit(entry: FormatCatalogEntry) {
    const definition = structuredClone(entry.definition);
    if (entry.builtin) { definition.id = `user-${crypto.randomUUID()}`; definition.name += ' copy'; }
    json = JSON.stringify(definition, null, 2); error = ''; editing = 'json';
  }
  async function storePattern() {
    const service = get(backend);
    if (!service || busy) return;
    busy = true; error = '';
    try {
      const definition = editing === 'json' ? parseFormatDefinition(JSON.parse(json)) : {
        schemaVersion: 1, id: `user-${crypto.randomUUID()}`, name: name.trim(), pattern,
        ...(valueGroup.trim() ? { valueGroup: valueGroup.trim() } : {}),
      };
      await saveCustomFormat(service, definition);
      if (!destroyed) editing = null;
    } catch (reason) { if (!destroyed) error = errorMessage(reason, 'Could not save pattern.'); }
    finally { if (!destroyed) busy = false; }
  }
  async function remove(entry: FormatCatalogEntry) {
    const service = get(backend);
    if (!service || busy || entry.builtin) return;
    busy = true; error = '';
    try { await deleteCustomFormat(service, entry.definition.id); }
    catch (reason) { if (!destroyed) error = errorMessage(reason, 'Could not delete pattern.'); }
    finally { if (!destroyed) busy = false; }
  }
  async function importPattern() {
    const service = get(backend);
    if (!service || busy) return;
    busy = true; error = '';
    try {
      await acceptSave(async () => {
        const source = await open({ multiple: false, filters: [{ name: 'Trivium pattern', extensions: ['json'] }] });
        if (!source || destroyed) return;
        const path = Array.isArray(source) ? source[0] : source;
        const info = await stat(path);
        if (!info.isFile || info.size > 1024 * 1024) throw new Error('Choose a pattern JSON file smaller than 1 MiB.');
        const text = await readTextFile(path);
        if (text.length > 1024 * 1024) throw new Error('Pattern JSON must be smaller than 1 MiB.');
        const definition = parseFormatDefinition(JSON.parse(text));
        if (destroyed) return;
        if ($formats.some(entry => entry.builtin && entry.definition.id === definition.id)) {
          definition.id = `user-${crypto.randomUUID()}`;
          definition.name += ' copy';
        }
        // Inspect imported IDs and examples before saving.
        json = JSON.stringify(definition, null, 2); editing = 'json';
      });
    } catch (reason) { if (!destroyed) error = errorMessage(reason, 'Could not read pattern file.'); }
    finally { if (!destroyed) busy = false; }
  }
  async function exportPattern(definition: FormatDefinition) {
    if (busy) return;
    const snapshot = structuredClone(definition);
    busy = true; error = '';
    try {
      await acceptSave(async () => {
        const destination = await save({ defaultPath: formatFileName(snapshot),
          filters: [{ name: 'Trivium pattern', extensions: ['json'] }] });
        if (!destination || destroyed) return;
        await writeTextFile(destination, JSON.stringify(snapshot, null, 2) + '\n');
      });
    } catch (reason) { if (!destroyed) error = errorMessage(reason, 'Could not export pattern.'); }
    finally { if (!destroyed) busy = false; }
  }
</script>

<div class="pattern-picker">
  {#if editing}
    <div class="pattern-form" aria-label="Pattern editor">
      {#if editing === 'new'}
        <label>Name<input aria-label="Pattern name" bind:value={name} autocomplete="off" /></label>
        <label>Regular expression<textarea aria-label="Pattern expression" bind:value={pattern} rows="3" spellcheck="false" placeholder="(?i)error|warning" /></label>
        <label>Capture group<input aria-label="Pattern capture group" bind:value={valueGroup} placeholder="Whole match" autocomplete="off" /></label>
      {:else}
        <label>Definition JSON<textarea class="definition-json" aria-label="Pattern definition JSON" bind:value={json} rows="12" spellcheck="false" /></label>
      {/if}
      <div class="pattern-actions">
        <button type="button" on:click={() => { editing = null; error = ''; }} disabled={busy}>Cancel</button>
        <button type="button" class="primary" on:click={storePattern} disabled={busy || (editing === 'new' && (!name.trim() || !pattern))}>{busy ? 'Saving…' : 'Save pattern'}</button>
      </div>
    </div>
  {:else}
    <input class="pattern-find" type="search" aria-label="Find a pattern" placeholder="Find pattern" bind:value={find} />
    <div class="pattern-list" aria-label="Pattern definitions">
      {#each visible as entry (entry.definition.id)}
        <div class="pattern-row">
          <button type="button" class="pattern-select" aria-label={`Use pattern ${entry.definition.name}`} title={entry.definition.pattern} disabled={busy}
            on:click={() => dispatch('choose', structuredClone(entry.definition))}>
            <strong>{entry.definition.name}</strong><span>{entry.definition.pattern}</span>
          </button>
          <button type="button" class="pattern-icon" aria-label={`${entry.builtin ? 'Copy' : 'Edit'} ${entry.definition.name}`} title={entry.builtin ? 'Copy definition' : 'Edit definition'} on:click={() => edit(entry)} disabled={busy}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path d="m15 4 5 5M4 20l5-1L21 7a2 2 0 0 0-5-5L4 14Z" /></svg>
          </button>
          <button type="button" class="pattern-icon" aria-label={`Export ${entry.definition.name}`} title="Export definition" on:click={() => exportPattern(entry.definition)} disabled={busy}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path d="M12 3v12m-4-4 4 4 4-4M4 15v6h16v-6" /></svg>
          </button>
          {#if !entry.builtin}
            <button type="button" class="pattern-icon" aria-label={`Delete ${entry.definition.name}`} title="Delete definition" on:click={() => remove(entry)} disabled={busy}>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7m4-7v7" /></svg>
            </button>
          {/if}
        </div>
      {/each}
      {#if loading}<p role="status">Loading patterns…</p>{:else if !visible.length}<p>No matching patterns</p>{/if}
    </div>
    <div class="pattern-actions">
      <button type="button" on:click={newPattern} disabled={busy}>New pattern</button>
      <button type="button" on:click={importPattern} disabled={busy}>Import JSON</button>
      <button type="button" class="pattern-icon" aria-label="Reload pattern definitions" title="Reload definitions" on:click={() => { const service = get(backend); if (service) void ensureFormatsLoaded(service, true).catch(() => {}); }} disabled={busy || loading}>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path d="M20 7v5h-5M4 17v-5h5M6 7a7 7 0 0 1 12-2l2 7M4 12l2 7a7 7 0 0 0 12-2" /></svg>
      </button>
    </div>
  {/if}
  {#if error || $formatCatalogError}<p class="pattern-error" role="alert">{error || $formatCatalogError}</p>{/if}
</div>

<style>
  .pattern-picker { min-width: 0; font-size: 11px; }
  input, textarea { width: 100%; min-width: 0; padding: 5px 7px; color: var(--fg); background: var(--surface); border: 1px solid var(--border); border-radius: 4px; font: inherit; }
  textarea { font-family: var(--font-mono); resize: vertical; }
  .pattern-find { margin-bottom: 5px; }
  .pattern-list { max-height: min(300px, 40vh); overflow: auto; }
  .pattern-row { display: flex; align-items: center; gap: 2px; border-bottom: 1px solid var(--border); }
  .pattern-select { min-width: 0; flex: 1; display: flex; flex-direction: column; align-items: start; gap: 2px; padding: 5px; text-align: left; border: 0; background: transparent; color: var(--fg); }
  .pattern-select span { display: block; width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--muted); font-family: var(--font-mono); }
  .pattern-select:hover { background: var(--surface-weak); }
  .pattern-icon { flex: 0 0 25px; width: 25px; height: 25px; display: inline-flex; align-items: center; justify-content: center; padding: 2px; color: var(--muted); border: 1px solid transparent; background: transparent; border-radius: 4px; }
  .pattern-icon:hover { color: var(--fg); background: var(--surface-weak); }
  .pattern-actions { display: flex; gap: 5px; align-items: center; justify-content: flex-end; margin-top: 7px; }
  .pattern-actions > button:not(.pattern-icon) { padding: 4px 7px; border: 1px solid var(--border); border-radius: 4px; color: var(--fg); background: var(--surface); font: inherit; }
  .pattern-actions > button.primary { color: var(--accent-contrast); background: var(--accent); border-color: var(--accent); }
  .pattern-form { display: grid; gap: 6px; }
  label { display: grid; gap: 3px; color: var(--muted); }
  p { margin: 5px 0; color: var(--muted); }
  .pattern-error { color: var(--danger); overflow-wrap: anywhere; white-space: pre-wrap; }
  button:disabled { opacity: .5; cursor: default; }
  button:focus-visible, input:focus-visible, textarea:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
</style>
