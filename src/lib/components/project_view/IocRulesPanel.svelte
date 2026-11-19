<script lang="ts">
  import { onDestroy, tick } from 'svelte';
  import { get } from 'svelte/store';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import { iocEntryError } from '../../iocRules';
  import { SEARCH_SYNTAX_HELP } from '../../searchHelp';
  import { errorMessage } from '../../errors';
  import { acceptSave } from '../../saveDrain';
  import { useWorkspace } from '../../workspaceContext';
  import FlagSelect from './FlagSelect.svelte';

  const { iocEditor, projectDetail, currentProjectId, columnLabels } = useWorkspace();
  const editorState = iocEditor.state;
  let panel: HTMLDivElement | null = null;
  let picking = false;
  let pickerError = '';
  let alive = true;
  onDestroy(() => { alive = false; });
  $: busy = picking || $editorState.busy;
  $: dirty = ($editorState.entries, iocEditor.hasUnsavedChanges());
  $: columns = $projectDetail?.columns ?? [];

  export async function focusRule(id: string): Promise<void> {
    await tick();
    if (!alive) return;
    const row = [...(panel?.querySelectorAll<HTMLElement>('[data-rule-id]') ?? [])]
      .find(element => element.dataset.ruleId === id);
    row?.scrollIntoView({ block: 'nearest' });
    row?.querySelector<HTMLElement>('.ioc-query input, .ioc-cell-value')?.focus({ preventScroll: true });
  }
  const add = () => { const id = iocEditor.add(); if (id) void focusRule(id); };
  const pick = async (operation: 'import' | 'export') => {
    if (busy) return;
    const owner = get(currentProjectId);
    const stem = ($projectDetail?.project.meta.source_file_name ?? $projectDetail?.project.meta.name ?? 'rules').replace(/\.[^.]+$/, '');
    picking = true; pickerError = '';
    try {
      await acceptSave(async () => {
        const filters = [{ name: 'IOC rules', extensions: ['csv'] }];
        const path = operation === 'import' ? await open({ multiple: false, filters })
          : await save({ filters, defaultPath: `trivium-iocs-${stem}.csv` });
        if (!path || !alive || get(currentProjectId) !== owner) return;
        const selected = Array.isArray(path) ? path[0] : path;
        if (operation === 'import') await iocEditor.importFile(selected);
        else await iocEditor.exportFile(selected);
      });
    } catch (error) { if (alive && get(currentProjectId) === owner) pickerError = errorMessage(error); }
    finally { if (alive) picking = false; }
  };
</script>

<div class="ioc-panel" bind:this={panel} aria-label="IOC rules">
  <div class="ioc-controls">
    <button type="button" class="ghost" on:click={add} disabled={busy}>Add rule</button>
    <span class="condition-count" title="Apply searches every row, independently of table filters.">All rows</span>
    <span class="ioc-spacer"></span>
    <button type="button" class="icon-button" aria-label="Import IOC rules" title="Replace saved rules from CSV"
      on:click={() => void pick('import')} disabled={busy}>
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M12 3v12m-4-4 4 4 4-4M4 18v3h16v-3" /></svg>
    </button>
    <button type="button" class="icon-button" aria-label="Export IOC rules" title="Export these rules to CSV"
      on:click={() => void pick('export')} disabled={busy}>
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M12 16V4m-4 4 4-4 4 4M4 18v3h16v-3" /></svg>
    </button>
  </div>
  <p class="condition-count ioc-intro">Save keeps rules. Apply changes matching rows.</p>
  <div class="ioc-table">
    {#if $editorState.entries.length === 0}<p class="condition-count ioc-empty">No rules</p>{/if}
    {#each $editorState.entries as entry, index (entry.id)}
      {@const problem = iocEntryError(entry, columns)}
      {@const id = entry.id ?? ''}
      <div class="ioc-row" data-rule-id={entry.id}>
        <div class="ioc-labels">
          <FlagSelect value={entry.flag} disabled={busy} on:change={event => iocEditor.update(id, 'flag', event.detail)} />
          <input value={entry.tag} placeholder="Tag" aria-label={`Rule tag ${index + 1}`}
            on:input={event => iocEditor.update(id, 'tag', event.currentTarget.value)} disabled={busy} />
        </div>
        {#if entry.cell}
          <div class="ioc-cell-label"><span>Cell equals</span>
            <select aria-label={`Rule column ${index + 1}`} disabled={busy} value={entry.cell.column}
              on:change={event => iocEditor.updateCell(id, event.currentTarget.value, entry.cell?.value ?? '')}>
              {#if !columns.includes(entry.cell.column)}<option value={entry.cell.column}>{entry.cell.column} (unavailable)</option>{/if}
              {#each columns as column}<option value={column}>{$columnLabels.get(column) ?? column}</option>{/each}
            </select>
          </div>
          <textarea class="ioc-cell-value" rows="2" value={entry.cell.value} aria-label={`Rule value ${index + 1}`}
            placeholder="Empty string" aria-invalid={problem ? 'true' : undefined}
            aria-describedby={problem ? `ioc-rule-error-${index}` : undefined}
            title="Exact source text; an empty value matches empty strings. Null and missing fields are excluded."
            on:input={event => iocEditor.updateCell(id, entry.cell?.column ?? '', event.currentTarget.value)} disabled={busy}></textarea>
        {:else}
          <div class="ioc-query">
            <input value={entry.query} placeholder="Search query" aria-label={`Rule query ${index + 1}`} title={SEARCH_SYNTAX_HELP}
              aria-invalid={problem ? 'true' : undefined} aria-describedby={problem ? `ioc-rule-error-${index}` : undefined}
              on:input={event => iocEditor.update(id, 'query', event.currentTarget.value)} disabled={busy} />
          </div>
        {/if}
        <div class="ioc-rule-actions">
          {#if problem}<small id={`ioc-rule-error-${index}`} class="ioc-query-error">{problem}</small>{/if}
          <button type="button" class="ghost" title={problem ?? 'Set this flag and add this tag to matching rows'}
            on:click={() => void iocEditor.apply(id)} disabled={busy || Boolean(problem)}>Apply</button>
          <button type="button" class="icon-button danger" aria-label={`Delete rule ${index + 1}`} title="Delete rule"
            on:click={() => iocEditor.remove(id)} disabled={busy}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7M14 10v7" /></svg>
          </button>
        </div>
      </div>
    {/each}
  </div>
  {#if pickerError || $editorState.error}<p class="memo-error" role="alert">{pickerError || $editorState.error}</p>
  {:else if $editorState.notice || busy}<p class="condition-count" role="status">{busy ? 'Working…' : $editorState.notice}</p>{/if}
  <div class="ioc-footer">
    <button type="button" class="ghost" on:click={() => void iocEditor.undo()} disabled={busy}
      title="Restore the last bulk operation in this case">Undo</button>
    <span class="ioc-spacer"></span>
    <button type="button" class="ghost" on:click={() => iocEditor.discard()} disabled={busy || !dirty}>Discard</button>
    <button type="button" class="primary" on:click={() => void iocEditor.save()} disabled={busy || !dirty}>Save rules</button>
  </div>
</div>

<style>
  .ioc-panel { display: flex; flex-direction: column; gap: 8px; min-width: 0; }
  .ioc-controls, .ioc-labels, .ioc-cell-label, .ioc-rule-actions, .ioc-footer { display: flex; align-items: center; gap: 6px; }
  .ioc-controls button, .ioc-rule-actions button, .ioc-footer button { padding: 4px 7px; }
  .ioc-spacer { flex: 1; }
  .ioc-intro, .ioc-empty { margin: 0; }
  .ioc-table { display: flex; flex-direction: column; gap: 8px; max-height: none; overflow: visible; }
  .ioc-row { display: grid; grid-template-columns: minmax(0, 1fr); gap: 5px; padding: 6px 0 8px; border-bottom: 1px solid var(--border); }
  .ioc-labels :global(.flag-select) { flex: 0 0 122px; width: 122px; }
  .ioc-labels :global(.flag-select-trigger) { padding: 4px 7px; border-radius: 4px; font-size: 11px; gap: 6px; }
  .ioc-labels :global(.flag-label) { font-size: 10.5px; text-transform: none; letter-spacing: 0; }
  .ioc-labels input { flex: 1; width: 0; }
  .ioc-cell-label { color: var(--muted); font-size: 11px; }
  .ioc-cell-label select { flex: 1; min-width: 0; }
  .ioc-row input, .ioc-row textarea, .ioc-row select { min-width: 0; width: 100%; padding: 4px 6px; border: 1px solid var(--border); border-radius: 3px; background: var(--surface); color: var(--fg); font-size: 11px; }
  .ioc-row input:focus-visible, .ioc-row textarea:focus-visible, .ioc-row select:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
  .ioc-query { min-width: 0; }
  .ioc-cell-value { resize: vertical; font-family: var(--font-mono); }
  .ioc-rule-actions { justify-content: flex-end; }
  .ioc-query-error { flex: 1; color: var(--danger); font-size: 10px; }
  .ioc-footer { flex-wrap: wrap; position: sticky; bottom: -8px; padding: 6px 0; background: var(--surface); border-top: 1px solid var(--border); z-index: 1; }
</style>
