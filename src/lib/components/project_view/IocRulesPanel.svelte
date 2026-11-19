<script lang="ts">
  import { onDestroy, tick } from 'svelte';
  import { get } from 'svelte/store';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import { iocEntryError } from '../../iocRules';
  import { SEARCH_SYNTAX_HELP } from '../../searchHelp';
  import { errorMessage } from '../../errors';
  import { acceptSave } from '../../saveDrain';
  import { useWorkspace } from '../../workspaceContext';
  import { FLAG_OPTIONS } from './state';
  import type { IocEntry } from '../../types';
  import FlagSelect from './FlagSelect.svelte';

  const { iocEditor, projectDetail, currentProjectId, columnLabels } = useWorkspace();
  const editorState = iocEditor.state;
  let panel: HTMLDivElement | null = null;
  let picking = false;
  let pickerError = '';
  let alive = true;
  let selectedId = '';
  let selectedPosition = 0;
  let focusTicket = 0;
  onDestroy(() => { alive = false; });
  $: busy = picking || $editorState.busy;
  $: dirty = ($editorState.entries, iocEditor.hasUnsavedChanges());
  $: columns = $projectDetail?.columns ?? [];
  $: selectedIndex = Math.max(0, $editorState.entries.findIndex(entry => entry.id === selectedId));
  $: activeIndex = $editorState.entries.some(entry => entry.id === selectedId) ? selectedIndex
    : Math.min(selectedPosition, Math.max(0, $editorState.entries.length - 1));
  $: activeEntry = $editorState.entries[activeIndex] ?? null;
  $: activeProblem = activeEntry ? iocEntryError(activeEntry, columns) : null;

  const summary = (entry: IocEntry) => {
    const text = entry.cell ? entry.cell.value : entry.query;
    return text === '' ? entry.cell ? 'Empty string' : 'New query rule' : text.slice(0, 180) + (text.length > 180 ? '…' : '');
  };

  export async function focusRule(id: string): Promise<void> {
    const index = get(editorState).entries.findIndex(entry => entry.id === id);
    if (!alive || index < 0) return;
    selectedId = id;
    selectedPosition = index;
    const ticket = ++focusTicket;
    await tick();
    if (!alive || ticket !== focusTicket) return;
    const row = [...(panel?.querySelectorAll<HTMLElement>('[data-rule-id]') ?? [])]
      .find(element => element.dataset.ruleId === id);
    row?.scrollIntoView({ block: 'nearest' });
    panel?.querySelector<HTMLElement>('.ioc-editor .ioc-query input, .ioc-editor .ioc-cell-value')?.focus({ preventScroll: true });
  }
  const add = () => { const id = iocEditor.add(); if (id) void focusRule(id); };
  const remove = (id: string) => {
    if (busy) return;
    const index = get(editorState).entries.findIndex(entry => entry.id === id);
    const wasActive = index === activeIndex;
    selectedPosition = Math.max(0, activeIndex - (index < activeIndex ? 1 : 0));
    if (wasActive) selectedId = '';
    iocEditor.remove(id);
    const next = get(editorState).entries[selectedPosition]?.id ?? get(editorState).entries.at(-1)?.id;
    if (wasActive && next) void focusRule(next);
  };
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
    <span class="condition-count" title="Apply searches every row, independently of table filters.">{$editorState.entries.length} rules · All rows</span>
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
  <div class="ioc-content">
    <div class="ioc-editor" aria-label="Edit selected IOC rule">
    {#if activeEntry}
      {@const entry = activeEntry}
      {@const id = entry.id ?? ''}
        <div class="ioc-labels">
          <span class="condition-count">Rule {activeIndex + 1}</span>
          <FlagSelect value={entry.flag} disabled={busy} on:change={event => iocEditor.update(id, 'flag', event.detail)} />
          <input value={entry.tag} placeholder="Tag" aria-label={`Rule tag ${activeIndex + 1}`}
            on:input={event => iocEditor.update(id, 'tag', event.currentTarget.value)} disabled={busy} />
        </div>
        {#if entry.cell}
          <div class="ioc-cell-label"><span>Cell equals</span>
            <select aria-label={`Rule column ${activeIndex + 1}`} disabled={busy} value={entry.cell.column}
              on:change={event => iocEditor.updateCell(id, event.currentTarget.value, entry.cell?.value ?? '')}>
              {#if !columns.includes(entry.cell.column)}<option value={entry.cell.column}>{entry.cell.column} (unavailable)</option>{/if}
              {#each columns as column}<option value={column}>{$columnLabels.get(column) ?? column}</option>{/each}
            </select>
          </div>
          <textarea class="ioc-cell-value" rows="2" value={entry.cell.value} aria-label={`Rule value ${activeIndex + 1}`}
            placeholder="Empty string" aria-invalid={activeProblem ? 'true' : undefined}
            aria-describedby={activeProblem ? 'ioc-editor-error' : undefined}
            title="Exact source text; an empty value matches empty strings. Null and missing fields are excluded."
            on:input={event => iocEditor.updateCell(id, entry.cell?.column ?? '', event.currentTarget.value)} disabled={busy}></textarea>
        {:else}
          <div class="ioc-query">
            <input value={entry.query} placeholder="Search query" aria-label={`Rule query ${activeIndex + 1}`} title={SEARCH_SYNTAX_HELP}
              aria-invalid={activeProblem ? 'true' : undefined} aria-describedby={activeProblem ? 'ioc-editor-error' : undefined}
              on:input={event => iocEditor.update(id, 'query', event.currentTarget.value)} disabled={busy} />
          </div>
        {/if}
      {#if activeProblem}<small id="ioc-editor-error" class="ioc-query-error" title={activeProblem}>{activeProblem}</small>
      {:else}<p class="condition-count ioc-intro">Save keeps rules. Apply changes matching rows.</p>{/if}
    {:else}<p class="condition-count ioc-empty">Add a rule to start.</p>{/if}
    </div>
    <div class="ioc-table" role="list" aria-label="Registered IOC rules">
      {#if $editorState.entries.length === 0}<p class="condition-count ioc-empty">No rules</p>{/if}
      {#each $editorState.entries as entry, index (entry.id)}
        {@const problem = iocEntryError(entry, columns)}
        {@const flag = FLAG_OPTIONS.find(option => option.value === entry.flag)}
        {@const id = entry.id ?? ''}
        <div class="ioc-row" class:selected={index === activeIndex} data-rule-id={entry.id} data-rule-index={index} role="listitem">
          <button type="button" class="ioc-select" aria-label={`Edit rule ${index + 1}`} aria-pressed={index === activeIndex}
            on:click={() => void focusRule(id)} title={problem ?? (entry.cell
              ? `${$columnLabels.get(entry.cell.column) ?? entry.cell.column} equals ${entry.cell.value === '' ? 'Empty string' : entry.cell.value}`
              : entry.query || 'Edit this rule')}>
            <span class={`ioc-list-flag flag-${entry.flag}`} data-flag={entry.flag} title={flag?.label ?? `Invalid flag: ${entry.flag || '(empty)'}`} aria-label={flag?.label ?? `Invalid flag: ${entry.flag || '(empty)'}`}>{flag?.hint ?? '!'}</span>
            <span class="ioc-summary">
              <span class="ioc-rule-text">{summary(entry)}</span>
              <span class="ioc-rule-meta">{entry.cell ? `${$columnLabels.get(entry.cell.column) ?? entry.cell.column} equals` : 'Query'}{entry.tag ? ` · ${entry.tag}` : ''}{problem ? ' · Needs attention' : ''}</span>
            </span>
          </button>
          <div class="ioc-rule-actions">
          <button type="button" class="ghost" title={problem ?? 'Set this flag and add this tag to matching rows'}
            on:click={() => void iocEditor.apply(id)} disabled={busy || Boolean(problem)}>Apply</button>
          <button type="button" class="icon-button danger" aria-label={`Delete rule ${index + 1}`} title="Delete rule"
            on:click={() => remove(id)} disabled={busy}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7M14 10v7" /></svg>
          </button>
          </div>
        </div>
      {/each}
    </div>
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
  .ioc-panel { display: flex; flex-direction: column; gap: 6px; min-width: 0; min-height: 0; height: 100%; }
  .ioc-controls, .ioc-labels, .ioc-cell-label, .ioc-rule-actions, .ioc-footer { display: flex; align-items: center; gap: 6px; }
  .ioc-controls button, .ioc-rule-actions button, .ioc-footer button { padding: 4px 7px; }
  .ioc-spacer { flex: 1; }
  .ioc-intro, .ioc-empty { margin: 0; }
  .ioc-content { display: flex; flex-direction: column; flex: 1; min-height: 0; gap: 6px; }
  .ioc-editor { display: flex; flex-direction: column; gap: 5px; flex: 0 1 auto; min-height: 65px; max-height: 33%; overflow: auto; padding-bottom: 6px; border-bottom: 1px solid var(--border); }
  .ioc-editor .ioc-empty { margin: auto 0; }
  .ioc-table { flex: 1; min-height: 60px; overflow: auto; overscroll-behavior: contain; }
  .ioc-row { display: flex; align-items: center; min-width: 0; min-height: 42px; border-bottom: 1px solid var(--border); border-left: 2px solid transparent; }
  .ioc-row.selected { border-left-color: var(--accent-border); background: var(--accent-soft); }
  .ioc-select { display: flex; align-items: center; gap: 7px; flex: 1; min-width: 0; padding: 4px 6px; text-align: left; border: 0; background: transparent; color: var(--fg); }
  .ioc-select:hover { background: var(--surface-weak); }
  .ioc-summary { display: flex; flex-direction: column; min-width: 0; gap: 2px; }
  .ioc-rule-text, .ioc-rule-meta { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ioc-rule-text { font-size: 11px; line-height: 14px; }
  .ioc-rule-meta { color: var(--muted); font-size: 10px; line-height: 12px; }
  .ioc-list-flag { display: inline-flex; align-items: center; justify-content: center; width: 22px; height: 22px; flex: 0 0 22px; font-size: 12px; font-weight: 700; }
  .ioc-list-flag.flag-safe { color: var(--flag-safe-text); }
  .ioc-list-flag.flag-suspicious { color: var(--flag-suspicious-text); }
  .ioc-list-flag.flag-critical, .ioc-list-flag:not(.flag-safe):not(.flag-suspicious) { color: var(--flag-critical-text); }
  .ioc-labels .condition-count { flex: 0 0 auto; }
  .ioc-labels :global(.flag-select) { flex: 0 0 112px; width: 112px; }
  .ioc-labels :global(.flag-select-trigger) { padding: 4px 7px; border-radius: 4px; font-size: 11px; gap: 6px; }
  .ioc-labels :global(.flag-label) { font-size: 10.5px; text-transform: none; letter-spacing: 0; }
  .ioc-labels :global(.flag-select-menu) { min-width: 132px; border-radius: 4px; padding: 3px; }
  .ioc-labels :global(.flag-option) { padding: 5px 7px; gap: 6px; }
  .ioc-labels input { flex: 1; width: 0; }
  .ioc-cell-label { color: var(--muted); font-size: 11px; }
  .ioc-cell-label select { flex: 1; min-width: 0; }
  .ioc-editor input, .ioc-editor textarea, .ioc-editor select { min-width: 0; width: 100%; padding: 4px 6px; border: 1px solid var(--border); border-radius: 3px; background: var(--surface); color: var(--fg); font-size: 11px; }
  .ioc-editor input:focus-visible, .ioc-editor textarea:focus-visible, .ioc-editor select:focus-visible, .ioc-select:focus-visible { outline: 2px solid var(--accent-border); outline-offset: -2px; }
  .ioc-query { min-width: 0; }
  .ioc-cell-value { resize: vertical; font-family: var(--font-mono); }
  .ioc-rule-actions { flex: 0 0 auto; gap: 3px; padding-right: 3px; }
  .ioc-rule-actions .icon-button { width: 25px; height: 25px; padding: 4px; }
  .ioc-query-error { color: var(--danger); font-size: 10px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; }
  .ioc-footer { flex-wrap: wrap; flex: 0 0 auto; padding-top: 6px; background: var(--surface); border-top: 1px solid var(--border); }
</style>
