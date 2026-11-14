<script lang="ts">
  import { createEventDispatcher, onDestroy, tick } from 'svelte';
  import { get } from 'svelte/store';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import { errorMessage } from '../../errors';
  import { createLatestRequest } from '../../latestRequest';
  import { isTabKey } from '../../keyboard';
  import { iocBulkArgs, iocRuleKey } from '../../iocRules';
  import { normalizeIocFlag } from './state';
  import type { IocEntry } from '../../types';
  import { useWorkspace } from '../../workspaceContext';
  import FlagSelect from './FlagSelect.svelte';

  const { iocManagerOpen, iocDraft, backend, projectDetail, currentProjectId, createReadScope } = useWorkspace();
  const reads = createReadScope();
  const requests = createLatestRequest();
  const dispatch = createEventDispatcher<{ notify: { message: string; tone: 'success' | 'error' }; refresh: void; annotationsChanged: void }>();
  let error = '';
  let notice = '';
  let busy = false;
  let alive = true;
  let dialogEl: HTMLDivElement | null = null;
  let previousFocus: HTMLElement | null = null;
  let preview: { key: string; token: string; count: number } | null = null;

  const invalidatePreview = () => { preview = null; reads.cancelAll(); };
  const focusDialog = async () => {
    previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    error = ''; notice = ''; busy = false; invalidatePreview(); requests.invalidate();
    await tick();
    dialogEl?.querySelector<HTMLButtonElement>('[aria-label="Close IOC rules"]')?.focus();
  };
  $: if ($iocManagerOpen) void focusDialog();
  $: if (!$iocManagerOpen) { reads.cancelAll(); requests.invalidate(); }
  $: projectId = $projectDetail?.project.meta.id ?? '';
  $: if (preview && !$iocDraft.some(entry => iocRuleKey(projectId, entry) === preview?.key)) preview = null;

  const close = async () => {
    if (busy) return;
    const owner = projectId;
    requests.invalidate(); invalidatePreview();
    iocManagerOpen.set(false);
    await tick();
    if (!alive || get(currentProjectId) !== owner) return;
    if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true });
    else document.querySelector<HTMLButtonElement>('[aria-label="Open Annotations"]')?.focus();
  };
  const keydown = (event: KeyboardEvent) => {
    if (!$iocManagerOpen || !dialogEl?.contains(event.target as Node)) return;
    if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); void close(); return; }
    if (!isTabKey(event) || !dialogEl) return;
    const items = [...dialogEl.querySelectorAll<HTMLElement>('button:not(:disabled),input:not(:disabled),select:not(:disabled)')];
    const first = items[0], last = items.at(-1);
    if (!first || !last) { event.preventDefault(); dialogEl.focus(); return; }
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  };
  onDestroy(() => { alive = false; requests.dispose(); reads.dispose(); });

  const begin = () => {
    const api = get(backend), owner = get(currentProjectId);
    if (!api || !owner || busy) return null;
    const ticket = requests.next();
    busy = true; error = ''; notice = '';
    return { api, owner, isCurrent: () => requests.isCurrent(ticket) && get(currentProjectId) === owner && get(iocManagerOpen) };
  };
  const add = async () => {
    if (busy) return;
    invalidatePreview();
    iocDraft.update(entries => [...entries, { id: crypto.randomUUID(), flag: 'critical', tag: '', query: '' }]);
    await tick();
    const inputs = dialogEl?.querySelectorAll<HTMLInputElement>('.ioc-row input[aria-label^="Rule query"]');
    inputs?.[inputs.length - 1]?.focus();
  };
  const update = (index: number, field: keyof IocEntry, value: string) => {
    invalidatePreview();
    iocDraft.update(entries => entries.map((entry, at) => at !== index ? entry
      : { ...entry, [field]: field === 'flag' ? normalizeIocFlag(value) : value }));
  };
  const remove = (index: number) => {
    invalidatePreview();
    iocDraft.update(entries => entries.filter((_, at) => at !== index));
  };
  const entriesToSave = (): IocEntry[] => $iocDraft.map((entry, index) => {
    const tag = entry.tag.trim(), query = entry.query.trim();
    if (tag && !query) throw new Error(`Enter a query for rule ${index + 1}.`);
    return { flag: normalizeIocFlag(entry.flag), tag, query };
  }).filter(entry => entry.query.length > 0);

  const saveRules = async () => {
    const active = begin(); if (!active) return;
    try {
      await active.api.saveIocs({ projectId: active.owner, entries: entriesToSave() });
      if (!active.isCurrent()) return;
      busy = false; await close();
      if (!alive || get(currentProjectId) !== active.owner) return;
      dispatch('notify', { message: 'IOC rules saved.', tone: 'success' });
      dispatch('refresh');
    } catch (err) { if (active.isCurrent()) error = errorMessage(err, 'Could not save rules.'); }
    finally { if (active.isCurrent()) busy = false; }
  };
  const importRules = async () => {
    const active = begin(); if (!active) return;
    try {
      const selected = await open({ multiple: false, filters: [{ name: 'IOC rules', extensions: ['csv'] }] });
      if (!selected || !active.isCurrent()) return;
      const entries = await active.api.importIocs({ projectId: active.owner, path: Array.isArray(selected) ? selected[0] : selected });
      if (!active.isCurrent()) return;
      iocDraft.set(entries.map(entry => ({ ...entry, id: crypto.randomUUID() })));
      busy = false; await close();
      if (!alive || get(currentProjectId) !== active.owner) return;
      dispatch('notify', { message: 'IOC rules imported.', tone: 'success' }); dispatch('refresh');
    } catch (err) { if (active.isCurrent()) error = errorMessage(err, 'Could not import rules.'); }
    finally { if (active.isCurrent()) busy = false; }
  };
  const exportRules = async () => {
    const active = begin(); if (!active) return;
    try {
      const entries = entriesToSave();
      if (!entries.length) throw new Error('No rules to export.');
      const stem = ($projectDetail?.project.meta.source_file_name ?? $projectDetail?.project.meta.name ?? 'rules').replace(/\.[^.]+$/, '');
      const destination = await save({ filters: [{ name: 'IOC rules', extensions: ['csv'] }], defaultPath: `trivium-iocs-${stem}.csv` });
      if (!destination || !active.isCurrent()) return;
      await active.api.exportIocs({ projectId: active.owner, destination, entries });
      if (active.isCurrent()) notice = 'Rules exported.';
    } catch (err) { if (active.isCurrent()) error = errorMessage(err, 'Could not export rules.'); }
    finally { if (active.isCurrent()) busy = false; }
  };
  const runRule = async (entry: IocEntry, apply: boolean) => {
    const active = begin(); if (!active) return;
    const key = iocRuleKey(active.owner, entry);
    try {
      if (apply) {
        if (!preview || preview.key !== key) throw new Error('Preview this rule first.');
        const result = await active.api.bulkUpdate(iocBulkArgs(active.owner, entry, preview.token));
        if (!active.isCurrent()) return;
        notice = `${result.updated.toLocaleString()} rows annotated.`;
        invalidatePreview(); dispatch('annotationsChanged');
      } else {
        invalidatePreview();
        const args = iocBulkArgs(active.owner, entry);
        const result = await reads.run('preview', active.api, requestId => active.api.bulkPreview({ ...args, requestId }));
        if (active.isCurrent()) preview = { key, token: result.previewToken, count: result.count };
      }
    } catch (err) {
      if (active.isCurrent()) { invalidatePreview(); error = errorMessage(err, 'Could not apply rule.'); }
    } finally { if (active.isCurrent()) busy = false; }
  };
  const undo = async () => {
    const active = begin(); if (!active) return;
    try {
      const result = await active.api.bulkUndo({ projectId: active.owner });
      if (!active.isCurrent()) return;
      invalidatePreview(); notice = `${result.updated.toLocaleString()} rows restored.`; dispatch('annotationsChanged');
    } catch (err) { if (active.isCurrent()) error = errorMessage(err, 'Could not undo.'); }
    finally { if (active.isCurrent()) busy = false; }
  };
</script>

<svelte:document on:keydown={keydown} />

{#if $iocManagerOpen}
  <div class="cell-dialog-backdrop" role="presentation" on:mousedown|self={() => void close()}>
    <div class="cell-dialog ioc-dialog" bind:this={dialogEl} role="dialog" aria-modal="true" aria-labelledby="ioc-title" tabindex="-1">
      <div class="cell-dialog-header">
        <h3 id="ioc-title">IOC Rules</h3>
        <button type="button" class="ghost ioc-icon" aria-label="Close IOC rules" title="Close" on:click={() => void close()} disabled={busy}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="m6 6 12 12M6 18 18 6" /></svg>
        </button>
      </div>
      <div class="ioc-controls">
        <button type="button" class="ghost" on:click={() => void add()} disabled={busy}>Add rule</button>
        <span class="condition-count" title="Each rule searches all rows and columns. Apply sets its flag and adds its tag.">All rows · Flag + tag</span>
        <div class="ioc-spacer"></div>
        <button type="button" class="ghost ioc-icon" aria-label="Import IOC rules" title="Replace saved rules from CSV" on:click={() => void importRules()} disabled={busy}>
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M12 3v12m-4-4 4 4 4-4M4 18v3h16v-3" /></svg>
        </button>
        <button type="button" class="ghost ioc-icon" aria-label="Export IOC rules" title="Export these rules to CSV" on:click={() => void exportRules()} disabled={busy}>
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M12 16V4m-4 4 4-4 4 4M4 18v3h16v-3" /></svg>
        </button>
      </div>
      <div class="ioc-table">
        <div class="ioc-header"><span>Flag</span><span>Tag</span><span>Query</span><span></span></div>
        {#if $iocDraft.length === 0}
          <p class="ioc-empty">No rules</p>
        {:else}
          {#each $iocDraft as entry, index (entry.id || `entry-${index}`)}
            {@const matchingPreview = preview?.key === iocRuleKey(projectId, entry) ? preview : null}
            <div class="ioc-row">
              <FlagSelect value={entry.flag} disabled={busy} on:change={event => update(index, 'flag', event.detail)} />
              <input value={entry.tag} placeholder="Tag" aria-label={`Rule tag ${index + 1}`} on:input={event => update(index, 'tag', event.currentTarget.value)} disabled={busy} />
              <input value={entry.query} placeholder="Search query" aria-label={`Rule query ${index + 1}`} title="Same syntax as search: terms, |, -, column:value" on:input={event => update(index, 'query', event.currentTarget.value)} disabled={busy} />
              <div class="ioc-rule-actions">
                {#if matchingPreview}<span class="condition-count" role="status">{matchingPreview.count.toLocaleString()} rows</span>{/if}
                <button type="button" class="ghost" title="Count matching rows without changing annotations" on:click={() => void runRule(entry, false)} disabled={busy || !entry.query.trim()}>Preview</button>
                <button type="button" class="ghost" title="Set this flag and add this tag to the previewed rows" on:click={() => void runRule(entry, true)} disabled={busy || !matchingPreview?.count}>Apply</button>
                <button type="button" class="ghost danger ioc-icon" aria-label={`Delete rule ${index + 1}`} title="Delete rule" on:click={() => remove(index)} disabled={busy}>
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M3 6h18M9 6V3h6v3M5 6l1 15h12l1-15M10 10v7M14 10v7" /></svg>
                </button>
              </div>
            </div>
          {/each}
        {/if}
      </div>
      {#if error}<p class="memo-error" role="alert">{error}</p>{:else if notice || busy}<p class="condition-count" role="status">{busy ? 'Working…' : notice}</p>{/if}
      <div class="ioc-footer">
        <button type="button" class="ghost" on:click={() => void undo()} disabled={busy} title="Restore the last bulk operation in this case">Undo</button>
        <span class="condition-count ioc-save-hint">Save keeps rules. Apply changes annotations.</span>
        <button type="button" class="ghost" on:click={() => void close()} disabled={busy}>Cancel</button>
        <button type="button" class="primary" on:click={() => void saveRules()} disabled={busy}>Save rules</button>
      </div>
    </div>
  </div>
{/if}

<style>
  .ioc-dialog { width: min(980px, calc(100vw - 32px)); max-height: calc(100vh - 32px); }
  .ioc-table { min-height: 0; overflow: auto; }
  .ioc-row, .ioc-header { grid-template-columns: 122px minmax(80px, 130px) minmax(160px, 1fr) auto; gap: 6px; }
  .ioc-row input { min-width: 0; }
  .ioc-rule-actions { display: flex; align-items: center; justify-content: flex-end; gap: 4px; }
  .ioc-rule-actions button { padding: 3px 6px; }
  .ioc-icon { display: inline-flex; align-items: center; justify-content: center; padding: 4px; }
  .ioc-save-hint { flex: 1; text-align: right; }
  .ioc-footer { gap: 7px; }
  @media (max-width: 700px) {
    .ioc-header { display: none; }
    .ioc-row { grid-template-columns: 122px minmax(90px, 1fr); }
    .ioc-row > input[aria-label^="Rule query"] { grid-column: 1 / -1; }
    .ioc-rule-actions { grid-column: 1 / -1; }
    .ioc-save-hint { display: none; }
    .ioc-footer { flex-wrap: wrap; }
  }
</style>
