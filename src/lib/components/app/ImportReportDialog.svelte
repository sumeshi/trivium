<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { isTabKey } from '../../keyboard';
  import { save } from '@tauri-apps/plugin-dialog';
  import { acceptSave } from '../../saveDrain';
  import { errorMessage } from '../../errors';
  import type { Backend } from '../../backend';
  import type { ImportReport } from '../../types';

  export let report: ImportReport;
  export let fileName: string;
  export let format: string | null | undefined;
  export let backend: Backend | undefined = undefined;
  export let projectId: string | undefined = undefined;
  const dispatch = createEventDispatcher<{ close: void }>();
  let closeButton: HTMLButtonElement;
  let reportBody: HTMLDivElement;
  let exportButton: HTMLButtonElement | undefined;
  let previousFocus: HTMLElement | null = null;
  let exporting = false;
  let error = '';
  let notice = '';
  let destroyed = false;

  onMount(() => {
    previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    closeButton.focus();
  });
  onDestroy(() => { destroyed = true; if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true }); });
  const keyboard = (event: KeyboardEvent) => {
    event.stopPropagation();
    if (event.key === 'Escape') { event.preventDefault(); dispatch('close'); }
    else if (isTabKey(event)) {
      event.preventDefault();
      const stops = [closeButton, reportBody, ...(!exporting && exportButton ? [exportButton] : [])];
      const current = stops.findIndex(node => node === document.activeElement);
      const next = current < 0 ? (event.shiftKey ? stops.length - 1 : 0)
        : (current + (event.shiftKey ? stops.length - 1 : 1)) % stops.length;
      stops[next]?.focus();
    }
  };
  const delimiterName = (value: string) => value === '\t' ? 'Tab' : value === ',' ? 'Comma (,)' : JSON.stringify(value);
  const exportReport = async () => {
    if (!backend || !projectId || exporting) return;
    const service = backend, owner = projectId;
    const isCurrent = () => !destroyed && projectId === owner && backend === service;
    const stem = (fileName.split(/[/\\]/).at(-1) ?? 'data').replace(/\.[^.]*$/, '') || 'data';
    exporting = true; error = ''; notice = '';
    try {
      await acceptSave(async () => {
        const destination = await save({
          defaultPath: `trivium-import-report-${stem}.jsonl`,
          filters: [{ name: 'Trivium import report', extensions: ['jsonl'] }],
        });
        if (!destination || !isCurrent()) return;
        // Native Save confirms replacing an existing destination.
        const result = await service.exportImportReport({ projectId: owner, destination, allowOverwrite: true });
        if (isCurrent()) notice = result.complete ? 'Report exported.' : 'Report exported with retained issue details.';
      });
    } catch (failure) {
      if (isCurrent()) error = errorMessage(failure, 'Failed to export import report.');
    } finally {
      if (isCurrent()) exporting = false;
    }
  };
  $: positionLabel = format === 'json' ? 'Element' : 'Line';
</script>

<div class="import-report-backdrop">
  <!-- The modal owns its keyboard focus and lets the report scroll with arrow keys. -->
  <div class="import-report-dialog" role="dialog" aria-modal="true" aria-labelledby="import-report-title"
    tabindex="-1" on:keydown={keyboard}>
    <header>
      <h3 id="import-report-title">Import report</h3>
      <button type="button" class="dialog-close" bind:this={closeButton} aria-label="Close import report" title="Close import report"
        on:click={() => dispatch('close')}>
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true"><path d="m6 6 12 12M18 6 6 18" /></svg>
      </button>
    </header>
    <!-- A keyboard-focusable scroll region is the modal's second tab stop. -->
    <!-- svelte-ignore a11y-no-noninteractive-tabindex -->
    <div class="report-body" role="region" bind:this={reportBody} tabindex="0" aria-label="Import report details">
      <p class="report-file">{fileName}</p>
      <dl>
        <dt>Encoding</dt><dd>{report.encoding}{report.bom ? ' (BOM)' : ''}</dd>
        {#if report.delimiter !== undefined && report.delimiter !== null}<dt>CSV delimiter</dt><dd>{delimiterName(report.delimiter)}</dd>{/if}
        {#if report.hasHeaders !== null}<dt>CSV header</dt><dd>{report.hasHeaders ? 'First row' : 'Generated column names'}</dd>{/if}
        {#if report.recover !== undefined && report.recover !== null}<dt>Mode</dt><dd>{report.recover ? 'Recovery' : 'Strict'}</dd>{/if}
        {#if report.restoreAnnotations !== undefined && report.restoreAnnotations !== null}<dt>Restore annotations</dt><dd>{report.restoreAnnotations ? 'On' : 'Off'}</dd>{/if}
        <dt>Imported rows</dt><dd>{report.importedRecords.toLocaleString()}</dd>
        <dt>Skipped records</dt><dd>{report.skippedRecords.toLocaleString()}</dd>
        <dt>Repaired records</dt><dd>{report.repairedRecords.toLocaleString()}</dd>
        {#if report.generatedColumns}<dt>Generated column names</dt><dd>{report.generatedColumns.toLocaleString()}</dd>{/if}
      </dl>
      {#if report.unreadableTail}
        <p class="report-warning" role="status">An unreadable remainder was not imported. Its record count is unknown.</p>
      {/if}
      {#if report.issues.length}
        <table aria-label="Import issues">
          <thead><tr><th>{positionLabel}</th><th>Action</th><th>Reason</th></tr></thead>
          <tbody>
            {#each report.issues as issue}
              <tr><td>{issue.sourceRecord?.toLocaleString() ?? '—'}</td>
                <td>{issue.action === 'unreadableTail' ? 'Unreadable remainder' : issue.action === 'skipped' ? 'Skipped' : 'Repaired'}</td>
                <td>{issue.reason}</td></tr>
            {/each}
          </tbody>
        </table>
        {#if report.issueCount > report.issues.length}
          <p class="report-hint">Showing {report.issues.length.toLocaleString()} of {report.issueCount.toLocaleString()} issues.</p>
          {#if report.fullDiagnostics !== true}<p class="report-hint">Only these retained issue details are available for this import.</p>{/if}
        {/if}
      {/if}
    </div>
    {#if backend && projectId}
      <footer>
        <div>
          {#if error}<p class="report-error" role="alert">{error}</p>{/if}
          {#if notice}<p class="report-hint" role="status">{notice}</p>{/if}
        </div>
        <button type="button" class="report-export" bind:this={exportButton} disabled={exporting}
          on:click={exportReport}>{exporting ? 'Exporting…' : 'Export JSONL'}</button>
      </footer>
    {/if}
  </div>
</div>

<style>
  .import-report-backdrop { position: fixed; inset: 0; z-index: 220; display: grid; place-items: center; background: color-mix(in srgb, var(--bg) 65%, transparent); }
  .import-report-dialog { display: grid; grid-template-rows: auto minmax(0, 1fr) auto; width: min(680px, calc(100vw - 32px)); max-height: calc(100vh - 48px); border: 1px solid var(--border-strong); border-radius: 6px; background: var(--surface); color: var(--fg); }
  header { display: flex; align-items: center; justify-content: space-between; padding: 9px 12px; border-bottom: 1px solid var(--border); }
  h3 { margin: 0; font-size: 13px; }
  .dialog-close { display: grid; place-items: center; width: 26px; height: 26px; padding: 0; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); }
  .dialog-close:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 2px; }
  .report-body { overflow: auto; min-height: 0; padding: 12px; font-size: 11px; line-height: 1.5; }
  .report-file { margin: 0 0 10px; overflow-wrap: anywhere; font-weight: 600; }
  dl { display: grid; grid-template-columns: minmax(140px, auto) 1fr; gap: 3px 16px; margin: 0 0 12px; }
  dt { color: var(--muted); } dd { margin: 0; overflow-wrap: anywhere; }
  .report-warning { padding: 7px 9px; border: 1px solid var(--warning); border-radius: 4px; }
  table { width: 100%; border-collapse: collapse; text-align: left; table-layout: fixed; }
  th { color: var(--muted); font-weight: 500; }
  th, td { padding: 6px 7px; border-bottom: 1px solid var(--border); vertical-align: top; overflow-wrap: anywhere; }
  th:first-child { width: 60px; } th:nth-child(2) { width: 100px; }
  .report-hint { color: var(--muted); margin: 8px 0 0; }
  footer { display: flex; justify-content: space-between; align-items: center; gap: 10px; padding: 8px 12px; border-top: 1px solid var(--border); font-size: 11px; }
  footer p { margin: 0; }
  .report-error { color: var(--critical); }
  .report-export { flex-shrink: 0; border: 1px solid var(--border); border-radius: 4px; padding: 5px 9px; font-size: 11px; background: var(--surface); color: var(--fg); }
</style>
