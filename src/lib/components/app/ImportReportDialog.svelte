<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import { isTabKey } from '../../keyboard';
  import type { ImportReport } from '../../types';

  export let report: ImportReport;
  export let fileName: string;
  export let format: string | null | undefined;
  const dispatch = createEventDispatcher<{ close: void }>();
  let closeButton: HTMLButtonElement;
  let reportBody: HTMLDivElement;
  let previousFocus: HTMLElement | null = null;

  onMount(() => {
    previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    closeButton.focus();
  });
  onDestroy(() => { if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true }); });
  const keyboard = (event: KeyboardEvent) => {
    event.stopPropagation();
    if (event.key === 'Escape') { event.preventDefault(); dispatch('close'); }
    else if (isTabKey(event)) {
      event.preventDefault();
      if (document.activeElement === closeButton) reportBody.focus();
      else closeButton.focus();
    }
  };
  $: positionLabel = format === 'json' ? 'Element' : 'Line';
</script>

<div class="import-report-backdrop">
  <!-- The modal owns its keyboard focus and lets the report scroll with arrow keys. -->
  <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
  <section class="import-report-dialog" role="dialog" aria-modal="true" aria-labelledby="import-report-title"
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
        {#if report.hasHeaders !== null}<dt>CSV header</dt><dd>{report.hasHeaders ? 'First row' : 'Generated column names'}</dd>{/if}
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
        {/if}
      {/if}
    </div>
  </section>
</div>

<style>
  .import-report-backdrop { position: fixed; inset: 0; z-index: 220; display: grid; place-items: center; background: color-mix(in srgb, var(--bg) 65%, transparent); }
  .import-report-dialog { display: grid; grid-template-rows: auto minmax(0, 1fr); width: min(680px, calc(100vw - 32px)); max-height: calc(100vh - 48px); border: 1px solid var(--border-strong); border-radius: 6px; background: var(--surface); color: var(--fg); }
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
</style>
