<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { fade, fly } from 'svelte/transition';
  import dayjs from 'dayjs';
  import type { ProjectSummary } from '../../types';

  const dispatch = createEventDispatcher();

  export let projects: ProjectSummary[];
  export let selectedProjectId: string | null;
  export let isLoadingProjects: boolean;
  export let creating: boolean;
  export let pendingDescription: string;
  export let pendingFileName: string;
  export let pendingDelimiter: string;
  export let canCreateProject: boolean;

  $: selectedFormat = pendingFileName.split('.').at(-1)?.toLowerCase() ?? '';
  $: csvSettingsRelevant = !pendingFileName || selectedFormat === 'csv';
  const shortSha = (sha: string) => `${sha.slice(0, 12)}${sha.length > 12 ? '…' : ''}`;

  const truncateText = (text: string, length: number) => {
    return text.length > length ? `${text.slice(0, length)}…` : text;
  };

  const handleDescriptionInput = (event: Event) => {
    const nextValue = (event.target as HTMLInputElement | null)?.value ?? '';
    dispatch('descriptionChange', nextValue);
  };

  const handleDelimiterChange = (event: Event) => {
    const nextValue = (event.target as HTMLSelectElement | null)?.value ?? '';
    dispatch('delimiterChange', nextValue);
  };
</script>

<div
  class="drawer-backdrop"
  transition:fade={{ duration: 120 }}
>
  <button
    type="button"
    class="drawer-scrim"
    aria-label="Close workspace menu"
    on:click={() => dispatch('close')}
  ></button>
  <aside
    class="workspace-drawer"
    role="dialog"
    aria-modal="true"
    transition:fly={{ x: -24, duration: 160 }}
  >
    <header class="drawer-header">
      <div>
        <p class="eyebrow">Workspace</p>
        <h2>Projects</h2>
      </div>
      <button
        type="button"
        class="icon-button"
        on:click={() => dispatch('close')}
        aria-label="Close workspace menu"
      >
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round">
          <path d="M6 6l12 12M18 6 6 18" />
        </svg>
      </button>
    </header>

    <section class="import-panel">
      <div class="import-heading">
        <h3>Open a data file</h3>
        <p id="import-formats">CSV · JSON · JSONL / NDJSON</p>
      </div>
      <button class="button choose-file full" disabled={creating}
        aria-describedby="import-formats" on:click={() => dispatch('pickCsv')}>
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <path d="M3 7h7l2-3h9v16H3V7Z" />
        </svg>
        {pendingFileName ? 'Choose another file' : 'Choose data file'}
      </button>
      {#if pendingFileName}
        <div class="pending-file" title={pendingFileName}>
          <span class="pending-dot"></span>
          <span>{pendingFileName}</span>
        </div>
      {/if}

      <div class="field-grid">
        <label class="field">
          <span>Description</span>
          <input
            type="text"
            placeholder="Optional"
            value={pendingDescription}
            on:input={handleDescriptionInput}
            disabled={creating}
          />
        </label>
        <label class="field">
          <span>CSV delimiter</span>
          <select
            aria-label="CSV delimiter"
            value={pendingDelimiter === ',' ? '' : pendingDelimiter}
            on:change={handleDelimiterChange}
            disabled={creating || !csvSettingsRelevant}
            aria-describedby="csv-import-help"
          >
            <option value="">Comma , (default)</option>
            <option value={'\t'}>Tab</option>
            <option value=";">Semicolon ;</option>
            <option value="|">Pipe |</option>
          </select>
        </label>
      </div>
      <p class="import-help" id="csv-import-help">
        {csvSettingsRelevant
          ? 'CSV uses its first row as column names. Choose the separator used by your file.'
          : 'CSV separator settings do not apply to JSON or JSONL files.'}
      </p>

      <button
        class="button primary full"
        disabled={!canCreateProject || creating}
        on:click={() => dispatch('createProject')}
      >
        {creating ? 'Importing…' : 'Import and open'}
      </button>
      <p class="import-help">Creates a saved investigation you can reopen from Recent. Your source file stays unchanged.</p>
    </section>

    <section class="project-section">
      <div class="section-head">
        <h3>Recent</h3>
        <button
          class="icon-button"
          on:click={() => dispatch('loadProjects')}
          aria-label="Refresh projects"
          title="Refresh projects"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
            <path d="M20 11a8 8 0 1 0-2.3 6.3M20 4v7h-7" />
          </svg>
        </button>
      </div>

      {#if isLoadingProjects}
        <p class="empty-list">Loading projects…</p>
      {:else if projects.length === 0}
        <div class="empty-list">
          <p>No projects yet.</p>
          <p>Import a CSV, JSON, or JSONL file to begin.</p>
        </div>
      {:else}
        <ul class="project-list">
          {#each projects as project}
            <li class="project-row">
              <button
                type="button"
                class="project-card"
                class:selected={project.meta.id === selectedProjectId}
                on:click={() => dispatch('selectProject', project.meta.id)}
              >
                <div class="project-top">
                  <span class="project-name" title={project.meta.source_file_name || project.meta.name}>{project.meta.name}</span>
                  <span class="project-date">
                    {dayjs(project.meta.created_at).format('YYYY-MM-DD HH:mm')}
                  </span>
                </div>
                {#if project.meta.source_sha256}
                  <div class="project-hash" title={`SHA-256: ${project.meta.source_sha256}`}>
                    <span>SHA-256</span><code>{shortSha(project.meta.source_sha256)}</code>
                  </div>
                {/if}
                {#if project.meta.description}
                  <p class="project-desc">{truncateText(project.meta.description, 70)}</p>
                {/if}
                <div class="project-stats">
                  <span>{project.meta.total_records.toLocaleString()} rows</span>
                  <span class="stat-dot">·</span>
                  <span>{project.meta.flagged_records + project.meta.ioc_applied_records} flagged</span>
                </div>
              </button>
              <button
                type="button"
                class="delete-button"
                on:click={(event) => {
                  event.stopPropagation();
                  dispatch('deleteProject', project.meta.id);
                }}
                aria-label={`Delete ${project.meta.name}`}
                title="Delete project"
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M4 7h16M9 7V5h6v2m-8 0 1 13h8l1-13M10 11v5m4-5v5" />
                </svg>
              </button>
            </li>
          {/each}
        </ul>
      {/if}
    </section>
  </aside>
</div>

<style>
  .drawer-backdrop {
    position: fixed;
    inset: 0;
    z-index: 80;
    display: flex;
    background: color-mix(in srgb, var(--bg) 55%, transparent);
    backdrop-filter: blur(3px);
  }

  .drawer-scrim {
    position: absolute;
    inset: 0;
    border: 0;
    background: transparent;
  }

  .workspace-drawer {
    position: relative;
    z-index: 1;
    width: min(400px, 92vw);
    height: 100%;
    display: flex;
    flex-direction: column;
    gap: 20px;
    padding: 22px;
    overflow-y: auto;
    background: var(--surface);
    border-right: 1px solid var(--border);
    box-shadow: var(--shadow-strong);
  }

  .drawer-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
  }

  .eyebrow {
    margin: 0 0 2px;
    color: var(--muted);
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }

  .drawer-header h2 {
    margin: 0;
    color: var(--fg);
    font-size: 20px;
    font-weight: 650;
    letter-spacing: -0.02em;
  }

  .icon-button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    border-radius: 8px;
    border: 1px solid transparent;
    background: transparent;
    color: var(--muted);
  }

  .icon-button:hover {
    background: var(--surface-weak);
    border-color: var(--border);
    color: var(--fg);
  }

  .import-panel {
    display: flex;
    flex-direction: column;
    gap: 10px;
    padding: 14px;
    border: 1px solid var(--border);
    border-radius: var(--radius-lg);
    background: var(--surface-weak);
  }

  .import-heading h3 { margin: 0 0 3px; color: var(--fg); font-size: 12.5px; font-weight: 600; }
  .import-heading p, .import-help { margin: 0; color: var(--muted); font-size: 10.5px; line-height: 1.45; }

  .button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    height: 36px;
    padding: 0 14px;
    border-radius: 8px;
    border: 1px solid transparent;
    font-size: 12.5px;
    font-weight: 600;
  }

  .button.full {
    width: 100%;
  }

  .button.primary {
    background: var(--accent);
    color: var(--accent-contrast);
  }

  .button.primary:hover {
    filter: brightness(1.06);
  }

  .button.choose-file { border-color: var(--border-strong); background: var(--surface); color: var(--fg); }
  .button.choose-file:hover:not(:disabled) { border-color: var(--accent-border); background: var(--accent-soft); }

  .button:disabled {
    opacity: 0.45;
    cursor: not-allowed;
  }

  .pending-file {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    color: var(--muted);
    font-size: 11px;
  }

  .pending-file span:last-child {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .pending-dot {
    width: 6px;
    height: 6px;
    flex: 0 0 6px;
    border-radius: 50%;
    background: var(--positive);
  }

  .field-grid {
    display: grid;
    gap: 8px;
  }

  .field {
    display: flex;
    flex-direction: column;
    gap: 4px;
    color: var(--muted);
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  .field input,
  .field select {
    height: 34px;
    padding: 0 10px;
    border-radius: 7px;
    border: 1px solid var(--border);
    background: var(--surface);
    color: var(--fg);
    font-size: 12.5px;
    font-weight: 400;
    letter-spacing: 0;
    text-transform: none;
    outline: none;
  }

  .field input:focus,
  .field select:focus {
    border-color: var(--accent-border);
    box-shadow: 0 0 0 3px var(--accent-soft);
  }
  .field select:disabled, .field input:disabled { opacity: .6; cursor: not-allowed; }

  .project-section {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }

  .section-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
  }

  .section-head h3 {
    margin: 0;
    color: var(--muted);
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }

  .empty-list {
    margin: 0;
    padding: 18px 2px;
    color: var(--muted);
    font-size: 12px;
  }

  .empty-list p {
    margin: 0 0 4px;
  }

  .project-list {
    list-style: none;
    margin: 0;
    padding: 0;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .project-row {
    display: flex;
    align-items: stretch;
    gap: 4px;
  }

  .project-card {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 10px 11px;
    border: 1px solid var(--border);
    border-radius: 9px;
    background: var(--surface);
    text-align: left;
    transition: background 120ms ease, border-color 120ms ease;
  }

  .project-card:hover {
    border-color: var(--border-strong);
    background: var(--surface-weak);
  }

  .project-card.selected {
    border-color: var(--accent-border);
    background: var(--accent-soft);
  }

  .project-top {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 8px;
    min-width: 0;
  }

  .project-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--fg);
    font-size: 12.5px;
    font-weight: 600;
  }

  .project-date {
    flex: 0 0 auto;
    color: var(--muted);
    font-family: var(--font-mono);
    font-size: 9.5px;
  }

  .project-desc {
    margin: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--muted);
    font-size: 11px;
  }

  .project-hash { display: flex; align-items: center; gap: 6px; color: var(--muted); font-size: 10px; }
  .project-hash code { font: 10.5px var(--font-mono); white-space: nowrap; }

  .project-stats {
    display: flex;
    align-items: center;
    gap: 5px;
    color: var(--muted);
    font-size: 10.5px;
  }

  .stat-dot {
    color: var(--border-strong);
  }

  .delete-button {
    flex: 0 0 30px;
    width: 30px;
    border: 1px solid transparent;
    border-radius: 8px;
    background: transparent;
    color: var(--muted);
  }

  .delete-button:hover {
    border-color: color-mix(in srgb, var(--danger) 35%, transparent);
    background: var(--danger-soft);
    color: var(--danger);
  }
</style>
