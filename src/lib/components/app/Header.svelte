<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { LoadProjectResponse } from '../../types';
  import { theme, toggleTheme } from '../../theme';

  export let projectDetail: LoadProjectResponse | null;

  const dispatch = createEventDispatcher();

  const shortSha = (sha: string) => (sha.length > 14 ? `${sha.slice(0, 12)}…` : sha);
</script>

<header class="app-header">
  <div class="header-left">
    <button
      class="icon-button"
      on:click={(event) => {
        event.stopPropagation();
        dispatch('menuClick');
      }}
      aria-label="Open project menu"
      title="Projects"
    >
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round">
        <path d="M4 7h16M4 12h16M4 17h16" />
      </svg>
    </button>

    <div class="brand-block">
      <div class="brand-line">
        <span class="brand-mark">T</span>
        <span class="brand-name">Trivium</span>
        {#if projectDetail}
          <span class="brand-slash">/</span>
          <span class="project-title" title={projectDetail.project.meta.name}>
            {projectDetail.project.meta.name}
          </span>
        {/if}
      </div>
      {#if projectDetail && (projectDetail.project.meta.description || projectDetail.project.meta.source_sha256)}
        <div class="meta-line">
          {#if projectDetail.project.meta.description}
            <span class="meta-text">{projectDetail.project.meta.description}</span>
          {/if}
          {#if projectDetail.project.meta.source_sha256}
            <span
              class="meta-mono"
              title={`sha256: ${projectDetail.project.meta.source_sha256}`}
            >
              {shortSha(projectDetail.project.meta.source_sha256)}
            </span>
          {/if}
        </div>
      {/if}
    </div>
  </div>

  <div class="header-actions">
    <button
      class="icon-button"
      on:click={() => void toggleTheme()}
      aria-label="Toggle theme"
      title={$theme === 'dark' ? 'Light theme' : 'Dark theme'}
    >
      {#if $theme === 'dark'}
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round">
          <circle cx="12" cy="12" r="4" />
          <path d="M12 2v2m0 16v2M2 12h2m16 0h2M4.9 4.9l1.4 1.4m11.4 11.4 1.4 1.4m0-14.2-1.4 1.4M6.3 17.7l-1.4 1.4" />
        </svg>
      {:else}
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 12.8A9 9 0 1 1 11.2 3 7 7 0 0 0 21 12.8z" />
        </svg>
      {/if}
    </button>
    <button
      class="icon-button refresh"
      on:click={(event) => {
        event.stopPropagation();
        dispatch('refreshClick');
      }}
      title="Reload project data"
    >
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M20 11a8 8 0 1 0-2.3 6.3M20 4v7h-7" />
      </svg>
      <span class="refresh-label">Refresh</span>
    </button>
  </div>
</header>

<style>
  .app-header {
    flex: 0 0 auto;
    height: 60px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding: 0 20px;
    border-bottom: 1px solid var(--border);
    background: color-mix(in srgb, var(--surface) 86%, transparent);
    backdrop-filter: blur(16px);
    position: relative;
    z-index: 40;
  }

  .header-left,
  .header-actions {
    display: flex;
    align-items: center;
    gap: 12px;
    min-width: 0;
  }

  .brand-block {
    min-width: 0;
  }

  .brand-line {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
  }

  .brand-mark {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
    flex: 0 0 24px;
    border-radius: 7px;
    background: var(--accent);
    color: var(--accent-contrast);
    font-size: 13px;
    font-weight: 700;
    letter-spacing: -0.02em;
  }

  .brand-name {
    color: var(--fg);
    font-size: 14px;
    font-weight: 650;
    letter-spacing: -0.01em;
  }

  .brand-slash {
    color: var(--border-strong);
    font-size: 14px;
  }

  .project-title {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--muted);
    font-size: 13px;
  }

  .meta-line {
    display: flex;
    align-items: center;
    gap: 10px;
    min-width: 0;
    margin-top: 1px;
    padding-left: 32px;
    color: var(--muted);
    font-size: 10.5px;
  }

  .meta-text {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .meta-mono {
    flex: 0 0 auto;
    font-family: var(--font-mono);
    color: color-mix(in srgb, var(--muted) 78%, transparent);
  }

  .icon-button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 7px;
    height: 34px;
    min-width: 34px;
    padding: 0 9px;
    border-radius: 8px;
    border: 1px solid transparent;
    background: transparent;
    color: var(--muted);
    transition: background 120ms ease, color 120ms ease, border-color 120ms ease;
  }

  .icon-button:hover {
    background: var(--surface-weak);
    border-color: var(--border);
    color: var(--fg);
  }

  .icon-button:focus-visible {
    outline: 2px solid var(--accent-border);
    outline-offset: 1px;
  }

  .refresh {
    border-color: var(--border);
    background: var(--surface-weak);
  }

  .refresh-label {
    font-size: 12px;
  }

  @media (max-width: 720px) {
    .app-header {
      height: 54px;
      padding: 0 12px;
    }

    .refresh-label,
    .meta-line {
      display: none;
    }
  }
</style>
