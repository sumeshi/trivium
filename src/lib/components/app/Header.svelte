<script lang="ts">
  import { createEventDispatcher, onDestroy } from 'svelte';
  import type { LoadProjectResponse } from '../../types';
  import { theme, setTheme, THEME_OPTIONS } from '../../theme';
  import { parseTheme } from '../../themeModel';
  import { BUILD_LABEL } from '../../buildInfo';

  export let projectDetail: LoadProjectResponse | null;

  const dispatch = createEventDispatcher();
  let menuEl: HTMLDetailsElement;
  let menuOpen = false;
  let copyMessage = '';
  let copyTimer: ReturnType<typeof setTimeout> | null = null;

  $: meta = projectDetail?.project.meta;
  $: fileName = meta?.source_file_name || meta?.name || '';
  $: sourcePath = meta?.source_path || '';
  const shortSha = (sha: string) => `${sha.slice(0, 12)}${sha.length > 12 ? '…' : ''}`;

  const copyText = async (value: string, label: string) => {
    let copied = false;
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(value);
        copied = true;
      }
    } catch {
      // Some webviews expose the API while denying its permission. Try the
      // user-initiated document copy path in that case too.
    }
    if (!copied) {
      const input = document.createElement('textarea');
      const focused = document.activeElement;
      input.value = value;
      input.style.position = 'fixed';
      input.style.opacity = '0';
      document.body.append(input);
      try {
        input.select();
        copied = document.execCommand('copy');
      } catch {
        copied = false;
      } finally {
        input.remove();
        if (focused instanceof HTMLElement && focused.isConnected) focused.focus({ preventScroll: true });
      }
    }
    copyMessage = copied ? `${label} copied` : `Could not copy ${label}`;
    if (copyTimer) clearTimeout(copyTimer);
    copyTimer = setTimeout(() => { copyMessage = ''; }, 2400);
  };
  const copySha = async () => {
    if (meta?.source_sha256) await copyText(meta.source_sha256, 'SHA-256');
  };

  const closeOutside = (event: MouseEvent) => {
    if (menuOpen && !menuEl?.contains(event.target as Node)) menuOpen = false;
  };
  const closeOnEscape = (event: KeyboardEvent) => {
    if (menuOpen && event.key === 'Escape') {
      menuOpen = false;
      menuEl?.querySelector('summary')?.focus();
    }
  };
  const changeTheme = (event: Event) => {
    const next = parseTheme((event.currentTarget as HTMLSelectElement).value);
    if (next) void setTheme(next);
  };

  onDestroy(() => { if (copyTimer) clearTimeout(copyTimer); });
</script>

<svelte:document on:click={closeOutside} on:keydown={closeOnEscape} />

<header class="app-header">
  <div class="header-left">
    <button
      type="button"
      class="icon-button"
      on:click={(event) => { event.stopPropagation(); dispatch('menuClick'); }}
      aria-label="Open project menu"
      title="Projects"
    >
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" aria-hidden="true">
        <path d="M4 7h16M4 12h16M4 17h16" />
      </svg>
    </button>

    <div class="brand-block">
      <div class="brand-line">
        <span class="brand-name">Trivium</span>
        {#if meta}
          <span class="brand-slash" aria-hidden="true">/</span>
          <span class="project-title" title={sourcePath || fileName}>{fileName}</span>
        {/if}
        {#if meta?.source_sha256}
          <button
            type="button"
            class="sha-button"
            on:click={() => void copySha()}
            aria-label="Copy full SHA-256"
            title={`SHA-256: ${meta.source_sha256}\nCopy full hash`}
          >
            <span class="sha-text">{shortSha(meta.source_sha256)}</span>
            <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
              <rect x="8" y="8" width="12" height="13" rx="2" /><path d="M16 8V3H3v13h5" />
            </svg>
          </button>
        {/if}
      </div>
    </div>
  </div>

  <div class="header-actions">
    <span class="copy-status" role="status" aria-live="polite">{copyMessage}</span>
    <details class="workspace-menu" bind:this={menuEl} bind:open={menuOpen}>
      <summary class="icon-button" aria-label="Workspace options" title="Workspace options" aria-expanded={menuOpen}>
        <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
          <circle cx="12" cy="5" r="1.6" /><circle cx="12" cy="12" r="1.6" /><circle cx="12" cy="19" r="1.6" />
        </svg>
      </summary>
      <div class="workspace-options">
        <label class="theme-setting">
          <span>Theme</span>
          <select aria-label="Theme" value={$theme} on:change={changeTheme}>
            {#each THEME_OPTIONS as option}
              <option value={option.value}>{option.label}</option>
            {/each}
          </select>
        </label>
        <button
          type="button"
          class="menu-action"
          disabled={!projectDetail}
          title="Reload the saved project and annotations"
          on:click={() => { menuOpen = false; dispatch('refreshClick'); }}
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <path d="M20 11a8 8 0 1 0-2.3 6.3M20 4v7h-7" />
          </svg>
          Reload saved project
        </button>
        {#if sourcePath}
          <button type="button" class="menu-action" title={sourcePath}
            on:click={() => { menuOpen = false; void copyText(sourcePath, 'Source path'); }}>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
              <rect x="8" y="8" width="12" height="13" rx="2" /><path d="M16 8V3H3v13h5" />
            </svg>
            Copy source path
          </button>
        {/if}
        {#if meta?.description}
          <p class="project-description">{meta.description}</p>
        {/if}
        <p class="build-identity" aria-label="Application version">
          <span>Version</span><span>{BUILD_LABEL}</span>
        </p>
      </div>
    </details>
  </div>
</header>

<style>
  .app-header {
    flex: 0 0 auto;
    height: 38px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 0 12px;
    border-bottom: 1px solid var(--border);
    background: var(--surface);
    position: relative;
    z-index: 40;
  }
  .header-left, .header-actions, .brand-line {
    display: flex;
    align-items: center;
    min-width: 0;
    gap: 8px;
  }
  .header-left { flex: 1; }
  .header-actions { flex-shrink: 0; }
  .brand-block { min-width: 0; flex: 1; }
  .brand-line { flex-wrap: nowrap; white-space: nowrap; }
  .brand-name { flex: 0 0 auto; font-size: 14px; font-weight: 650; }
  .brand-slash { flex: 0 0 auto; color: var(--muted); font-size: 14px; }
  .project-title {
    min-width: 0;
    flex: 0 1 auto;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--muted);
    font-size: 13px;
  }
  .sha-button {
    display: inline-flex;
    flex: 0 1 auto;
    min-width: 64px;
    max-width: 126px;
    align-items: center;
    gap: 5px;
    padding: 0;
    border: 0;
    background: transparent;
    color: var(--muted);
    font: 10.5px var(--font-mono);
    line-height: 14px;
    white-space: nowrap;
  }
  .sha-text { min-width: 0; overflow: hidden; text-overflow: ellipsis; }
  .sha-button svg { flex: 0 0 auto; }
  .sha-button:hover { color: var(--fg); }
  .icon-button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    height: 28px;
    min-width: 28px;
    padding: 0 5px;
    border: 0;
    border-radius: 4px;
    background: transparent;
    color: var(--muted);
    cursor: pointer;
    list-style: none;
  }
  .icon-button::-webkit-details-marker { display: none; }
  .icon-button:hover, .workspace-menu[open] > summary { background: var(--surface-weak); color: var(--fg); }
  button:focus-visible, summary:focus-visible, select:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
  .copy-status { color: var(--muted); font-size: 11px; white-space: nowrap; }
  .workspace-menu { position: relative; }
  .workspace-options {
    position: absolute;
    right: 0;
    top: calc(100% + 5px);
    width: 232px;
    padding: 6px;
    border: 1px solid var(--border-strong);
    border-radius: var(--radius-sm);
    background: var(--surface);
    box-shadow: var(--shadow-soft);
  }
  .theme-setting { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 3px 5px 6px; font-size: 12px; }
  select { max-width: 132px; padding: 3px 6px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface-weak); color: var(--fg); }
  .menu-action { display: flex; align-items: center; gap: 8px; width: 100%; padding: 6px 5px; border: 0; border-radius: 4px; background: transparent; color: var(--fg); text-align: left; font-size: 12px; }
  .menu-action:hover { background: var(--surface-weak); }
  .menu-action:disabled { opacity: .45; cursor: default; }
  .project-description { margin: 5px 5px 2px; padding-top: 6px; border-top: 1px solid var(--border); color: var(--muted); font-size: 11px; overflow-wrap: anywhere; max-height: 96px; overflow-y: auto; }
  .build-identity { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin: 6px 5px 2px; padding-top: 6px; border-top: 1px solid var(--border); color: var(--muted); font-size: 10.5px; }
  @media (max-width: 540px) { .copy-status { position: absolute; right: 44px; bottom: 0; font-size: 10px; } }
</style>
