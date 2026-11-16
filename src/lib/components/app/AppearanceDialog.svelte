<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount, tick } from 'svelte';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import { writeTextFile } from '@tauri-apps/plugin-fs';
  import { beginThemePreview, theme, themeOptions, setTheme, importThemeFile, themeCatalogWarning } from '../../theme';
  import { exportTheme, parseTheme, themeBase } from '../../themeModel';
  import type { ThemeColor, UserTheme } from '../../themeModel';
  import { BASIC_THEME_FIELDS, ADVANCED_THEME_FIELDS, createThemeDraft, validThemeDraft, withPreviewColor, themeContrastWarning } from '../../themeEditor';
  import ThemeColorField from './ThemeColorField.svelte';
  import { wallpaper, wallpaperWarning, importWallpaper, clearWallpaper, setWallpaperSettings } from '../../appearance';
  import { acceptSave } from '../../saveDrain';
  import { errorMessage } from '../../errors';
  import type { WallpaperSettings } from '../../appearanceModel';
  import { createCompositionGuard } from '../../keyboard';

  const dispatch = createEventDispatcher<{ close: void }>();
  let dialog: HTMLElement;
  let busy = false;
  let message = '';
  let failed = false;
  let draft: UserTheme | null = null;
  let initialDraft: UserTheme | null = null;
  let paintedDraft: UserTheme | null = null;
  let preview: ReturnType<typeof beginThemePreview> | null = null;
  const composition = createCompositionGuard();
  $: validDraft = draft ? validThemeDraft(draft) : null;
  $: contrastWarning = paintedDraft ? themeContrastWarning(paintedDraft) : '';
  const cancelEditing = () => {
    preview?.cancel(); preview = null;
    draft = initialDraft = paintedDraft = null;
  };
  const editColors = () => {
    if (busy) return;
    cancelEditing();
    draft = createThemeDraft($theme, `custom-${crypto.randomUUID()}`);
    initialDraft = structuredClone(draft);
    paintedDraft = structuredClone(draft);
    preview = beginThemePreview(); preview.apply(paintedDraft);
    message = ''; failed = false;
    void tick().then(() => {
      if (!draft || busy) return;
      const input = dialog.querySelector<HTMLInputElement>('[aria-label="Theme name"]');
      input?.focus(); input?.select();
    });
  };
  const changeColor = (key: ThemeColor, value: string) => {
    if (!draft || !paintedDraft || busy) return;
    draft = { ...draft, colors: { ...draft.colors, [key]: value } };
    paintedDraft = withPreviewColor(paintedDraft, key, value);
    preview?.apply(paintedDraft);
  };
  const resetColors = () => {
    if (!initialDraft || busy) return;
    draft = { ...structuredClone(initialDraft), name: draft?.name ?? initialDraft.name };
    paintedDraft = structuredClone(initialDraft);
    preview?.apply(paintedDraft);
  };
  const run = async (operation: () => Promise<unknown>) => {
    if (busy) return;
    busy = true; message = ''; failed = false;
    try { await operation(); }
    catch (error) { failed = true; message = errorMessage(error, 'Could not update appearance.'); }
    finally { busy = false; }
  };
  const saveColors = () => {
    const definition = validDraft, editor = preview;
    if (!definition || !editor) return;
    void run(async () => {
      await editor.save(definition);
      preview = null; draft = initialDraft = paintedDraft = null;
      message = 'Theme saved.';
    });
  };
  const chooseTheme = (event: Event) => {
    const next = parseTheme((event.currentTarget as HTMLSelectElement).value);
    if (next) void run(() => setTheme(next));
  };
  const importTheme = () => run(() => acceptSave(async () => {
    const selected = await open({ multiple: false, filters: [{ name: 'Trivium theme JSON', extensions: ['json', 'JSON'] }] });
    if (typeof selected !== 'string') return;
    const imported = await importThemeFile(selected, name => window.confirm(`Replace the existing theme “${name}”?`));
    if (imported) message = 'Theme imported.';
  }));
  const exportCurrent = () => run(() => acceptSave(async () => {
    const definition = exportTheme($theme, `${themeBase($theme)}-copy-${crypto.randomUUID().slice(0, 8)}`);
    const selected = await save({ defaultPath: `trivium-theme-${definition.id}.json`, filters: [{ name: 'Trivium theme JSON', extensions: ['json'] }] });
    if (!selected) return;
    await writeTextFile(selected, JSON.stringify(definition, null, 2));
    message = 'Theme JSON exported.';
  }));
  const chooseWallpaper = () => run(() => acceptSave(async () => {
    const selected = await open({ multiple: false, filters: [{ name: 'Background image', extensions: ['png', 'PNG', 'jpg', 'JPG', 'jpeg', 'JPEG', 'webp', 'WEBP'] }] });
    if (typeof selected === 'string') await importWallpaper(selected);
  }));
  const changeWallpaper = (patch: Partial<WallpaperSettings>) => {
    void setWallpaperSettings({ ...$wallpaper.settings, ...patch }).catch(error => {
      failed = true; message = errorMessage(error, 'Could not save background settings.');
    });
  };
  const keydown = (event: KeyboardEvent) => {
    event.stopPropagation();
    if (composition.blocksShortcut(event)) return;
    if (event.key === 'Escape') { event.preventDefault(); if (!busy) dispatch('close'); }
    if (event.key !== 'Tab') return;
    const controls = [...dialog.querySelectorAll<HTMLElement>('button:not(:disabled),select:not(:disabled),input:not(:disabled)')];
    if (!controls.length) { event.preventDefault(); dialog.focus(); return; }
    const first = controls[0], last = controls.at(-1);
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  };
  onMount(() => { dialog.querySelector<HTMLElement>('button')?.focus(); });
  onDestroy(cancelEditing);
</script>

<div class="appearance-backdrop" role="presentation">
  <!-- This modal owns the keyboard focus loop and prevents table shortcuts. -->
  <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
  <section bind:this={dialog} class="appearance-dialog" role="dialog" aria-modal="true" aria-labelledby="appearance-title" tabindex="-1" on:keydown={keydown} on:compositionstart={composition.start} on:compositionend={composition.end}>
    <header><h2 id="appearance-title">Appearance</h2>
      <button class="close" aria-label="Close appearance" title="Close" disabled={busy} on:click={() => dispatch('close')}>×</button>
    </header>
    <div class="appearance-content">
      <section class="settings-section" aria-labelledby="appearance-theme-title">
        <h3 id="appearance-theme-title">Theme</h3>
        <select aria-label="Appearance theme" value={$theme} disabled={busy || Boolean(draft)} on:change={chooseTheme}>
          {#each $themeOptions as option}<option value={option.value}>{option.label}</option>{/each}
        </select>
        <div class="actions">
          <button disabled={busy || Boolean(draft)} on:click={editColors}>Edit colors</button>
          <button disabled={busy || Boolean(draft)} on:click={importTheme}>Import JSON…</button>
          <button disabled={busy || Boolean(draft)} on:click={exportCurrent}>Export theme…</button>
        </div>
        {#if draft && paintedDraft}
          <div class="theme-editor" aria-label="Theme color editor" aria-busy={busy}>
            <label class="theme-name">Name<input aria-label="Theme name" bind:value={draft.name} aria-invalid={!draft.name.trim()} disabled={busy} maxlength="64" autocomplete="off" /></label>
            <fieldset disabled={busy}>
              <legend class="visually-hidden">Theme colors</legend>
              {#each BASIC_THEME_FIELDS as field}
                <ThemeColorField name={field.key} label={field.label} value={draft.colors[field.key] ?? ''}
                  fallback={paintedDraft.colors[field.key] ?? '#000000'} on:change={event => changeColor(field.key, event.detail)} />
              {/each}
              <details class="advanced-colors"><summary>More colors</summary>
                {#each ADVANCED_THEME_FIELDS as field}
                  <ThemeColorField name={field.key} label={field.label} value={draft.colors[field.key] ?? ''}
                    fallback={paintedDraft.colors[field.key] ?? '#000000'} on:change={event => changeColor(field.key, event.detail)} />
                {/each}
              </details>
            </fieldset>
            {#if contrastWarning}<p class="contrast-warning" role="status">{contrastWarning}</p>{/if}
            <div class="editor-actions">
              <button disabled={busy} title="Restore the colors from when editing started" on:click={resetColors}>Reset colors</button>
              <span></span>
              <button disabled={busy} on:click={cancelEditing}>Cancel editing</button>
              <button class="primary" disabled={busy || !validDraft} on:click={saveColors}>Save theme</button>
            </div>
          </div>
        {/if}
      </section>
      <section class="settings-section" aria-labelledby="appearance-background-title">
        <h3 id="appearance-background-title">Table wallpaper</h3>
        <div class="actions"><button disabled={busy} on:click={chooseWallpaper}>Choose image…</button>
          <button disabled={busy || !$wallpaper.image} on:click={() => run(clearWallpaper)}>Remove</button></div>
        <p>PNG / JPEG / WebP · table body only</p>
        <label class="check"><input type="checkbox" disabled={busy || !$wallpaper.image} checked={$wallpaper.settings.enabled} on:change={event => changeWallpaper({ enabled: event.currentTarget.checked })} />Enable background</label>
        <div class="setting"><label for="wallpaper-strength">Strength</label><output for="wallpaper-strength">{$wallpaper.settings.strength}%</output></div>
        <input id="wallpaper-strength" type="range" min="0" max="100" step="1" disabled={busy || !$wallpaper.image} value={$wallpaper.settings.strength} on:input={event => changeWallpaper({ strength: Number(event.currentTarget.value) })} />
      </section>
      {#if message}<p class:error={failed} class="status" role="status">{message}</p>{/if}
      {#if $themeCatalogWarning || $wallpaperWarning}<p class="status error" role="status">{$themeCatalogWarning || $wallpaperWarning}</p>{/if}
    </div>
  </section>
</div>

<style>
  .appearance-backdrop { position: fixed; inset: 0; z-index: 200; display: grid; place-items: center; background: color-mix(in srgb, var(--bg) 12%, transparent); }
  .appearance-dialog { width: min(460px, 94vw); max-height: min(720px, 92vh); display: flex; flex-direction: column; border: 1px solid var(--border-strong); border-radius: var(--radius); background: var(--surface); color: var(--fg); box-shadow: var(--shadow-strong); }
  header { display: flex; align-items: center; justify-content: space-between; padding: 10px 14px; border-bottom: 1px solid var(--border); }
  h2 { margin: 0; font-size: 14px; font-weight: 600; }
  .close { border: 0; background: transparent; color: var(--muted); font-size: 23px; width: 28px; height: 28px; padding: 0; }
  .appearance-content { overflow-y: auto; min-height: 0; padding: 0 14px 12px; }
  .settings-section { display: grid; gap: 8px; padding: 13px 0; }
  .settings-section + .settings-section { border-top: 1px solid var(--border); }
  h3 { margin: 0; font-size: 12px; font-weight: 600; }
  select, button { border: 1px solid var(--border); border-radius: 4px; background: var(--surface-weak); color: var(--fg); padding: 5px 8px; font-size: 12px; }
  select { min-width: 100px; }
  .actions { display: flex; flex-wrap: wrap; gap: 6px; }
  .theme-editor { display: grid; gap: 8px; padding-top: 8px; border-top: 1px solid var(--border); }
  .theme-name { display: grid; grid-template-columns: 42px minmax(0, 1fr); align-items: center; gap: 7px; font-size: 11px; }
  .theme-name input { width: 100%; min-width: 0; height: 28px; padding: 4px 7px; font: inherit; color: var(--fg); background: var(--surface); border: 1px solid var(--border); border-radius: 4px; }
  .theme-name input[aria-invalid=true] { border-color: var(--danger); }
  .theme-name input:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 1px; }
  fieldset { display: grid; gap: 3px; min-width: 0; margin: 0; padding: 0; border: 0; }
  .advanced-colors summary { margin-top: 4px; padding: 3px 0; color: var(--muted); font-size: 11px; cursor: pointer; }
  .advanced-colors[open] summary { margin-bottom: 3px; }
  .editor-actions { display: flex; flex-wrap: wrap; align-items: center; gap: 5px; }
  .editor-actions span { flex: 1; }
  .editor-actions button { font-size: 11px; white-space: nowrap; padding: 4px 7px; }
  .editor-actions .primary { color: var(--accent-contrast); background: var(--accent); border-color: var(--accent); }
  .contrast-warning { color: var(--warning); }
  .visually-hidden { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0,0,0,0); white-space: nowrap; }
  .setting { display: flex; align-items: center; justify-content: space-between; font-size: 12px; }
  .check { display: flex; align-items: center; gap: 7px; font-size: 12px; }
  p { margin: 0; font-size: 11px; color: var(--muted); }
  input[type=range] { width: 100%; accent-color: var(--accent); }
  .status { margin-top: 5px; overflow-wrap: anywhere; }
  .error { color: var(--danger); }
  button:disabled, select:disabled, input:disabled { opacity: .5; cursor: default; }
  button:focus-visible, select:focus-visible, input:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 2px; }
</style>
