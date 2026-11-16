<script lang="ts">
  import { createEventDispatcher, onMount } from 'svelte';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import { writeTextFile } from '@tauri-apps/plugin-fs';
  import { theme, themeOptions, setTheme, importThemeFile, themeCatalogWarning } from '../../theme';
  import { exportTheme, parseTheme, themeBase } from '../../themeModel';
  import { wallpaper, wallpaperWarning, importWallpaper, clearWallpaper, setWallpaperSettings } from '../../appearance';
  import { acceptSave } from '../../saveDrain';
  import { errorMessage } from '../../errors';
  import type { WallpaperSettings } from '../../appearanceModel';

  const dispatch = createEventDispatcher<{ close: void }>();
  let dialog: HTMLElement;
  let busy = false;
  let message = '';
  let failed = false;
  const run = async (operation: () => Promise<unknown>) => {
    if (busy) return;
    busy = true; message = ''; failed = false;
    try { await operation(); }
    catch (error) { failed = true; message = errorMessage(error, 'Could not update appearance.'); }
    finally { busy = false; }
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
    message = 'Theme JSON exported. Edit its colors and import it.';
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
    if (event.key === 'Escape') { event.preventDefault(); dispatch('close'); }
    if (event.key !== 'Tab') return;
    const controls = [...dialog.querySelectorAll<HTMLElement>('button:not(:disabled),select:not(:disabled),input:not(:disabled)')];
    const first = controls[0], last = controls.at(-1);
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
  };
  onMount(() => { dialog.querySelector<HTMLElement>('button')?.focus(); });
</script>

<div class="appearance-backdrop" role="presentation">
  <!-- This modal owns the keyboard focus loop and prevents table shortcuts. -->
  <!-- svelte-ignore a11y-no-noninteractive-element-interactions -->
  <section bind:this={dialog} class="appearance-dialog" role="dialog" aria-modal="true" aria-labelledby="appearance-title" tabindex="-1" on:keydown={keydown}>
    <header><h2 id="appearance-title">Appearance</h2>
      <button class="close" aria-label="Close appearance" title="Close" on:click={() => dispatch('close')}>×</button>
    </header>
    <div class="appearance-content">
      <section class="settings-section" aria-labelledby="appearance-theme-title">
        <h3 id="appearance-theme-title">Theme</h3>
        <select aria-label="Appearance theme" value={$theme} disabled={busy} on:change={chooseTheme}>
          {#each $themeOptions as option}<option value={option.value}>{option.label}</option>{/each}
        </select>
        <div class="actions"><button disabled={busy} on:click={importTheme}>Import JSON…</button><button disabled={busy} on:click={exportCurrent}>Export theme…</button></div>
        <p>Export a theme, edit its colors, then import your copy.</p>
      </section>
      <section class="settings-section" aria-labelledby="appearance-background-title">
        <h3 id="appearance-background-title">Background</h3>
        <div class="actions"><button disabled={busy} on:click={chooseWallpaper}>Choose image…</button>
          <button disabled={busy || !$wallpaper.image} on:click={() => run(clearWallpaper)}>Remove</button></div>
        <p>PNG / JPEG / WebP · stored as a local copy</p>
        <label class="check"><input type="checkbox" disabled={busy || !$wallpaper.image} checked={$wallpaper.settings.enabled} on:change={event => changeWallpaper({ enabled: event.currentTarget.checked })} />Enable background</label>
        <div class="setting"><label for="wallpaper-fit">Fit</label><select id="wallpaper-fit" disabled={busy || !$wallpaper.image} value={$wallpaper.settings.fit} on:change={event => changeWallpaper({ fit: event.currentTarget.value === 'contain' ? 'contain' : 'cover' })}><option value="cover">Cover</option><option value="contain">Contain</option></select></div>
        <div class="setting"><label for="wallpaper-strength">Strength</label><output for="wallpaper-strength">{$wallpaper.settings.strength}%</output></div>
        <input id="wallpaper-strength" type="range" min="0" max="100" step="1" disabled={busy || !$wallpaper.image} value={$wallpaper.settings.strength} on:input={event => changeWallpaper({ strength: Number(event.currentTarget.value) })} />
        <label class="check"><input type="checkbox" disabled={busy || !$wallpaper.image} checked={$wallpaper.settings.behindTable} on:change={event => changeWallpaper({ behindTable: event.currentTarget.checked })} />Show behind table</label>
        <p>Fixed columns and controls stay opaque.</p>
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
  .setting { display: flex; align-items: center; justify-content: space-between; font-size: 12px; }
  .check { display: flex; align-items: center; gap: 7px; font-size: 12px; }
  p { margin: 0; font-size: 11px; color: var(--muted); }
  input[type=range] { width: 100%; accent-color: var(--accent); }
  .status { margin-top: 5px; overflow-wrap: anywhere; }
  .error { color: var(--danger); }
  button:disabled, select:disabled, input:disabled { opacity: .5; cursor: default; }
  button:focus-visible, select:focus-visible, input:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 2px; }
</style>
