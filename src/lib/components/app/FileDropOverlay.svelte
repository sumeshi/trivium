<script lang="ts">
  import { DATA_FILE_TYPES } from '../../fileDrop';
  import type { FileDropState } from '../../fileDrop';
  export let state: FileDropState;
</script>

<div class="file-drop-overlay" role="status" aria-live="polite" aria-atomic="true">
  <div class="file-drop-target" class:unavailable={Boolean(state.blocked) || !state.supported}>
    <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8Z" />
      <path d="M14 2v6h6m-8 3v7m-3-3 3 3 3-3" />
    </svg>
    <p class="file-drop-title">{state.blocked ?? (state.supported ? 'Drop files here' : 'Unsupported file type')}</p>
    <p>{DATA_FILE_TYPES}</p>
    {#if !state.blocked && state.supported}
      <p class="file-drop-hint">{state.supported} file{state.supported === 1 ? '' : 's'} · Review import settings next</p>
      {#if state.ignored}<p class="file-drop-hint">{state.ignored} unsupported item{state.ignored === 1 ? '' : 's'} will be skipped</p>{/if}
    {/if}
  </div>
</div>

<style>
  .file-drop-overlay { position: fixed; inset: 0; z-index: 250; display: grid; place-items: center; padding: 20px; background: rgb(0 0 0 / 65%); color: #fff; pointer-events: none; }
  .file-drop-target { display: flex; flex-direction: column; align-items: center; gap: 10px; max-width: 440px; padding: 28px 36px; border: 2px dashed rgb(255 255 255 / 70%); border-radius: 10px; text-align: center; }
  .file-drop-target.unavailable { border-style: solid; }
  p { margin: 0; font-size: 13px; }
  .file-drop-title { font-size: 19px; font-weight: 600; line-height: 1.4; }
  .file-drop-hint { color: rgb(255 255 255 / 80%); font-size: 12px; }
</style>
