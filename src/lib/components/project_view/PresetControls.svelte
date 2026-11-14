<script lang="ts">
  import { errorMessage } from "../../errors";
  import { get } from 'svelte/store';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import { onDestroy } from 'svelte';
  import { applyPresetView } from '../../presetView';
  import { presetFileName } from '../../presetFileName';
  import { useWorkspace } from "../../workspaceContext";
  const workspace = useWorkspace();
  const {
    conditionPayload, backend, hiddenColumns, projectDetail, sortDirection, sortKey,
  } = workspace;

  let error = '';
  let notice = '';
  let warning = '';
  let busy = false;
  let destroyed = false;
  onDestroy(() => { destroyed = true; });

  const clearMessages = () => {
    error = '';
    notice = '';
    warning = '';
  };

  const savePreset = async () => {
    if (!$backend || !$projectDetail || busy) {
      return;
    }
    const service = $backend, projectId = $projectDetail.project.meta.id;
    clearMessages();
    busy = true;
    try {
      const destination = await save({
        filters: [{ name: 'Trivium preset', extensions: ['json'] }],
        defaultPath: presetFileName($projectDetail.project.meta.source_file_name ?? $projectDetail.project.meta.name),
      });
      if (!destination || destroyed) {
        return;
      }
      const fileName = destination.split(/[\\/]/).pop() ?? '';
      await service.savePreset({
        projectId,
        name: fileName.replace(/\.json$/i, '') || null,
        condition: $conditionPayload,
        hiddenColumns: Array.from(get(hiddenColumns)),
        sortKey: get(sortKey),
        sortDirection: get(sortDirection),
        destination,
      });
      notice = 'Preset saved.';
    } catch (err) {
      console.error(err);
      error = errorMessage(err, 'Failed to save preset.');
    } finally {
      busy = false;
    }
  };

  const applyPreset = async () => {
    if (!$backend || !$projectDetail || busy) {
      return;
    }
    const service = $backend, projectId = $projectDetail.project.meta.id;
    const isCurrent = () => !destroyed && get(workspace.currentProjectId) === projectId;
    clearMessages();
    busy = true;
    try {
      const selected = await open({
        multiple: false,
        filters: [{ name: 'Trivium preset', extensions: ['json'] }],
      });
      if (!selected || !isCurrent()) {
        return;
      }
      const source = Array.isArray(selected) ? selected[0] : selected;
      const preset = await service.loadPreset({
        projectId,
        source,
      });
      if (!isCurrent()) return;
      if (preset.unknown_columns.length > 0) {
        warning = `Unknown columns: ${preset.unknown_columns.join(', ')}`;
        return;
      }
      await service.setHiddenColumns({ projectId, hiddenColumns: preset.hiddenColumns });
      if (!isCurrent()) return;
      applyPresetView(workspace, preset);
      notice = 'Preset applied.';
    } catch (err) {
      console.error(err);
      error = errorMessage(err, 'Failed to apply preset.');
    } finally {
      busy = false;
    }
  };
</script>

<div class="stats-panel">
  <p class="condition-count preset-description">Rules / hidden columns / sort</p>
  <div class="condition-toolbar">
    <span class="filter-label">Preset</span>
    <button type="button" class="ghost" on:click={savePreset} disabled={busy}>
      Save preset file
    </button>
    <button type="button" class="ghost" on:click={applyPreset} disabled={busy}>
      Load preset file
    </button>
  </div>
  {#if error}
    <p class="condition-count">{error}</p>
  {:else if warning}
    <p class="condition-count">{warning}</p>
  {:else if notice}
    <p class="condition-count">{notice}</p>
  {/if}
</div>
