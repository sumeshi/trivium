<script lang="ts">
  import { get } from 'svelte/store';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import { conditionPayload, conditionTree, presetCondition } from '../../stores/condition';
  import { backend, hiddenColumns, projectDetail, sortDirection, sortKey } from './state';

  let error = '';
  let notice = '';
  let warning = '';
  let busy = false;

  const clearMessages = () => {
    error = '';
    notice = '';
    warning = '';
  };

  const savePreset = async () => {
    if (!$backend || !$projectDetail || busy) {
      return;
    }
    clearMessages();
    try {
      const stem = $projectDetail.project.meta.name.replace(/\.[^.]+$/, '') || 'trivium';
      const destination = await save({
        filters: [{ name: 'Trivium preset', extensions: ['json'] }],
        defaultPath: `${stem}-preset.json`,
      });
      if (!destination) {
        return;
      }
      busy = true;
      const fileName = destination.split(/[\\/]/).pop() ?? '';
      await $backend.savePreset({
        projectId: $projectDetail.project.meta.id,
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
      error = err instanceof Error ? err.message : 'Failed to save preset.';
    } finally {
      busy = false;
    }
  };

  const applyPreset = async () => {
    if (!$backend || !$projectDetail || busy) {
      return;
    }
    clearMessages();
    try {
      const selected = await open({
        multiple: false,
        filters: [{ name: 'Trivium preset', extensions: ['json'] }],
      });
      if (!selected) {
        return;
      }
      const source = Array.isArray(selected) ? selected[0] : selected;
      busy = true;
      const preset = await $backend.loadPreset({
        projectId: $projectDetail.project.meta.id,
        source,
      });
      const tree = presetCondition(preset.condition);
      if (tree) {
        conditionTree.set(tree);
      }
      hiddenColumns.set(new Set(preset.hiddenColumns));
      const nextDirection = preset.sortDirection === 'desc' ? 'desc' : 'asc';
      const sortChanged = get(sortKey) !== preset.sortKey || get(sortDirection) !== nextDirection;
      sortKey.set(preset.sortKey);
      sortDirection.set(nextDirection);
      await $backend.setHiddenColumns({
        projectId: $projectDetail.project.meta.id,
        hiddenColumns: preset.hiddenColumns,
      });
      if (sortChanged) {
        document.dispatchEvent(new CustomEvent('sortChanged'));
      }
      if (preset.unknown_columns.length > 0) {
        warning = `Unknown columns: ${preset.unknown_columns.join(', ')}`;
      } else {
        notice = 'Preset applied.';
      }
    } catch (err) {
      console.error(err);
      error = err instanceof Error ? err.message : 'Failed to apply preset.';
    } finally {
      busy = false;
    }
  };
</script>

<div class="stats-panel">
  <div class="condition-toolbar">
    <span class="filter-label">Preset</span>
    <button type="button" class="ghost" on:click={savePreset} disabled={busy}>
      Save preset
    </button>
    <button type="button" class="ghost" on:click={applyPreset} disabled={busy}>
      Apply preset
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
