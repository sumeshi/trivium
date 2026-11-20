<script lang="ts">
  import { errorMessage } from "../../errors";
  import { get } from 'svelte/store';
  import { open, save } from '@tauri-apps/plugin-dialog';
  import { createEventDispatcher, onDestroy } from 'svelte';
  import { applyPresetAndSaveColumns, changedPresetLayout } from '../../presetView';
  import { presetFileName } from '../../presetFileName';
  import { acceptSave } from '../../saveDrain';
  import { useWorkspace } from "../../workspaceContext";
  const workspace = useWorkspace();
  const {
    conditionPayload, backend, hiddenColumns, projectDetail, sortDirection, sortKey, columnOrder,
    search, searchColumns, flagFilter, bookmarkedOnly,
  } = workspace;

  export let columnWidths: ReadonlyMap<string, number> = new Map();
  export let defaultColumnWidths: ReadonlyMap<string, number> = new Map();
  export let ready = true;
  const dispatch = createEventDispatcher<{ columnWidths: Record<string, number> }>();

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
    if (!ready || !$backend || !$projectDetail || busy) {
      return;
    }
    const service = $backend, projectId = $projectDetail.project.meta.id;
    const sourceName = $projectDetail.project.meta.source_file_name ?? $projectDetail.project.meta.name;
    const layout = changedPresetLayout($projectDetail.columns, get(columnOrder), columnWidths, defaultColumnWidths);
    const snapshot = structuredClone({
      condition: $conditionPayload,
      hiddenColumns: Array.from(get(hiddenColumns)),
      sortKey: get(sortKey),
      sortDirection: get(sortDirection),
      columnOrder: layout.columnOrder ?? [],
      columnWidths: layout.columnWidths ?? {},
      filters: { search: get(search), searchColumns: [...get(searchColumns)],
        flagFilter: get(flagFilter), bookmarkedOnly: get(bookmarkedOnly) },
    });
    const isCurrent = () => ready && !destroyed && get(workspace.currentProjectId) === projectId;
    clearMessages();
    busy = true;
    try {
      await acceptSave(async () => {
        const destination = await save({
          filters: [{ name: 'Trivium preset', extensions: ['json'] }],
          defaultPath: presetFileName(sourceName),
        });
        if (!destination || !isCurrent()) {
          return;
        }
        const fileName = destination.split(/[\\/]/).pop() ?? '';
        // The native Save dialog confirms replacing an existing destination.
        await service.savePreset({
          projectId,
          name: fileName.replace(/\.json$/i, '') || null,
          ...snapshot,
          destination,
          allowOverwrite: true,
        });
        if (isCurrent()) notice = 'Preset saved.';
      });
    } catch (err) {
      console.error(err);
      if (isCurrent()) error = errorMessage(err, 'Failed to save preset.');
    } finally {
      busy = false;
    }
  };

  const applyPreset = async () => {
    if (!ready || !$backend || !$projectDetail || busy) {
      return;
    }
    const service = $backend, projectId = $projectDetail.project.meta.id;
    const isCurrent = () => ready && !destroyed && get(workspace.currentProjectId) === projectId;
    clearMessages();
    busy = true;
    let applied = false;
    try {
      await acceptSave(async () => {
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
        applied = true;
        await applyPresetAndSaveColumns(workspace, preset, {
          setColumnWidths: widths => dispatch('columnWidths', widths),
          saveHiddenColumns: hiddenColumns => service.setHiddenColumns({ projectId, hiddenColumns }),
        });
        if (isCurrent()) notice = 'Preset applied.';
      });
    } catch (err) {
      console.error(err);
      if (isCurrent()) error = applied
        ? `Preset applied. ${errorMessage(err, 'Could not save visible columns.')}`
        : errorMessage(err, 'Failed to apply preset.');
    } finally {
      busy = false;
    }
  };
</script>

<div class="stats-panel">
  <p class="condition-count preset-description">Saves search, flag / bookmark filters, rules, hidden columns, sort and column layout.</p>
  <div class="condition-toolbar">
    <span class="filter-label">Preset</span>
    <button type="button" class="ghost" on:click={savePreset} disabled={busy || !ready}>
      Save preset file
    </button>
    <button type="button" class="ghost" on:click={applyPreset} disabled={busy || !ready}>
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
