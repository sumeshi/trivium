<script lang="ts">
  import { onDestroy, onMount, tick } from 'svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { listen } from '@tauri-apps/api/event';
  import { getCurrentWebview } from '@tauri-apps/api/webview';
  import type { UnlistenFn } from '@tauri-apps/api/event';
  import { open } from '@tauri-apps/plugin-dialog';
  import { createBackend, listenToJobEvents } from './lib/backend';
  import type { Backend } from './lib/backend';
  import Sidebar from './lib/components/app/Sidebar.svelte';
  import Header from './lib/components/app/Header.svelte';
  import ProjectView from './lib/components/ProjectView.svelte';
  import ProjectTabs from './lib/components/app/ProjectTabs.svelte';
  import Toast from './lib/components/app/Toast.svelte';
  import ExtractionProgress from './lib/components/app/ExtractionProgress.svelte';
  import CloseDialog from './lib/components/app/CloseDialog.svelte';
  import DeleteProjectDialog from './lib/components/app/DeleteProjectDialog.svelte';
  import FileDropOverlay from './lib/components/app/FileDropOverlay.svelte';
  import { initWallpaper } from './lib/appearance';
  import { createExtractScan } from './lib/extractScan';
  import type { ExtractIntent } from './lib/extractTypes';
  import { createFileDrop } from './lib/fileDrop';
  import { createAppClose, installAppCloseListener } from './lib/appClose';
  import { acceptSave, flushAllSaves, reopenSaveAdmission, sealSaveAdmission } from './lib/saveDrain';
  import { showToast } from './lib/utils/toast';
  import { initTheme } from './lib/theme';
  import { initTableTextSize } from './lib/tableTextSize';
  import { createProjectController } from './lib/stores/projects';
  import { errorMessage } from './lib/errors';
  import { copyWorkspaceSession } from './lib/workspaceSession';
  import type { WorkspaceSession } from './lib/workspaceSession';

  // The window's native close guard is installed before any editable UI opens.
  sealSaveAdmission();
  const backend: Backend = createBackend();
  const projectController = createProjectController(backend);
  const projectState = projectController.state;
  const canCreateProject = projectController.canCreateProject;
  const extractions = createExtractScan(backend, message => showToast(message, 'error'));
  const extractState = extractions.state;
  let stopExtractJobs: UnlistenFn | undefined;

  onDestroy(() => {
    appDestroyed = true;
    stopCloseListener?.();
    stopFileDropListener?.();
    stopExtractJobs?.();
    extractions.dispose();
    fileDrop.dispose();
    close.dispose();
    projectController.dispose();
    sessions.clear();
  });

  let sidebarOpen = true;
  let deleteCandidate: { id: string; name: string } | null = null;
  let activeView: ProjectView | null = null;
  let closeReady = false;
  let closeListenerError = '';
  let stopCloseListener: UnlistenFn | undefined;
  let stopFileDropListener: UnlistenFn | undefined;
  let startingCloseListener = false;
  let tabCloseBusy = false;
  let appDestroyed = false;
  let initializedApp = false;
  const close = createAppClose({
    unsavedDraft: () => activeView?.unsavedDraftMessage() ?? null,
    notifyDraft: (message) => {
      showToast(message, 'error');
      void tick().then(() => activeView?.focusUnsavedDraft());
    },
    freeze: () => { extractions.cancel(); activeView?.pauseForClose(); },
    drain: flushAllSaves,
    flushActive: async () => {
      await tick();
      await activeView?.flushForClose();
      await projectController.cleanupTemporaryProjects(tick);
    },
    seal: sealSaveAdmission,
    finish: () => invoke<void>('finish_app_close'),
    cancelNative: (acknowledgeFailures) => invoke<void>('cancel_app_close', { acknowledgeFailures }),
    resume: reopenSaveAdmission,
  });
  const closeState = close.state;
  $: closing = $closeState.phase !== 'idle';
  const editsBlocked = () => !closeReady || close.isBlocked() || tabCloseBusy;
  const fileDrop = createFileDrop({
    blocked: () => !closeReady ? 'Wait for the app to be ready.'
      : close.isBlocked() ? 'Finish the close request before adding files.'
      : tabCloseBusy ? 'Wait for tabs to close before adding files.'
      : $projectState.creating ? 'An import is already running.'
      : document.querySelector('.cell-dialog-backdrop,.appearance-backdrop,.delete-project-backdrop') ? 'Close the dialog before adding files.' : null,
    select: (paths) => {
      projectController.setPendingFiles(paths);
      sidebarOpen = true;
      void tick().then(() => document.querySelector<HTMLElement>('.workspace-drawer [aria-label="CSV delimiter"], .workspace-drawer .button.primary')?.focus());
    },
    notify: message => showToast(message, 'error'),
  });
  const fileDropState = fileDrop.state;
  $: if (closing) fileDrop.clear();
  $: if (closing) deleteCandidate = null;
  const sessions = new Map<string, WorkspaceSession>();
  const pendingDeletions = new Set<string>();
  $: openProjects = $projectState.openProjectIds.flatMap((id) => {
    const project = $projectState.projects.find((item) => item.meta.id === id);
    return project ? [project] : [];
  });
  $: for (const id of sessions.keys()) {
    if (!$projectState.openProjectIds.includes(id) && !pendingDeletions.has(id)) sessions.delete(id);
  }

  const retainSession = (session: WorkspaceSession) => {
    if ($projectState.openProjectIds.includes(session.projectId) || pendingDeletions.has(session.projectId)) {
      sessions.set(session.projectId, copyWorkspaceSession(session));
    }
  };

  const captureActive = () => {
    const session = activeView?.snapshotSession();
    if (session) retainSession(session);
  };

  const handleNotify = (event: CustomEvent<{ message: string; tone: 'success' | 'error' }>) => {
    showToast(event.detail.message, event.detail.tone);
  };

  const canLeaveProject = () => {
    const message = activeView?.unsavedDraftMessage();
    if (!message && activeView?.canAutoNavigate() !== false) return true;
    showToast(message ?? 'Wait for the current edit to finish.', 'error');
    activeView?.focusUnsavedDraft();
    return false;
  };

  const pickCsv = async () => {
    if (editsBlocked()) return;
    try {
      const selected = await open({
        multiple: true,
        // Linux file-dialog filters are case-sensitive, so list the uppercase
        // spellings as well. The backend lowercases the extension again.
        filters: [
          {
            name: 'Data files (CSV / JSON / JSONL / NDJSON)',
            extensions: ['csv', 'CSV', 'json', 'JSON', 'jsonl', 'JSONL', 'ndjson', 'NDJSON']
          }
        ]
      });
      if (!selected || editsBlocked()) {
        return;
      }
      const paths = Array.isArray(selected) ? selected : [selected];
      if (paths.length) projectController.setPendingFiles(paths);
    } catch (error) {
      console.error(error);
      showToast('Failed to open file picker.', 'error');
    }
  };

  const createProject = async () => {
    if (editsBlocked()) return;
    try {
      await acceptSave(async () => {
        captureActive();
        const result = await projectController.createProjects(() => activeView?.canAutoNavigate() ?? true);
        if (result.failures.length) {
          const failure = result.failures[0];
          showToast(`${failure.fileName}: ${failure.message}`, 'error');
        } else if (result.openError) showToast(result.openError, 'error');
        else {
          showToast(result.imported.length === 1
            ? `Imported ${result.imported[0].meta.name}` : `Imported ${result.imported.length} files`);
          sidebarOpen = false;
        }
        await tick();
      });
    } catch (error) {
      console.error(error);
      // Tauri commands return Result<_, String>, so the line/record reason is
      // usually a plain string rather than an Error instance.
      showToast(errorMessage(error, 'Failed to import file.'), 'error');
    }
  };

  const requestDeleteProject = (projectId: string) => {
    if (editsBlocked() || deleteCandidate || $projectState.deletingProjectIds.includes(projectId)) return;
    const project = $projectState.projects.find(project => project.meta.id === projectId);
    if (project) deleteCandidate = { id: projectId, name: project.meta.source_file_name || project.meta.name };
  };

  const confirmDeleteProject = () => {
    const candidate = deleteCandidate;
    deleteCandidate = null;
    if (candidate) void deleteProject(candidate.id);
  };

  const deleteProject = async (projectId: string) => {
    if (editsBlocked()) return;
    if ($extractState.pending?.request.projectId === projectId) extractions.cancel();
    try {
      await acceptSave(async () => {
        captureActive();
        pendingDeletions.add(projectId);
        try {
          const result = await projectController.deleteProject(projectId, tick);
          sessions.delete(projectId);
          showToast(result?.adjacentLoadFailed ? 'Project deleted. Could not open the next tab.' : 'Project deleted.',
            result?.adjacentLoadFailed ? 'error' : 'success');
          await tick();
        } finally {
          pendingDeletions.delete(projectId);
        }
      });
    } catch (error) {
      console.error(error);
      showToast('Failed to delete project.', 'error');
    }
  };

  const readProject = async (read: () => Promise<void>, fallback: string) => {
    try { await read(); return true; }
    catch (error) {
      // Navigation still drains before close; a recoverable read is not a failed save.
      console.error(error);
      showToast(errorMessage(error, fallback), 'error');
      return false;
    }
  };

  const handleRefreshRequest = async () => {
    if (editsBlocked() || !canLeaveProject()) return;
    try {
      await acceptSave(async () => {
        captureActive();
        await readProject(() => projectController.refreshSelected(), 'Failed to refresh project data.');
        await tick();
      });
    } catch (error) {
      console.error(error);
      showToast('Failed to refresh project data.', 'error');
    }
  };

  const handleSelectProject = async (projectId: string) => {
    if (editsBlocked() || (projectId !== $projectState.selectedProjectId && !canLeaveProject())) return;
    try {
      await acceptSave(async () => {
        captureActive();
        if (await readProject(() => projectController.selectProject(projectId), 'Failed to load project data.')) {
          sidebarOpen = false;
        }
        await tick();
      });
    } catch (error) {
      console.error(error);
      showToast('Failed to load project data.', 'error');
    }
  };

  const closeTab = async (projectId: string, selectAdjacent = true) => {
    if ($projectState.temporaryProjectIds.includes(projectId)) {
      pendingDeletions.add(projectId);
      try { await projectController.deleteProject(projectId, tick, selectAdjacent); }
      finally { pendingDeletions.delete(projectId); }
    } else await readProject(() => projectController.closeProject(projectId, selectAdjacent), 'Failed to load the next project.');
    sessions.delete(projectId);
    await tick();
  };

  const handleCloseTab = async (projectId: string) => {
    if (editsBlocked() || (projectId === $projectState.selectedProjectId && !canLeaveProject())) return;
    try {
      await acceptSave(async () => { captureActive(); await closeTab(projectId); });
    } catch (error) {
      showToast(errorMessage(error, 'Failed to load the next project.'), 'error');
    }
  };

  const handleCloseTabs = async (keepProjectId: string | null, rightOnly = false) => {
    if (editsBlocked() || keepProjectId && !$projectState.openProjectIds.includes(keepProjectId)) return;
    const targets = rightOnly
      ? keepProjectId ? $projectState.openProjectIds.slice($projectState.openProjectIds.indexOf(keepProjectId) + 1) : []
      : $projectState.openProjectIds.filter(id => id !== keepProjectId);
    if (!targets.length) return;
    if (targets.includes($projectState.selectedProjectId ?? '') && !canLeaveProject()) return;
    if ($projectState.creating) { showToast('Wait for the import to finish before closing multiple tabs.', 'error'); return; }
    tabCloseBusy = true;
    if (!keepProjectId || targets.includes($extractState.pending?.request.projectId ?? '')) extractions.cancel();
    try {
      await acceptSave(async () => {
        captureActive();
        if (keepProjectId && targets.includes($projectState.selectedProjectId ?? '')
          && !await readProject(() => projectController.selectProject(keepProjectId), 'Failed to open the kept tab.')) return;
        // Retire the active view first; never load a neighbor that is also closing.
        const active = $projectState.selectedProjectId;
        targets.sort((a, b) => Number(b === active) - Number(a === active));
        for (const projectId of targets) {
          if (!$projectState.openProjectIds.includes(projectId)) continue;
          try { await closeTab(projectId, false); }
          catch (error) {
            // A failed temporary-CSV deletion restores its tab. Keep it reachable.
            if (!$projectState.selectedProjectId && $projectState.openProjectIds.includes(projectId)) {
              await readProject(() => projectController.selectProject(projectId), 'Could not reopen the tab.');
            }
            throw error;
          }
        }
      });
    } catch (error) { showToast(errorMessage(error, 'Could not close the tabs.'), 'error'); }
    finally { tabCloseBusy = false; }
  };

  const handleSummaryUpdate = (event: CustomEvent<{ projectId: string; flagged: number; iocApplied: number; hiddenColumns: string[] }>) => {
    projectController.updateSummary(event.detail);
  };

  const startExtraction = async (intent: ExtractIntent) => {
    if (editsBlocked()) return;
    try {
      await acceptSave(async () => {
        const result = await extractions.start(intent);
        if (!result) return;
        if (!closeReady || close.isBlocked()) { await backend.deleteProject(result.meta.id); return; }
        await projectController.adoptTemporaryProject(result, false);
        showToast(`Extracted CSV ready: ${result.meta.source_file_name || result.meta.name}`);
      });
    } catch (error) { showToast(errorMessage(error, 'Could not start extraction.'), 'error'); }
  };

  const startCloseListener = async () => {
    if (startingCloseListener || appDestroyed) return;
    startingCloseListener = true;
    closeListenerError = '';
    try {
      stopCloseListener = await installAppCloseListener({
        listen: (request) => listen('trivium:close-requested', request),
        pending: () => invoke<boolean>('app_close_requested'),
        current: () => !appDestroyed,
        request: () => { void close.request(); },
        enable: () => {
          closeReady = true;
          reopenSaveAdmission();
          if (!initializedApp) {
            initializedApp = true;
            void initTableTextSize();
            void initTheme();
            void initWallpaper();
            void projectController.loadProjects().catch((error) => {
              console.error(error); showToast('Failed to load projects.', 'error');
            });
          }
        },
      });
    } catch (error) {
      stopCloseListener?.(); stopCloseListener = undefined;
      closeListenerError = errorMessage(error, 'Could not initialize safe closing.');
    } finally {
      startingCloseListener = false;
    }
  };

  onMount(() => {
    void startCloseListener();
    void listenToJobEvents({ onStarted: extractions.started, onProgress: extractions.progress, onFinished: extractions.finished })
      .then(stop => { if (appDestroyed) stop(); else stopExtractJobs = stop; })
      .catch(() => { if (!appDestroyed) showToast('Could not track extraction progress.', 'error'); });
    void getCurrentWebview().onDragDropEvent(event => fileDrop.handle(event.payload))
      .then(stop => { if (appDestroyed) stop(); else stopFileDropListener = stop; })
      .catch(() => { if (!appDestroyed) showToast('Could not enable file drops. Use Choose data files.', 'error'); });
  });
</script>

<svelte:window on:blur={() => fileDrop.clear()} on:keydown={(event) => { if (event.key === 'Escape') fileDrop.clear(); }} />

<div class="app-shell" inert={!closeReady || $closeState.phase !== 'idle' || Boolean(deleteCandidate)}>
  <Header {backend} projectDetail={$projectState.projectDetail} on:menuClick={() => { if (!editsBlocked()) sidebarOpen = !sidebarOpen; }} on:refreshClick={handleRefreshRequest} />
  <ProjectTabs projects={openProjects} selectedProjectId={$projectState.selectedProjectId}
    temporaryProjectIds={$projectState.temporaryProjectIds} busy={tabCloseBusy}
    on:select={(event) => handleSelectProject(event.detail)} on:close={(event) => handleCloseTab(event.detail)}
    on:closeOthers={(event) => void handleCloseTabs(event.detail)}
    on:closeRight={(event) => void handleCloseTabs(event.detail, true)} on:closeAll={() => void handleCloseTabs(null)} />

  {#if sidebarOpen}
    <Sidebar
      {backend}
      projects={$projectState.projects.filter(project => !$projectState.temporaryProjectIds.includes(project.meta.id))}
      selectedProjectId={$projectState.selectedProjectId}
      isLoadingProjects={$projectState.isLoadingProjects}
      deletingProjectIds={$projectState.deletingProjectIds}
      projectsError={$projectState.projectsError}
      creating={$projectState.creating}
      pendingDescription={$projectState.pendingDescription}
      pendingFilePaths={$projectState.pendingFilePaths}
      pendingDelimiter={$projectState.pendingDelimiter}
      pendingRestoreAnnotations={$projectState.pendingRestoreAnnotations}
      pendingImportOptions={$projectState.pendingImportOptions}
      importProgress={$projectState.importProgress}
      importFailures={$projectState.importFailures}
      canCreateProject={$canCreateProject}
      on:descriptionChange={(e) => projectController.setPendingDescription(e.detail)}
      on:delimiterChange={(e) => projectController.setPendingDelimiter(e.detail)}
      on:restoreAnnotationsChange={(e) => projectController.setPendingRestoreAnnotations(e.detail)}
      on:importOptionsChange={(e) => projectController.setPendingImportOptions(e.detail)}
      on:selectProject={(e) => handleSelectProject(e.detail)}
      on:deleteProject={(e) => requestDeleteProject(e.detail)}
      on:pickCsv={pickCsv}
      on:createProject={createProject}
      on:loadProjects={() =>
        projectController.loadProjects(true).catch((error) => {
          console.error(error);
          showToast('Failed to load projects.', 'error');
        })
      }
      on:close={() => (sidebarOpen = false)}
    />
  {/if}

  <main class="app-main">
    {#if $projectState.selectedProjectId}
      {#if $projectState.isLoadingDetail}
        <div class="flex flex-1 items-center justify-center text-muted">
          Loading project…
        </div>
      {:else if $projectState.detailError}
        <div class="project-load-error" role="alert">
          <h2>Could not open project</h2>
          <p>{$projectState.detailError}</p>
          <button type="button" aria-label="Retry loading project" on:click={() => {
            const projectId = $projectState.selectedProjectId;
            if (projectId) void handleSelectProject(projectId);
          }}>Retry</button>
        </div>
      {:else if $projectState.projectDetail}
        {#key $projectState.selectedProjectId}
        <div class="flex-1 min-h-0" id={`project-panel-${$projectState.selectedProjectId}`} role="tabpanel" aria-labelledby={`project-tab-${$projectState.selectedProjectId}`}>
          <ProjectView
            bind:this={activeView}
            {backend}
            closing={closing || !closeReady}
            temporary={$projectState.temporaryProjectIds.includes($projectState.selectedProjectId)}
            projectDetail={$projectState.projectDetail}
            session={sessions.get($projectState.selectedProjectId) ?? null}
            extractPending={$extractState.pending}
            extractError={$extractState.error?.projectId === $projectState.selectedProjectId ? $extractState.error.message : null}
            onSessionSnapshot={retainSession}
            on:refresh={handleRefreshRequest}
            on:summary={handleSummaryUpdate}
            on:iocsSaved={event => projectController.updateIocs(event.detail.projectId, event.detail.entries)}
            on:notify={handleNotify}
            on:extract={(event) => void startExtraction(event.detail)}
          />
        </div>
        {/key}
      {/if}
    {:else}
      <div class="empty-workspace">
        <h2>Select a project</h2>
        <p>Open the project menu or import a CSV, JSON, or JSONL file to begin.</p>
      </div>
    {/if}
  </main>
</div>

{#if deleteCandidate}
  <DeleteProjectDialog name={deleteCandidate.name} on:cancel={() => deleteCandidate = null} on:confirm={confirmDeleteProject} />
{/if}

{#if $fileDropState}<FileDropOverlay state={$fileDropState} />{/if}

  {#if closeListenerError || $closeState.phase !== 'idle'}
    <CloseDialog phase={closeListenerError ? 'failed' : $closeState.phase}
      message={closeListenerError || $closeState.message} startupError={Boolean(closeListenerError)}
      on:keepOpen={() => void close.keepOpen()}
      on:retry={() => { if (closeListenerError) void startCloseListener(); else void close.retry(); }} />
  {/if}
  <div class="background-notices">
    <ExtractionProgress pending={$extractState.pending} on:cancel={() => extractions.cancel()} />
    <Toast />
  </div>

<style>
  .background-notices { position: fixed; right: 20px; bottom: 44px; z-index: 300; display: flex; flex-direction: column; align-items: flex-end; gap: 8px; max-width: min(420px, calc(100vw - 40px)); pointer-events: none; }
  .project-load-error { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 8px; padding: 24px; text-align: center; min-width: 0; }
  .project-load-error h2 { margin: 0; color: var(--fg); font-size: 14px; }
  .project-load-error p { margin: 0; max-width: 48rem; color: var(--muted); font-size: 12px; overflow-wrap: anywhere; }
  .project-load-error button { padding: 5px 10px; border: 1px solid var(--border); border-radius: 4px; background: var(--surface); color: var(--fg); font-size: 12px; }
  .project-load-error button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: 2px; }
</style>
