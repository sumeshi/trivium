<script lang="ts">
  import { onDestroy, onMount, tick } from 'svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { listen } from '@tauri-apps/api/event';
  import { getCurrentWebview } from '@tauri-apps/api/webview';
  import type { UnlistenFn } from '@tauri-apps/api/event';
  import { open } from '@tauri-apps/plugin-dialog';
  import { createBackend } from './lib/backend';
  import type { Backend } from './lib/backend';
  import Sidebar from './lib/components/app/Sidebar.svelte';
  import Header from './lib/components/app/Header.svelte';
  import ProjectView from './lib/components/ProjectView.svelte';
  import ProjectTabs from './lib/components/app/ProjectTabs.svelte';
  import Toast from './lib/components/app/Toast.svelte';
  import CloseDialog from './lib/components/app/CloseDialog.svelte';
  import FileDropOverlay from './lib/components/app/FileDropOverlay.svelte';
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

  onDestroy(() => {
    appDestroyed = true;
    stopCloseListener?.();
    stopFileDropListener?.();
    fileDrop.dispose();
    close.dispose();
    projectController.dispose();
    sessions.clear();
  });

  let sidebarOpen = true;
  let activeView: ProjectView | null = null;
  let closeReady = false;
  let closeListenerError = '';
  let stopCloseListener: UnlistenFn | undefined;
  let stopFileDropListener: UnlistenFn | undefined;
  let startingCloseListener = false;
  let appDestroyed = false;
  let initializedApp = false;
  const close = createAppClose({
    unsavedDraft: () => activeView?.unsavedDraftMessage() ?? null,
    notifyDraft: (message) => {
      showToast(message, 'error');
      void tick().then(() => activeView?.focusUnsavedDraft());
    },
    freeze: () => activeView?.pauseForClose(),
    drain: flushAllSaves,
    flushActive: async () => { await tick(); await activeView?.flushForClose(); },
    seal: sealSaveAdmission,
    finish: () => invoke<void>('finish_app_close'),
    cancelNative: (acknowledgeFailures) => invoke<void>('cancel_app_close', { acknowledgeFailures }),
    resume: reopenSaveAdmission,
  });
  const closeState = close.state;
  $: closing = $closeState.phase !== 'idle';
  const editsBlocked = () => !closeReady || close.isBlocked();
  const fileDrop = createFileDrop({
    blocked: () => !closeReady ? 'Wait for the app to be ready.'
      : close.isBlocked() ? 'Finish the close request before adding files.'
      : $projectState.creating ? 'An import is already running.'
      : document.querySelector('.cell-dialog-backdrop') ? 'Close the dialog before adding files.' : null,
    select: (paths) => {
      projectController.setPendingFiles(paths);
      sidebarOpen = true;
      void tick().then(() => document.querySelector<HTMLElement>('.workspace-drawer [aria-label="CSV delimiter"], .workspace-drawer .button.primary')?.focus());
    },
    notify: message => showToast(message, 'error'),
  });
  const fileDropState = fileDrop.state;
  $: if (closing) fileDrop.clear();
  const sessions = new Map<string, WorkspaceSession>();
  $: openProjects = $projectState.openProjectIds.flatMap((id) => {
    const project = $projectState.projects.find((item) => item.meta.id === id);
    return project ? [project] : [];
  });
  $: for (const id of sessions.keys()) {
    if (!$projectState.openProjectIds.includes(id)) sessions.delete(id);
  }

  const retainSession = (session: WorkspaceSession) => {
    if ($projectState.openProjectIds.includes(session.projectId)) {
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
        const result = await projectController.createProjects();
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

  const deleteProject = async (projectId: string) => {
    if (editsBlocked()) return;
    const confirmed = window.confirm('Delete this project? The imported copy will be removed.');
    if (!confirmed) {
      return;
    }
    try {
      await acceptSave(async () => {
        captureActive();
        await projectController.deleteProject(projectId);
        sessions.delete(projectId);
        showToast('Project deleted.');
        await tick();
      });
    } catch (error) {
      console.error(error);
      showToast('Failed to delete project.', 'error');
    }
  };

  const handleRefreshRequest = async () => {
    if (editsBlocked()) return;
    try {
      await acceptSave(async () => {
        captureActive();
        await projectController.refreshSelected();
        await tick();
      });
    } catch (error) {
      console.error(error);
      showToast('Failed to refresh project data.', 'error');
    }
  };

  const handleSelectProject = async (projectId: string) => {
    if (editsBlocked()) return;
    try {
      await acceptSave(async () => {
        captureActive();
        await projectController.selectProject(projectId);
        sidebarOpen = false;
        await tick();
      });
    } catch (error) {
      console.error(error);
      showToast('Failed to load project data.', 'error');
    }
  };

  const handleCloseTab = async (projectId: string) => {
    if (editsBlocked()) return;
    try {
      await acceptSave(async () => {
        captureActive();
        sessions.delete(projectId);
        await projectController.closeProject(projectId);
        await tick();
      });
    } catch (error) {
      showToast(errorMessage(error, 'Failed to load the next project.'), 'error');
    }
  };

  const handleSummaryUpdate = (event: CustomEvent<{ projectId: string; flagged: number; iocApplied: number; hiddenColumns: string[] }>) => {
    projectController.updateSummary(event.detail);
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
    void getCurrentWebview().onDragDropEvent(event => fileDrop.handle(event.payload))
      .then(stop => { if (appDestroyed) stop(); else stopFileDropListener = stop; })
      .catch(() => { if (!appDestroyed) showToast('Could not enable file drops. Use Choose data files.', 'error'); });
  });
</script>

<svelte:window on:blur={() => fileDrop.clear()} on:keydown={(event) => { if (event.key === 'Escape') fileDrop.clear(); }} />

<div class="app-shell" inert={!closeReady || $closeState.phase !== 'idle'}>
  <Header projectDetail={$projectState.projectDetail} on:menuClick={() => { if (!editsBlocked()) sidebarOpen = !sidebarOpen; }} on:refreshClick={handleRefreshRequest} />
  <ProjectTabs projects={openProjects} selectedProjectId={$projectState.selectedProjectId} on:select={(event) => handleSelectProject(event.detail)} on:close={(event) => handleCloseTab(event.detail)} />

  {#if sidebarOpen}
    <Sidebar
      projects={$projectState.projects}
      selectedProjectId={$projectState.selectedProjectId}
      isLoadingProjects={$projectState.isLoadingProjects}
      creating={$projectState.creating}
      pendingDescription={$projectState.pendingDescription}
      pendingFilePaths={$projectState.pendingFilePaths}
      pendingDelimiter={$projectState.pendingDelimiter}
      pendingRestoreAnnotations={$projectState.pendingRestoreAnnotations}
      importProgress={$projectState.importProgress}
      importFailures={$projectState.importFailures}
      canCreateProject={$canCreateProject}
      on:descriptionChange={(e) => projectController.setPendingDescription(e.detail)}
      on:delimiterChange={(e) => projectController.setPendingDelimiter(e.detail)}
      on:restoreAnnotationsChange={(e) => projectController.setPendingRestoreAnnotations(e.detail)}
      on:selectProject={(e) => handleSelectProject(e.detail)}
      on:deleteProject={(e) => deleteProject(e.detail)}
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
      {:else if $projectState.projectDetail}
        {#key $projectState.selectedProjectId}
        <div class="flex-1 min-h-0" id={`project-panel-${$projectState.selectedProjectId}`} role="tabpanel" aria-labelledby={`project-tab-${$projectState.selectedProjectId}`}>
          <ProjectView
            bind:this={activeView}
            {backend}
            closing={closing || !closeReady}
            projectDetail={$projectState.projectDetail}
            session={sessions.get($projectState.selectedProjectId) ?? null}
            onSessionSnapshot={retainSession}
            on:refresh={handleRefreshRequest}
            on:summary={handleSummaryUpdate}
            on:notify={handleNotify}
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

{#if $fileDropState}<FileDropOverlay state={$fileDropState} />{/if}

  {#if closeListenerError || $closeState.phase !== 'idle'}
    <CloseDialog phase={closeListenerError ? 'failed' : $closeState.phase}
      message={closeListenerError || $closeState.message} startupError={Boolean(closeListenerError)}
      on:keepOpen={() => void close.keepOpen()}
      on:retry={() => { if (closeListenerError) void startCloseListener(); else void close.retry(); }} />
  {/if}
  <Toast />
