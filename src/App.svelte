<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { open } from '@tauri-apps/plugin-dialog';
  import { createBackend } from './lib/backend';
  import type { Backend } from './lib/backend';
  import Sidebar from './lib/components/app/Sidebar.svelte';
  import Header from './lib/components/app/Header.svelte';
  import ProjectView from './lib/components/ProjectView.svelte';
  import ProjectTabs from './lib/components/app/ProjectTabs.svelte';
  import Toast from './lib/components/app/Toast.svelte';
  import { showToast } from './lib/utils/toast';
  import { initTheme } from './lib/theme';
  import { initTableTextSize } from './lib/tableTextSize';
  import { createProjectController } from './lib/stores/projects';
  import { errorMessage } from './lib/errors';
  import { copyWorkspaceSession } from './lib/workspaceSession';
  import type { WorkspaceSession } from './lib/workspaceSession';

  const backend: Backend = createBackend();
  const projectController = createProjectController(backend);
  const projectState = projectController.state;
  const canCreateProject = projectController.canCreateProject;

  onDestroy(() => {
    projectController.dispose();
    sessions.clear();
  });

  let sidebarOpen = true;
  let activeView: ProjectView | null = null;
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
    try {
      const selected = await open({
        multiple: false,
        // Linux file-dialog filters are case-sensitive, so list the uppercase
        // spellings as well. The backend lowercases the extension again.
        filters: [
          {
            name: 'Data files (CSV, JSON, JSONL)',
            extensions: ['csv', 'CSV', 'json', 'JSON', 'jsonl', 'JSONL', 'ndjson', 'NDJSON']
          }
        ]
      });
      if (!selected || Array.isArray(selected)) {
        return;
      }
      projectController.setPendingFile(selected);
    } catch (error) {
      console.error(error);
      showToast('Failed to open file picker.', 'error');
    }
  };

  const createProject = async () => {
    try {
      captureActive();
      const summary = await projectController.createProject();
      showToast(`Imported ${summary.meta.name}`);
      sidebarOpen = false;
    } catch (error) {
      console.error(error);
      // Tauri commands return Result<_, String>, so the line/record reason is
      // usually a plain string rather than an Error instance.
      showToast(errorMessage(error, 'Failed to import file.'), 'error');
    }
  };

  const deleteProject = async (projectId: string) => {
    const confirmed = window.confirm('Delete this project? The imported copy will be removed.');
    if (!confirmed) {
      return;
    }
    try {
      captureActive();
      await projectController.deleteProject(projectId);
      sessions.delete(projectId);
      showToast('Project deleted.');
    } catch (error) {
      console.error(error);
      showToast('Failed to delete project.', 'error');
    }
  };

  const handleRefreshRequest = async () => {
    try {
      captureActive();
      await projectController.refreshSelected();
    } catch (error) {
      console.error(error);
      showToast('Failed to refresh project data.', 'error');
    }
  };

  const handleSelectProject = async (projectId: string) => {
    try {
      captureActive();
      await projectController.selectProject(projectId);
      sidebarOpen = false;
    } catch (error) {
      console.error(error);
      showToast('Failed to load project data.', 'error');
    }
  };

  const handleCloseTab = async (projectId: string) => {
    captureActive();
    sessions.delete(projectId);
    try {
      await projectController.closeProject(projectId);
    } catch (error) {
      showToast(errorMessage(error, 'Failed to load the next project.'), 'error');
    }
  };

  const handleSummaryUpdate = (event: CustomEvent<{ projectId: string; flagged: number; iocApplied: number; hiddenColumns: string[] }>) => {
    projectController.updateSummary(event.detail);
  };

  onMount(() => {
    void initTableTextSize();
    void initTheme();
    void projectController.loadProjects().catch((error) => {
      console.error(error);
      showToast('Failed to load projects.', 'error');
    });
  });
</script>

<div class="app-shell">
  <Header projectDetail={$projectState.projectDetail} on:menuClick={() => { sidebarOpen = !sidebarOpen; }} on:refreshClick={handleRefreshRequest} />
  <ProjectTabs projects={openProjects} selectedProjectId={$projectState.selectedProjectId} on:select={(event) => handleSelectProject(event.detail)} on:close={(event) => handleCloseTab(event.detail)} />

  {#if sidebarOpen}
    <Sidebar
      projects={$projectState.projects}
      selectedProjectId={$projectState.selectedProjectId}
      isLoadingProjects={$projectState.isLoadingProjects}
      creating={$projectState.creating}
      pendingDescription={$projectState.pendingDescription}
      pendingFileName={$projectState.pendingFileName}
      pendingDelimiter={$projectState.pendingDelimiter}
      canCreateProject={$canCreateProject}
      on:descriptionChange={(e) => projectController.setPendingDescription(e.detail)}
      on:delimiterChange={(e) => projectController.setPendingDelimiter(e.detail)}
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

  <Toast />
