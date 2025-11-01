<script lang="ts">
  import { onMount } from 'svelte';
  import { open } from '@tauri-apps/api/dialog';
  import { createBackend } from './lib/backend';
  import type { Backend } from './lib/backend';
  import Sidebar from './lib/components/app/Sidebar.svelte';
  import Header from './lib/components/app/Header.svelte';
  import ProjectView from './lib/components/ProjectView.svelte';
  import Toast from './lib/components/app/Toast.svelte';
  import { showToast } from './lib/utils/toast';
  import { initTheme } from './lib/theme';
  import { createProjectController } from './lib/stores/projects';

  const backend: Backend = createBackend();
  const projectController = createProjectController(backend);
  const projectState = projectController.state;
  const canCreateProject = projectController.canCreateProject;

  let sidebarOpen = true;

  const handleNotify = (event: CustomEvent<{ message: string; tone: 'success' | 'error' }>) => {
    showToast(event.detail.message, event.detail.tone);
  };

  const pickCsv = async () => {
    try {
      const selected = await open({
        multiple: false,
        filters: [{ name: 'CSV Files', extensions: ['csv'] }]
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
      const summary = await projectController.createProject();
      showToast(`Imported ${summary.meta.name}`);
      sidebarOpen = false;
    } catch (error) {
      console.error(error);
      const message = error instanceof Error ? error.message : 'Failed to import CSV.';
      showToast(message, 'error');
    }
  };

  const deleteProject = async (projectId: string) => {
    const confirmed = window.confirm('Delete this project? The imported copy will be removed.');
    if (!confirmed) {
      return;
    }
    try {
      await projectController.deleteProject(projectId);
      showToast('Project deleted.');
    } catch (error) {
      console.error(error);
      showToast('Failed to delete project.', 'error');
    }
  };

  const handleRefreshRequest = async () => {
    try {
      await projectController.refreshSelected();
    } catch (error) {
      console.error(error);
      showToast('Failed to refresh project data.', 'error');
    }
  };

  const handleSelectProject = async (projectId: string) => {
    try {
      await projectController.selectProject(projectId);
      sidebarOpen = false;
    } catch (error) {
      console.error(error);
      showToast('Failed to load project data.', 'error');
    }
  };

  const handleSummaryUpdate = (event: CustomEvent<{ flagged: number; iocApplied: number; hiddenColumns: string[] }>) => {
    projectController.updateSummary(event.detail);
  };

  onMount(() => {
    void initTheme();
    void projectController.loadProjects().catch((error) => {
      console.error(error);
      showToast('Failed to load projects.', 'error');
    });
  });
</script>

<div class="app-shell">
  <Header projectDetail={$projectState.projectDetail} on:menuClick={() => { sidebarOpen = !sidebarOpen; }} on:refreshClick={handleRefreshRequest} />

  {#if sidebarOpen}
    <Sidebar
      projects={$projectState.projects}
      selectedProjectId={$projectState.selectedProjectId}
      isLoadingProjects={$projectState.isLoadingProjects}
      creating={$projectState.creating}
      pendingDescription={$projectState.pendingDescription}
      pendingFileName={$projectState.pendingFileName}
      canCreateProject={$canCreateProject}
      on:descriptionChange={(e) => projectController.setPendingDescription(e.detail)}
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

  <main class="relative z-0 flex w-full flex-col px-4 pt-24 pb-4 sm:px-6" style="height: 100vh; overflow: hidden;">
    {#if $projectState.selectedProjectId}
      {#if $projectState.isLoadingDetail}
        <div class="flex flex-1 items-center justify-center text-muted">
          Loading project…
        </div>
      {:else if $projectState.projectDetail}
        <div class="flex-1 min-h-0">
          <ProjectView
            {backend}
            projectDetail={$projectState.projectDetail}
            on:refresh={handleRefreshRequest}
            on:summary={handleSummaryUpdate}
            on:notify={handleNotify}
          />
        </div>
      {/if}
    {:else}
      <div class="flex flex-1 flex-col items-center justify-center gap-3 text-center text-muted">
        <h2 class="text-xl font-semibold heading-text">Select a project</h2>
        <p class="max-w-sm text-sm">
          Pick a project from the menu or import a new CSV to get started.
        </p>
      </div>
    {/if}
  </main>
</div>

  <Toast />
