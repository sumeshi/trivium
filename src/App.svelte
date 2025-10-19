<script lang="ts">
  import { onMount } from 'svelte';
  import { fade, fly } from 'svelte/transition';
  import { open } from '@tauri-apps/api/dialog';
  import dayjs from 'dayjs';
  import { createBackend } from './lib/backend';
  import type { Backend } from './lib/backend';
  import type { LoadProjectResponse, ProjectSummary } from './lib/types';
  import Sidebar from './lib/components/app/Sidebar.svelte';
  import Header from './lib/components/app/Header.svelte';
  import ProjectView from './lib/components/ProjectView.svelte';
  import Toast from './lib/components/app/Toast.svelte';
import { showToast } from './lib/utils/toast';

  const backend: Backend = createBackend();

  const projectCache = new Map<string, LoadProjectResponse>();

  let projects: ProjectSummary[] = [];
  let selectedProjectId: string | null = null;
  let projectDetail: LoadProjectResponse | null = null;

  let isLoadingProjects = false;
  let isLoadingDetail = false;
  let creating = false;

  let pendingDescription = '';
  let pendingFilePath: string | null = null;
  let pendingFile: File | null = null;
  let pendingFileName = '';
  let fileInput: HTMLInputElement | null = null;
  let canCreateProject = false;

  let sidebarOpen = false;

  const handleNotify = (event: CustomEvent<{ message: string; tone: 'success' | 'error' }>) => {
    showToast(event.detail.message, event.detail.tone);
  };

  let projectsLoaded = false;

  const loadProjects = async (force = false) => {
    if (projectsLoaded && !force) {
      return;
    }
    isLoadingProjects = true;
    try {
      const result = await backend.listProjects();
      projects = result;
      projectsLoaded = true;
      if (selectedProjectId) {
        const stillExists = projects.some((item) => item.meta.id === selectedProjectId);
        if (!stillExists) {
          selectedProjectId = null;
          projectDetail = null;
        }
      }
    } catch (error) {
      console.error(error);
      showToast('Failed to load projects.', 'error');
    } finally {
      isLoadingProjects = false;
    }
  };

  const loadProjectDetail = async (projectId: string, force = false) => {
    if (!force) {
      const cached = projectCache.get(projectId);
      if (cached) {
        projectDetail = cached;
        selectedProjectId = projectId;
        console.log('Project selected (cached), closing sidebar');
        sidebarOpen = false;
        return;
      }
    }
    isLoadingDetail = true;
    try {
      const response = await backend.loadProject(projectId);
      projectDetail = response;
      selectedProjectId = projectId;
      console.log('Project selected (loaded), closing sidebar');
      sidebarOpen = false;
      projectCache.set(projectId, response);
    } catch (error) {
      console.error(error);
      showToast('Failed to load project data.', 'error');
    } finally {
      isLoadingDetail = false;
    }
  };

  const handleSelectProject = async (projectId: string) => {
    await loadProjectDetail(projectId);
  };

  const pickCsv = async () => {
    if (backend.isNative) {
      try {
        const selected = await open({
          multiple: false,
          filters: [{ name: 'CSV Files', extensions: ['csv'] }]
        });
        if (!selected || Array.isArray(selected)) {
          return;
        }
        pendingFilePath = selected;
        pendingFile = null;
        const pathParts = selected.split(/[/\\]/);
        pendingFileName = pathParts[pathParts.length - 1] ?? 'selected.csv';
      } catch (error) {
        console.error(error);
        showToast('Failed to open file picker.', 'error');
      }
    } else if (fileInput) {
      fileInput.value = '';
      fileInput.click();
    }
  };

  const handleFileSelection = (event: Event) => {
    const target = event.currentTarget as HTMLInputElement | null;
    if (!target || !target.files || target.files.length === 0) {
      return;
    }
    const file = target.files[0];
    pendingFile = file;
    pendingFilePath = null;
    pendingFileName = file.name;
  };

  const resetPending = () => {
    pendingFilePath = null;
    pendingFile = null;
    pendingFileName = '';
    pendingDescription = '';
    if (fileInput) {
      fileInput.value = '';
    }
  };

  const createProject = async () => {
    creating = true;
    try {
      let summary: ProjectSummary;
      if (backend.isNative) {
        if (!pendingFilePath) {
          throw new Error('Select a CSV file first.');
        }
        summary = await backend.createProject({
          path: pendingFilePath,
          description: pendingDescription || null
        });
      } else {
        if (!pendingFile) {
          throw new Error('Select a CSV file first.');
        }
        summary = await backend.createProject({
          file: pendingFile,
          description: pendingDescription || null
        });
      }
      showToast(`Imported ${summary.meta.name}`);
      await loadProjects(true);
      await loadProjectDetail(summary.meta.id, true);
      resetPending();
    } catch (error) {
      console.error(error);
      const message = error instanceof Error ? error.message : 'Failed to import CSV.';
      showToast(message, 'error');
    } finally {
      creating = false;
    }
  };

  const deleteProject = async (projectId: string) => {
    const confirmed = window.confirm('Delete this project? The imported copy will be removed.');
    if (!confirmed) {
      return;
    }
    try {
      await backend.deleteProject(projectId);
      showToast('Project deleted.');
      projectCache.delete(projectId);
      await loadProjects(true);
      if (selectedProjectId === projectId) {
        selectedProjectId = null;
        projectDetail = null;
      }
    } catch (error) {
      console.error(error);
      showToast('Failed to delete project.', 'error');
    }
  };

  const handleRefreshRequest = async () => {
    if (selectedProjectId) {
      await loadProjectDetail(selectedProjectId, true);
    }
    await loadProjects(true);
  };

  const handleSummaryUpdate = (event: CustomEvent<{ flagged: number; hiddenColumns: string[] }>) => {
    if (!projectDetail || !selectedProjectId) return;
    projectDetail = {
      ...projectDetail,
      project: {
        ...projectDetail.project,
        flagged_records: event.detail.flagged
      },
      hidden_columns: event.detail.hiddenColumns
    };
    projects = projects.map((item) =>
      item.meta.id === selectedProjectId
        ? {
            ...item,
            flagged_records: event.detail.flagged,
            meta: {
              ...item.meta,
              hidden_columns: event.detail.hiddenColumns
            }
          }
        : item
    );
    projectCache.set(selectedProjectId, projectDetail);
  };

  onMount(() => {
    if (!backend.isNative) {
      console.info('Running without Tauri backend; using in-memory data store.');
    }
    void loadProjects();
  });

  $: canCreateProject = backend.isNative ? Boolean(pendingFilePath) : Boolean(pendingFile);
</script>

<div class="min-h-screen bg-slate-950 text-slate-100">
  <Header {projectDetail} on:menuClick={() => { sidebarOpen = !sidebarOpen; }} on:refreshClick={handleRefreshRequest} />

  {#if sidebarOpen}
    <Sidebar
      {projects}
      {selectedProjectId}
      {isLoadingProjects}
      {creating}
      bind:pendingDescription
      {pendingFileName}
      {canCreateProject}
      on:selectProject={(e) => handleSelectProject(e.detail)}
      on:deleteProject={(e) => deleteProject(e.detail)}
      on:pickCsv={pickCsv}
      on:createProject={createProject}
      on:fileSelection={handleFileSelection}
      on:loadProjects={() => loadProjects()}
      on:close={() => (sidebarOpen = false)}
    />
  {/if}

  <main class="relative z-0 flex min-h-screen w-full flex-col px-4 pb-12 pt-24 sm:px-6">
    {#if !backend.isNative}
      <div class="mb-6 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
        Web preview stores data in memory only. Use the Tauri desktop build to persist projects.
      </div>
    {/if}
    {#if selectedProjectId}
      {#if isLoadingDetail}
        <div class="flex flex-1 items-center justify-center text-slate-400">
          Loading project…
        </div>
      {:else if projectDetail}
        <div class="flex-1 min-h-0">
          <ProjectView
            {backend}
            {projectDetail}
            on:refresh={handleRefreshRequest}
            on:summary={handleSummaryUpdate}
            on:notify={handleNotify}
          />
        </div>
      {/if}
    {:else}
      <div class="flex flex-1 flex-col items-center justify-center gap-3 text-center text-slate-400">
        <h2 class="text-xl font-semibold text-slate-100">Select a project</h2>
        <p class="max-w-sm text-sm">
          Pick a project from the menu or import a new CSV to get started.
        </p>
      </div>
    {/if}
  </main>
</div>

  <Toast />
