<script lang="ts">
  import { onMount } from 'svelte';
  import { fade, fly } from 'svelte/transition';
  import { open } from '@tauri-apps/api/dialog';
  import dayjs from 'dayjs';
  import { createBackend } from './lib/backend';
  import type { Backend } from './lib/backend';
  import type { LoadProjectResponse, ProjectSummary } from './lib/types';
  import ProjectView from './lib/components/ProjectView.svelte';

  const backend: Backend = createBackend();

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

  let sidebarOpen = backend.isNative;

  const descriptionInputId = 'project-description';

  let toast: { message: string; tone: 'success' | 'error' } | null = null;

  const showToast = (message: string, tone: 'success' | 'error' = 'success') => {
    toast = { message, tone };
    setTimeout(() => {
      toast = null;
    }, 3200);
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

  const loadProjectDetail = async (projectId: string) => {
    isLoadingDetail = true;
    try {
      const response = await backend.loadProject(projectId);
      projectDetail = response;
      selectedProjectId = projectId;
      sidebarOpen = false;
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
      await loadProjectDetail(summary.meta.id);
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

  const truncateText = (text: string, length: number) => {
    return text.length > length ? text.slice(0, length) + '...' + text.slice(text.length - 6) : text;
  };

const handleRefreshRequest = async () => {
  if (selectedProjectId) {
    await loadProjectDetail(selectedProjectId);
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
  <header class="fixed inset-x-0 top-0 z-50 border-b border-white/10 bg-slate-950/90 backdrop-blur">
    <div class="flex w-full items-center justify-between gap-3 px-4 py-3 sm:px-6">
      <div class="flex items-center gap-3">
        <button
          class="inline-flex items-center justify-center rounded-md border border-white/10 p-2 text-slate-100 transition hover:bg-white/10 focus:outline-none focus:ring-2 focus:ring-indigo-400/40"
          on:click={() => (sidebarOpen = true)}
          aria-label="Open menu"
        >
          ☰
        </button>
        <div class="flex flex-col">
          <h1 class="text-base font-semibold text-white sm:text-lg">
            Trivium
            {#if projectDetail}
              <span class="font-normal text-slate-300"> – {projectDetail.project.meta.name}</span>
            {/if}
          </h1>
          {#if projectDetail?.project.meta.description}
            <p class="text-xs text-slate-400 sm:text-sm">{projectDetail.project.meta.description}</p>
          {/if}
        </div>
      </div>
      <button
        class="inline-flex items-center gap-2 rounded-md border border-white/10 px-3 py-2 text-sm text-slate-300 transition hover:bg-white/10 hover:text-slate-100 focus:outline-none focus:ring-2 focus:ring-indigo-400/40"
        on:click={() => {
          void handleRefreshRequest();
        }}
      >
        ↻ <span class="hidden sm:inline">Refresh</span>
      </button>
    </div>
  </header>

  {#if sidebarOpen}
    <div
      class="fixed inset-x-0 bottom-0 top-[64px] z-[60] flex items-stretch justify-start bg-slate-950/80 backdrop-blur-sm"
      in:fade={{ duration: 120 }}
      out:fade={{ duration: 120 }}
    >
      <button
        type="button"
        class="absolute inset-0 cursor-pointer"
        aria-label="Close workspace menu"
        on:click={() => (sidebarOpen = false)}
        on:keydown={(event) => {
          if (event.key === 'Escape' || event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            sidebarOpen = false;
          }
        }}
      ></button>
      <aside
        class="relative z-10 h-full w-full max-w-sm transform overflow-y-auto border-r border-white/10 bg-trivium-surface p-6 shadow-2xl transition-transform duration-150 sm:w-96"
        role="dialog"
        aria-modal="true"
        in:fly={{ x: -48, duration: 180 }}
        out:fly={{ x: -48, duration: 150 }}
      >
        <div class="flex items-start justify-between">
          <div>
            <h2 class="text-lg font-semibold text-white">Workspace</h2>
            <p class="text-sm text-slate-400">Manage imports and projects</p>
          </div>
        </div>

        <section class="mt-6 space-y-4">
          <div class="space-y-3">
            <button
              class="w-full rounded-lg bg-indigo-500 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-indigo-400 focus:outline-none focus:ring-2 focus:ring-indigo-400/60"
              on:click={() => {
                void pickCsv();
              }}
            >
              Import CSV File
            </button>
            <input
              type="file"
            accept=".csv"
            bind:this={fileInput}
            on:change={handleFileSelection}
            class="hidden"
          />
          {#if pendingFileName}
            <p class="truncate text-xs text-slate-300">{pendingFileName}</p>
          {/if}
          <div class="space-y-2">
            <input
              type="text"
              id={descriptionInputId}
              placeholder="Enter project description [Optional]"
              bind:value={pendingDescription}
              class="w-full rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-500 focus:border-indigo-400 focus:outline-none focus:ring-2 focus:ring-indigo-400/40"
            />
          </div>
            <button
              class="w-full rounded-lg bg-emerald-500 px-4 py-2 text-sm font-semibold text-emerald-950 shadow-sm transition hover:bg-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-400/50 disabled:cursor-not-allowed disabled:bg-emerald-500/30 disabled:text-emerald-900"
              disabled={!canCreateProject || creating}
              on:click={() => {
                void createProject();
            }}
          >
            {creating ? 'Importing…' : 'Create Project'}
          </button>
        </div>
        </section>

        <section class="mt-8 space-y-3">
          <div class="flex items-center justify-between">
            <h2 class="text-md font-semibold">Projects</h2>
            <button
              class="rounded-md border border-white/10 p-2 text-xs text-slate-300 transition hover:bg-white/10 hover:text-slate-100 focus:outline-none focus:ring-2 focus:ring-indigo-400/40"
              on:click={() => {
              void loadProjects();
            }}
            aria-label="Refresh projects"
          >
            ↻
          </button>
        </div>

        {#if isLoadingProjects}
          <p class="text-xs text-slate-400">Loading projects…</p>
        {:else if projects.length === 0}
            <p class="text-xs text-slate-400">No projects yet. Import a CSV to get started.</p>
          {:else}
            <ul class="space-y-3">
              {#each projects as project}
                <li>
                  <div class="flex items-stretch gap-2">
                    <button
                      type="button"
                      class={`flex-1 rounded-lg border border-transparent bg-white/5 px-4 py-3 text-left transition hover:border-white/20 hover:bg-white/10 focus:outline-none focus:ring-2 focus:ring-indigo-400/60 ${
                        project.meta.id === selectedProjectId
                          ? 'border-indigo-400/70 bg-indigo-500/20 shadow-lg shadow-indigo-500/10'
                          : ''
                      }`}
                      on:click={() => {
                        void handleSelectProject(project.meta.id);
                      }}
                    >
                      <div class="flex items-center justify-end gap-3">
                        <span class="whitespace-nowrap text-xs text-slate-400">
                          {dayjs(project.meta.created_at).format('YYYY-MM-DD HH:mm')}
                        </span>
                      </div>
                      <div class="flex items-center justify-between gap-3 mt-1">
                        <span class="truncate text-sm font-semibold text-slate-100">{truncateText(project.meta.name, 19)}</span>
                      </div>
                      <hr class="border-white/10 my-2">
                      {#if project.meta.description}
                        <p class="mt-2 text-xs text-slate-400">{truncateText(project.meta.description, 23)}</p>
                      {/if}
                      <div class="flex flex-wrap items-center justify-between gap-3 text-xs text-slate-400 mt-2">
                        <span>{project.meta.total_records} rows</span>
                        <span>{project.flagged_records} flagged</span>
                      </div>
                    </button>
                    <button
                      type="button"
                      class="shrink-0 rounded-md border border-rose-500/60 px-3 py-2 text-[0.7rem] font-semibold text-rose-200 transition hover:bg-rose-500/20 focus:outline-none focus:ring-2 focus:ring-rose-400/60"
                      on:click={(event) => {
                        event.stopPropagation();
                        void deleteProject(project.meta.id);
                      }}
                    >
                      Delete
                    </button>
                  </div>
                </li>
              {/each}
          </ul>
        {/if}
      </section>
      </aside>
    </div>
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
            on:notify={(event) => showToast(event.detail.message, event.detail.tone)}
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

{#if toast}
  <div
    class={`fixed bottom-6 right-6 z-50 rounded-xl border px-4 py-3 text-sm shadow-lg backdrop-blur ${
      toast.tone === 'error'
        ? 'border-rose-500/60 bg-rose-500/10 text-rose-200'
        : 'border-emerald-500/60 bg-emerald-500/10 text-emerald-100'
    }`}
  >
    {toast.message}
  </div>
{/if}
