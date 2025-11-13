<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { fade, fly } from 'svelte/transition';
  import dayjs from 'dayjs';
  import type { ProjectSummary } from '../../types';
  import { theme, toggleTheme } from '../../theme';

  const dispatch = createEventDispatcher();

  export let projects: ProjectSummary[];
  export let selectedProjectId: string | null;
  export let isLoadingProjects: boolean;
  export let creating: boolean;
  export let pendingDescription: string;
  export let pendingFileName: string;
  export let canCreateProject: boolean;

  const truncateText = (text: string, length: number) => {
    return text.length > length ? text.slice(0, length) + '...' + text.slice(text.length - 6) : text;
  };

  const handleDescriptionInput = (event: Event) => {
    const nextValue = (event.target as HTMLInputElement | null)?.value ?? '';
    dispatch('descriptionChange', nextValue);
  };
</script>

<div
  class="fixed inset-x-0 bottom-0 top-[64px] z-[60] flex items-stretch justify-start bg-overlay backdrop-blur-sm"
  in:fade={{ duration: 120 }}
  out:fade={{ duration: 120 }}
>
  <button
    type="button"
    class="absolute inset-0 cursor-pointer"
    aria-label="Close workspace menu"
    on:click={() => dispatch('close')}
    on:keydown={(event) => {
      if (event.key === 'Escape' || event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        dispatch('close');
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
        <h2 class="text-lg font-semibold heading-text">Workspace</h2>
        <p class="text-sm text-muted">Manage imports and projects</p>
      </div>
      <button
        type="button"
        class="rounded-md border border-white/10 p-2 text-muted transition hover:bg-white/10 hover:text-white focus:outline-none focus:ring-2 focus:ring-indigo-400/40"
        on:click={() => toggleTheme()}
        aria-label="Toggle theme"
        title="Toggle theme"
      >
        {#if $theme === 'dark'}
          <!-- Sun icon for light mode target -->
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <circle cx="12" cy="12" r="4"/>
            <path d="M12 2v2m0 16v2m10-10h-2M4 12H2m15.364-7.364-1.414 1.414M8.05 16.95l-1.414 1.414m12.728 0-1.414-1.414M8.05 7.05 6.636 5.636"/>
          </svg>
        {:else}
          <!-- Moon icon for dark mode target -->
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
          </svg>
        {/if}
      </button>
    </div>

    <section class="mt-6 space-y-4">
      <div class="space-y-3">
        <button
          class="w-full rounded-lg bg-indigo-500 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-indigo-400 focus:outline-none focus:ring-2 focus:ring-indigo-400/60"
          on:click={() => dispatch('pickCsv')}
        >
          Import CSV File
        </button>
        {#if pendingFileName}
          <p class="truncate text-xs text-muted">{pendingFileName}</p>
        {/if}
        <div class="space-y-2">
          <input
            type="text"
            id="project-description"
            placeholder="Enter project description [Optional]"
            value={pendingDescription}
            on:input={handleDescriptionInput}
            class="w-full rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-sm text-white placeholder:text-muted focus:border-indigo-400 focus:outline-none focus:ring-2 focus:ring-indigo-400/40"
          />
        </div>
        <button
          class="w-full rounded-lg bg-emerald-500 px-4 py-2 text-sm font-semibold text-emerald-950 shadow-sm transition hover:bg-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-400/50 disabled:cursor-not-allowed disabled:bg-emerald-500/30 disabled:text-emerald-900"
          disabled={!canCreateProject || creating}
          on:click={() => dispatch('createProject')}
        >
          {creating ? 'Importing…' : 'Create Project'}
        </button>
      </div>
    </section>

    <section class="mt-8 space-y-3">
      <div class="flex items-center justify-between">
        <h2 class="text-md font-semibold heading-text">Projects</h2>
        <button
          class="rounded-md border border-white/10 p-2 text-xs text-muted transition hover:bg-white/10 hover:text-white focus:outline-none focus:ring-2 focus:ring-indigo-400/40"
          on:click={() => dispatch('loadProjects')}
          aria-label="Refresh projects"
        >
          ↻
        </button>
      </div>

      {#if isLoadingProjects}
        <p class="text-xs text-muted">Loading projects…</p>
      {:else if projects.length === 0}
        <p class="text-xs text-muted">No projects yet. Import a CSV to get started.</p>
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
                  on:click={() => dispatch('selectProject', project.meta.id)}
                >
                  <div class="flex items-center justify-end gap-3">
                    <span class="whitespace-nowrap text-xs text-muted">
                      {dayjs(project.meta.created_at).format('YYYY-MM-DD HH:mm')}
                    </span>
                  </div>
                  <div class="flex items-center justify-between gap-3 mt-1">
                    <span class="truncate text-sm font-semibold heading-text">{truncateText(project.meta.name, 16)}</span>
                  </div>
                  <hr class="border-white/10 my-2">
                  {#if project.meta.description}
                    <p class="mt-2 text-xs text-muted">{truncateText(project.meta.description, 20)}</p>
                  {/if}
                  <div class="flex flex-wrap items-center justify-between gap-3 text-xs text-muted mt-2">
                    <span>{project.meta.total_records} rows</span>
                    <span>{project.meta.flagged_records + project.meta.ioc_applied_records} flagged</span>
                  </div>
                </button>
                <button
                  type="button"
                  class="shrink-0 rounded-md border border-rose-500/60 px-3 py-2 text-[0.7rem] font-semibold text-rose-200 transition hover:bg-rose-500/20 focus:outline-none focus:ring-2 focus:ring-rose-400/60"
                  on:click={(event) => {
                    event.stopPropagation();
                    dispatch('deleteProject', project.meta.id);
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
