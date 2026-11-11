<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { ProjectSummary } from '../../types';

  export let projects: ProjectSummary[] = [];
  export let selectedProjectId: string | null = null;
  const dispatch = createEventDispatcher<{ select: string; close: string }>();
  const label = (project: ProjectSummary) => project.meta.source_file_name || project.meta.name;

  const navigate = (event: KeyboardEvent, index: number) => {
    if (event.key === 'Delete') {
      event.preventDefault();
      dispatch('close', projects[index].meta.id);
      return;
    }
    const next = event.key === 'ArrowRight' ? (index + 1) % projects.length
      : event.key === 'ArrowLeft' ? (index + projects.length - 1) % projects.length
      : event.key === 'Home' ? 0 : event.key === 'End' ? projects.length - 1 : -1;
    if (next < 0) return;
    event.preventDefault();
    const tabs = (event.currentTarget as HTMLElement).closest('[role=tablist]')?.querySelectorAll<HTMLButtonElement>('[role=tab]');
    tabs?.[next]?.focus();
    dispatch('select', projects[next].meta.id);
  };
</script>

{#if projects.length > 0}
  <div class="project-tabs" role="tablist" aria-label="Open projects">
    {#each projects as project, index (project.meta.id)}
      <div class="project-tab" class:active={selectedProjectId === project.meta.id} role="presentation">
        <button
          class="tab-label"
          type="button"
          role="tab"
          id={`project-tab-${project.meta.id}`}
          aria-selected={selectedProjectId === project.meta.id}
          aria-controls={`project-panel-${project.meta.id}`}
          tabindex={selectedProjectId === project.meta.id ? 0 : -1}
          title={label(project)}
          on:click={() => dispatch('select', project.meta.id)}
          on:keydown={(event) => navigate(event, index)}
        >{label(project)}</button>
        <button
          class="tab-close"
          type="button"
          aria-label={`Close ${label(project)}`}
          title="Close tab; saved project and annotations are kept"
          tabindex={selectedProjectId === project.meta.id ? 0 : -1}
          on:click={() => dispatch('close', project.meta.id)}
        >
          <svg aria-hidden="true" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="m6 6 12 12M6 18 18 6" /></svg>
        </button>
      </div>
    {/each}
  </div>
{/if}

<style>
  .project-tabs { display: flex; flex: 0 0 auto; gap: 2px; height: 31px; padding: 0 12px; overflow-x: auto; border-bottom: 1px solid var(--border); background: var(--surface-soft); }
  .project-tab { display: flex; flex: 0 0 auto; align-items: center; min-width: 70px; max-width: 240px; border-bottom: 2px solid transparent; color: var(--muted); }
  .project-tab.active { border-bottom-color: var(--accent); background: var(--surface); color: var(--fg); }
  button { height: 27px; border: 0; border-radius: 0; background: transparent; color: inherit; box-shadow: none; }
  .tab-label { display: block; min-width: 0; padding: 2px 7px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 11.5px; text-align: left; }
  .tab-close { display: flex; align-items: center; justify-content: center; flex: 0 0 24px; width: 24px; padding: 0; }
  button:hover { background: var(--hover); }
  button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: -2px; }
</style>
