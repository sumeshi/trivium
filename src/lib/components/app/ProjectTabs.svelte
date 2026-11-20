<script lang="ts">
  import { createEventDispatcher, onDestroy, tick } from 'svelte';
  import type { ProjectSummary } from '../../types';
  import ProjectTabContextMenu from './ProjectTabContextMenu.svelte';

  export let projects: ProjectSummary[] = [];
  export let selectedProjectId: string | null = null;
  export let temporaryProjectIds: string[] = [];
  export let busy = false;
  const dispatch = createEventDispatcher<{ select: string; close: string; closeOthers: string; closeRight: string; closeAll: void }>();
  const label = (project: ProjectSummary) => project.meta.source_file_name || project.meta.name;
  let tabList: HTMLDivElement;
  let stripEl: HTMLDivElement;
  let hasOverflow = false;
  let atStart = true;
  let atEnd = true;
  let layoutVersion = 0;
  let disposed = false;
  let menuState: { projectId: string; trigger: HTMLButtonElement; x: number; y: number } | null = null;

  const closeMenu = (restoreFocus = false) => {
    const trigger = menuState?.trigger;
    menuState = null;
    if (restoreFocus && trigger?.isConnected) trigger.focus({ preventScroll: true });
  };
  const openMenu = (event: MouseEvent | KeyboardEvent, projectId: string) => {
    event.preventDefault();
    event.stopPropagation();
    if (busy || !projects.some(project => project.meta.id === projectId)) return;
    const trigger = event.currentTarget as HTMLButtonElement;
    const bounds = trigger.getBoundingClientRect();
    const pointer = 'clientX' in event && (event.clientX !== 0 || event.clientY !== 0);
    menuState = { projectId, trigger, x: pointer ? event.clientX : bounds.left, y: pointer ? event.clientY : bounds.bottom };
  };
  const contextKey = (event: KeyboardEvent, projectId: string) => {
    if (event.key !== 'ContextMenu' && !(event.shiftKey && event.key === 'F10')) return false;
    openMenu(event, projectId);
    return true;
  };
  const runMenuAction = (action: 'close' | 'others' | 'right' | 'all') => {
    const target = menuState?.projectId;
    closeMenu();
    const index = projects.findIndex(project => project.meta.id === target);
    if (busy || !target || index < 0) return;
    if (action === 'close') dispatch('close', target);
    else if (action === 'others' && projects.length > 1) dispatch('closeOthers', target);
    else if (action === 'right' && index < projects.length - 1) dispatch('closeRight', target);
    else if (action === 'all') dispatch('closeAll');
  };
  const selectProject = (projectId: string) => { if (!busy) dispatch('select', projectId); };
  const closeProject = (projectId: string) => { if (!busy) dispatch('close', projectId); };
  $: if (menuState && (busy || !projects.some(project => project.meta.id === menuState?.projectId))) closeMenu();
  $: contextIndex = menuState ? projects.findIndex(project => project.meta.id === menuState?.projectId) : -1;
  $: contextProject = projects[contextIndex];

  const updateScrollEdges = () => {
    if (!tabList) return;
    atStart = tabList.scrollLeft <= 1;
    atEnd = tabList.scrollLeft + tabList.clientWidth >= tabList.scrollWidth - 1;
  };

  const revealSelected = () => {
    if (!tabList || !selectedProjectId) return;
    const tab = document.getElementById(`project-tab-${selectedProjectId}`)?.parentElement;
    if (!tab || !tabList.contains(tab)) return;
    const viewport = tabList.getBoundingClientRect();
    const bounds = tab.getBoundingClientRect();
    if (bounds.left < viewport.left) tabList.scrollLeft += bounds.left - viewport.left;
    else if (bounds.right > viewport.right) tabList.scrollLeft += bounds.right - viewport.right;
    updateScrollEdges();
  };

  const refreshLayout = async (_key: string) => {
    const version = ++layoutVersion;
    await tick();
    if (disposed || version !== layoutVersion || !stripEl || !tabList) return;
    const style = getComputedStyle(stripEl);
    // Compare with the full strip width, independent of whether scroll buttons
    // currently occupy space, so removing the buttons cannot oscillate.
    const available = stripEl.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
    hasOverflow = tabList.scrollWidth > available + 1;
    await tick();
    if (!disposed && version === layoutVersion) revealSelected();
  };

  const observeStrip = (node: HTMLDivElement) => {
    const observer = new ResizeObserver(() => { void refreshLayout(layoutKey); });
    observer.observe(node);
    void tick().then(() => {
      if (node.isConnected && tabList) observer.observe(tabList);
    });
    return { destroy: () => observer.disconnect() };
  };

  // Counts/annotations may replace summary objects without changing tab layout.
  // Those updates must not move a manually scrolled strip back to its selection.
  $: layoutKey = JSON.stringify([selectedProjectId, projects.map((project) => [project.meta.id, label(project)])]);
  $: void refreshLayout(layoutKey);

  const scrollTabs = (direction: -1 | 1) => {
    tabList?.scrollBy({ left: direction * Math.max(96, tabList.clientWidth * .8) });
    updateScrollEdges();
  };

  const handleWheel = (event: WheelEvent) => {
    if (!hasOverflow || !tabList) return;
    const delta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
    if (!delta) return;
    event.preventDefault();
    const scale = event.deltaMode === 1 ? 24 : event.deltaMode === 2 ? tabList.clientWidth : 1;
    tabList.scrollLeft += delta * scale;
    updateScrollEdges();
  };

  onDestroy(() => { disposed = true; layoutVersion += 1; });

  const navigate = (event: KeyboardEvent, index: number) => {
    const project = projects[index];
    if (busy || !project || contextKey(event, project.meta.id)) return;
    if (event.key === 'Delete') {
      event.preventDefault();
      closeProject(project.meta.id);
      return;
    }
    const next = event.key === 'ArrowRight' ? (index + 1) % projects.length
      : event.key === 'ArrowLeft' ? (index + projects.length - 1) % projects.length
      : event.key === 'Home' ? 0 : event.key === 'End' ? projects.length - 1 : -1;
    if (next < 0) return;
    event.preventDefault();
    const tabs = (event.currentTarget as HTMLElement).closest('[role=tablist]')?.querySelectorAll<HTMLButtonElement>('[role=tab]');
    tabs?.[next]?.focus();
    selectProject(projects[next].meta.id);
  };
</script>

{#if projects.length > 0}
  <div class="project-tab-strip" bind:this={stripEl} use:observeStrip>
    {#if hasOverflow}
      <button class="tab-scroll" type="button" aria-label="Scroll tabs left" title="Scroll tabs left"
        disabled={busy || atStart} on:click={() => scrollTabs(-1)}>
        <svg aria-hidden="true" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="m15 5-7 7 7 7" /></svg>
      </button>
    {/if}
  <div class="project-tabs" bind:this={tabList} role="tablist" aria-label="Open projects"
    on:wheel|nonpassive={handleWheel} on:scroll={updateScrollEdges}>
    {#each projects as project, index (project.meta.id)}
      <div class="project-tab" class:active={selectedProjectId === project.meta.id} role="presentation">
        <button
          class="tab-label"
          type="button"
          role="tab"
          id={`project-tab-${project.meta.id}`}
          aria-selected={selectedProjectId === project.meta.id}
          aria-controls={`project-panel-${project.meta.id}`}
          aria-haspopup="menu" aria-expanded={menuState?.projectId === project.meta.id}
          tabindex={selectedProjectId === project.meta.id ? 0 : -1}
          disabled={busy}
          title={label(project)}
          on:click={() => selectProject(project.meta.id)}
          on:contextmenu={(event) => openMenu(event, project.meta.id)}
          on:keydown={(event) => navigate(event, index)}
        >{label(project)}</button>
        <button
          class="tab-close"
          type="button"
          aria-label={`Close ${label(project)}`}
          title={temporaryProjectIds.includes(project.meta.id) ? 'Close temporary CSV; export to keep it' : 'Close tab; saved project and annotations are kept'}
          tabindex={selectedProjectId === project.meta.id ? 0 : -1}
          disabled={busy} aria-haspopup="menu"
          on:click={() => closeProject(project.meta.id)}
          on:contextmenu={(event) => openMenu(event, project.meta.id)}
          on:keydown={(event) => contextKey(event, project.meta.id)}
        >
          <svg aria-hidden="true" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="m6 6 12 12M6 18 18 6" /></svg>
        </button>
      </div>
    {/each}
  </div>
    {#if hasOverflow}
      <button class="tab-scroll" type="button" aria-label="Scroll tabs right" title="Scroll tabs right"
        disabled={busy || atEnd} on:click={() => scrollTabs(1)}>
        <svg aria-hidden="true" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="m9 5 7 7-7 7" /></svg>
      </button>
    {/if}
  </div>
{/if}

{#if menuState && contextProject}
  {#key menuState}
    <ProjectTabContextMenu label={label(contextProject)}
      x={menuState.x} y={menuState.y} canCloseOthers={projects.length > 1}
      canCloseRight={contextIndex < projects.length - 1} disabled={busy}
      on:close={event => closeMenu(event.detail.restoreFocus)} on:action={event => runMenuAction(event.detail)} />
  {/key}
{/if}

<style>
  .project-tab-strip { display: flex; flex: 0 0 auto; align-items: center; min-width: 0; height: 31px; padding: 0 12px; border-bottom: 1px solid var(--border); background: var(--surface-soft); }
  .project-tabs { display: flex; flex: 1 1 auto; min-width: 0; gap: 2px; height: 100%; overflow: hidden; }
  .project-tab { display: flex; flex: 0 0 auto; align-items: center; min-width: 70px; max-width: 240px; border-bottom: 2px solid transparent; color: var(--muted); }
  .project-tab.active { border-bottom-color: var(--accent); background: var(--surface); color: var(--fg); }
  button { height: 27px; border: 0; border-radius: 0; background: transparent; color: inherit; box-shadow: none; }
  .tab-label { display: block; min-width: 0; padding: 2px 7px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 11.5px; text-align: left; }
  .tab-close { display: flex; align-items: center; justify-content: center; flex: 0 0 24px; width: 24px; padding: 0; }
  .tab-scroll { display: inline-flex; align-items: center; justify-content: center; flex: 0 0 22px; width: 22px; padding: 0; color: var(--muted); }
  .tab-scroll:disabled { opacity: .3; cursor: default; }
  button:hover:not(:disabled) { background: var(--hover); }
  .tab-label:disabled, .tab-close:disabled { opacity: .5; cursor: wait; }
  button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: -2px; }
</style>
