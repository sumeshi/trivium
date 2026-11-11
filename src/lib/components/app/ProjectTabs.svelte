<script lang="ts">
  import { createEventDispatcher, onDestroy, tick } from 'svelte';
  import type { ProjectSummary } from '../../types';

  export let projects: ProjectSummary[] = [];
  export let selectedProjectId: string | null = null;
  const dispatch = createEventDispatcher<{ select: string; close: string }>();
  const label = (project: ProjectSummary) => project.meta.source_file_name || project.meta.name;
  let tabList: HTMLDivElement;
  let stripEl: HTMLDivElement;
  let hasOverflow = false;
  let atStart = true;
  let atEnd = true;
  let layoutVersion = 0;
  let disposed = false;

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
  <div class="project-tab-strip" bind:this={stripEl} use:observeStrip>
    {#if hasOverflow}
      <button class="tab-scroll" type="button" aria-label="Scroll tabs left" title="Scroll tabs left"
        disabled={atStart} on:click={() => scrollTabs(-1)}>
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
    {#if hasOverflow}
      <button class="tab-scroll" type="button" aria-label="Scroll tabs right" title="Scroll tabs right"
        disabled={atEnd} on:click={() => scrollTabs(1)}>
        <svg aria-hidden="true" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="m9 5 7 7-7 7" /></svg>
      </button>
    {/if}
  </div>
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
  button:hover { background: var(--hover); }
  button:focus-visible { outline: 2px solid var(--accent-border); outline-offset: -2px; }
</style>
