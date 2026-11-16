<script lang="ts">
  import { createEventDispatcher, onDestroy, tick } from 'svelte';
  import type { ProjectSummary } from '../../types';
  import type { ExtractTab } from '../../extractTypes';

  export let projects: ProjectSummary[] = [];
  export let selectedProjectId: string | null = null;
  export let extractions: ExtractTab[] = [];
  export let selectedExtractId: string | null = null;
  const dispatch = createEventDispatcher<{ select: string; close: string; selectExtract: string; closeExtract: string }>();
  const label = (project: ProjectSummary) => project.meta.source_file_name || project.meta.name;
  $: tabs = [
    ...projects.map(project => ({ id: project.meta.id, kind: 'project' as const, label: label(project) })),
    ...extractions.map(tab => ({ id: tab.id, kind: 'extract' as const, label: `${tab.result.format.name} — ${tab.sourceName}` })),
  ];
  const tabId = (tab: { id: string; kind: string }) => `${tab.kind}-tab-${tab.id}`;
  const active = (tab: { id: string; kind: string }, projectId: string | null, extractId: string | null) =>
    tab.kind === 'extract' ? extractId === tab.id : !extractId && projectId === tab.id;
  const select = (tab: typeof tabs[number]) => dispatch(tab.kind === 'extract' ? 'selectExtract' : 'select', tab.id);
  const close = (tab: typeof tabs[number]) => dispatch(tab.kind === 'extract' ? 'closeExtract' : 'close', tab.id);
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
    if (!tabList) return;
    const selected = tabs.find(tab => active(tab, selectedProjectId, selectedExtractId));
    const tab = selected && document.getElementById(tabId(selected))?.parentElement;
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
  $: layoutKey = JSON.stringify([selectedProjectId, selectedExtractId, tabs.map(tab => [tab.kind, tab.id, tab.label])]);
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
      close(tabs[index]);
      return;
    }
    const next = event.key === 'ArrowRight' ? (index + 1) % tabs.length
      : event.key === 'ArrowLeft' ? (index + tabs.length - 1) % tabs.length
      : event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : -1;
    if (next < 0) return;
    event.preventDefault();
    const buttons = (event.currentTarget as HTMLElement).closest('[role=tablist]')?.querySelectorAll<HTMLButtonElement>('[role=tab]');
    buttons?.[next]?.focus();
    select(tabs[next]);
  };
</script>

{#if tabs.length > 0}
  <div class="project-tab-strip" bind:this={stripEl} use:observeStrip>
    {#if hasOverflow}
      <button class="tab-scroll" type="button" aria-label="Scroll tabs left" title="Scroll tabs left"
        disabled={atStart} on:click={() => scrollTabs(-1)}>
        <svg aria-hidden="true" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="m15 5-7 7 7 7" /></svg>
      </button>
    {/if}
  <div class="project-tabs" bind:this={tabList} role="tablist" aria-label="Open projects"
    on:wheel|nonpassive={handleWheel} on:scroll={updateScrollEdges}>
    {#each tabs as tab, index (`${tab.kind}:${tab.id}`)}
      <div class="project-tab" class:active={active(tab, selectedProjectId, selectedExtractId)} role="presentation">
        <button
          class="tab-label"
          type="button"
          role="tab"
          id={tabId(tab)}
          aria-selected={active(tab, selectedProjectId, selectedExtractId)}
          aria-controls={`${tab.kind === 'project' ? 'project' : 'extract'}-panel-${tab.id}`}
          tabindex={active(tab, selectedProjectId, selectedExtractId) ? 0 : -1}
          title={tab.kind === 'extract' ? `Extract: ${tab.label}` : tab.label}
          on:click={() => select(tab)}
          on:keydown={(event) => navigate(event, index)}
        >{#if tab.kind === 'extract'}<span aria-hidden="true">⌕ </span>{/if}{tab.label}</button>
        <button
          class="tab-close"
          type="button"
          aria-label={tab.kind === 'extract' ? `Close extraction ${tab.label}` : `Close ${tab.label}`}
          title={tab.kind === 'extract' ? 'Close extraction result' : 'Close tab; saved project and annotations are kept'}
          tabindex={active(tab, selectedProjectId, selectedExtractId) ? 0 : -1}
          on:click={() => close(tab)}
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
