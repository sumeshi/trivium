<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { LoadProjectResponse } from '../../types';

  export let projectDetail: LoadProjectResponse | null;
  export let onRefreshClick: () => void;

  const dispatch = createEventDispatcher();
</script>

<header class="fixed inset-x-0 top-0 z-50 border-b border-white/10 bg-slate-950/90 backdrop-blur">
  <div class="flex w-full items-center justify-between gap-3 px-4 py-3 sm:px-6">
    <div class="flex items-center gap-3">
      <button
        class="inline-flex items-center justify-center rounded-md border border-white/10 p-2 text-slate-100 transition hover:bg-white/10 focus:outline-none focus:ring-2 focus:ring-indigo-400/40"
        on:click={(event) => { event.stopPropagation(); dispatch('menuClick'); }}
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
      on:click={onRefreshClick}
    >
      ↻ <span class="hidden sm:inline">Refresh</span>
    </button>
  </div>
</header>
