<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount, tick } from 'svelte';
  import { FLAG_OPTIONS } from './state';
  import type { FlagSymbol } from '../../types';

  export let value: FlagSymbol = 'safe';
  export let disabled = false;

  const dispatch = createEventDispatcher<{ change: FlagSymbol }>();

  let expanded = false;
  let rootEl: HTMLDivElement | null = null;
  let triggerEl: HTMLButtonElement | null = null;
  let menuPosition: { top: number; left: number; width: number } | null = null;

  const toggleExpanded = async (event?: Event) => {
    event?.stopPropagation();
    if (disabled) {
      return;
    }
    expanded = !expanded;
    if (expanded) {
      await updateMenuPosition();
      attachGlobalPositionListeners();
    } else {
      detachGlobalPositionListeners();
    }
  };

  const closeMenu = () => {
    expanded = false;
    detachGlobalPositionListeners();
  };

  const selectValue = (next: FlagSymbol) => {
    if (next !== value) {
      dispatch('change', next);
    }
    expanded = false;
    detachGlobalPositionListeners();
  };

  const handleDocumentClick = (event: MouseEvent) => {
    if (!expanded || !rootEl) {
      return;
    }
    const target = event.target as Node;
    if (!rootEl.contains(target)) {
      expanded = false;
      detachGlobalPositionListeners();
    }
  };

  const updateMenuPosition = async () => {
    await tick();
    if (!triggerEl) {
      return;
    }
    const rect = triggerEl.getBoundingClientRect();
    menuPosition = {
      top: rect.bottom + 6,
      left: rect.left,
      width: rect.width,
    };
  };

  const handleWindowScroll = () => {
    if (!expanded) {
      return;
    }
    updateMenuPosition();
  };

  const attachGlobalPositionListeners = () => {
    window.addEventListener('scroll', handleWindowScroll, true);
    window.addEventListener('resize', handleWindowScroll);
  };

  const detachGlobalPositionListeners = () => {
    window.removeEventListener('scroll', handleWindowScroll, true);
    window.removeEventListener('resize', handleWindowScroll);
  };

  const handleKeydown = (event: KeyboardEvent) => {
    if (disabled) {
      return;
    }
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      const direction = event.key === 'ArrowDown' ? 1 : -1;
      const currentIndex = FLAG_OPTIONS.findIndex((option) => option.value === value);
      const nextIndex =
        (currentIndex + direction + FLAG_OPTIONS.length) % FLAG_OPTIONS.length;
      selectValue(FLAG_OPTIONS[nextIndex].value);
      return;
    }
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      toggleExpanded();
      return;
    }
    if (event.key === 'Escape' && expanded) {
      event.preventDefault();
      expanded = false;
      detachGlobalPositionListeners();
    }
  };

  onMount(() => {
    document.addEventListener('click', handleDocumentClick);
  });

  onDestroy(() => {
    document.removeEventListener('click', handleDocumentClick);
    detachGlobalPositionListeners();
  });

  $: activeOption =
    FLAG_OPTIONS.find((option) => option.value === value) ?? FLAG_OPTIONS[0];
</script>

<div
  class={`flag-select flag-${value}`}
  data-flag={value}
  bind:this={rootEl}
>
  <button
    type="button"
    class={`flag-select-trigger flag-${value}`}
    aria-haspopup="listbox"
    aria-expanded={expanded}
    aria-label="Select flag severity"
    on:click|stopPropagation={toggleExpanded}
    on:keydown={handleKeydown}
    disabled={disabled}
    bind:this={triggerEl}
  >
    <span class="flag-select-label">
      <span class="flag-icon">{activeOption.hint}</span>
      <span class="flag-label">{activeOption.label}</span>
    </span>
    <span class="flag-caret" aria-hidden="true">▾</span>
  </button>
  {#if expanded && menuPosition}
    <ul
      class="flag-select-menu"
      role="listbox"
      aria-label="Flag options"
      style={`top:${menuPosition.top}px; left:${menuPosition.left}px; width:${menuPosition.width}px;`}
    >
      {#each FLAG_OPTIONS as option}
        <li>
          <button
            type="button"
            role="option"
            aria-selected={option.value === value}
            class={`flag-option flag-${option.value} ${option.value === value ? 'selected' : ''}`}
            on:click|stopPropagation={() => selectValue(option.value)}
            disabled={disabled}
          >
            <span class="flag-icon">{option.hint}</span>
            <span class="flag-label">{option.label}</span>
          </button>
        </li>
      {/each}
    </ul>
  {/if}
</div>
