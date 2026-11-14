<script lang="ts">
  import { createEventDispatcher, onDestroy } from 'svelte';
  import { MIN_DATA_WIDTH, MAX_DATA_WIDTH } from './state';

  export let label: string;
  export let width: number;
  const dispatch = createEventDispatcher<{ resize: { width: number } }>();
  let start: { x: number; width: number } | null = null;
  let pendingWidth = width;
  let frame: number | null = null;
  const clamp = (value: number) => Math.min(MAX_DATA_WIDTH, Math.max(MIN_DATA_WIDTH, Math.round(value)));
  const publish = () => { frame = null; dispatch('resize', { width: clamp(pendingWidth) }); };
  const cleanup = () => {
    window.removeEventListener('mousemove', move);
    window.removeEventListener('mouseup', finish);
    window.removeEventListener('keydown', cancel);
    window.removeEventListener('blur', finish);
    if (frame !== null) cancelAnimationFrame(frame);
    frame = null;
    start = null;
  };
  const move = (event: MouseEvent) => {
    if (!start) return;
    pendingWidth = clamp(start.width + event.clientX - start.x);
    if (frame === null) frame = requestAnimationFrame(publish);
  };
  const finish = () => {
    if (!start) return;
    const finalWidth = pendingWidth;
    cleanup();
    dispatch('resize', { width: clamp(finalWidth) });
  };
  const cancel = (event: KeyboardEvent) => {
    if (event.key !== 'Escape' || !start) return;
    event.preventDefault();
    const originalWidth = start.width;
    cleanup();
    dispatch('resize', { width: originalWidth });
  };
  const begin = (event: MouseEvent) => {
    if (event.button !== 0) return;
    event.preventDefault();
    event.stopPropagation();
    cleanup();
    start = { x: event.clientX, width };
    pendingWidth = width;
    window.addEventListener('mousemove', move);
    window.addEventListener('mouseup', finish);
    window.addEventListener('keydown', cancel);
    window.addEventListener('blur', finish);
  };
  const keydown = (event: KeyboardEvent) => {
    if (event.altKey || event.ctrlKey || event.metaKey) return;
    const step = event.shiftKey ? 1 : 10;
    const next = event.key === 'ArrowLeft' ? width - step
      : event.key === 'ArrowRight' ? width + step
      : event.key === 'Home' ? MIN_DATA_WIDTH
      : event.key === 'End' ? MAX_DATA_WIDTH : null;
    if (next === null) return;
    event.preventDefault();
    event.stopPropagation();
    dispatch('resize', { width: clamp(next) });
  };
  onDestroy(cleanup);
</script>

<div class="column-resize-handle" role="slider"
  aria-orientation="horizontal" aria-label={`Resize column ${label}`}
  aria-valuemin={MIN_DATA_WIDTH} aria-valuemax={MAX_DATA_WIDTH} aria-valuenow={Math.round(width)}
  aria-valuetext={`${Math.round(width)} pixels`}
  title={`Resize ${label} (drag or arrow keys)`} draggable="false" tabindex="-1"
  on:mousedown={begin} on:keydown={keydown} on:click|stopPropagation></div>

<style>
  .column-resize-handle {
    position: absolute; inset: 0 0 0 auto; width: 7px; height: 100%;
    padding: 0; margin: 0; border: 0; border-radius: 0; background: transparent;
    cursor: col-resize; touch-action: none; z-index: 2;
  }
  .column-resize-handle::after {
    content: ''; position: absolute; top: 6px; bottom: 6px; right: 2px; width: 1px;
    background: var(--border); opacity: 0;
  }
  .column-resize-handle:hover::after, .column-resize-handle:focus-visible::after { opacity: 1; background: var(--accent); }
  .column-resize-handle:focus-visible { outline: 1px solid var(--accent); outline-offset: -2px; }
</style>
