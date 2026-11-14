<svelte:options immutable={true} />

<script lang="ts">
  import { beforeUpdate } from 'svelte';
  import type { CachedRow } from './state';
  import { ROW_HEIGHT, PAGE_SIZE, FLAG_OPTIONS, normalizeFlag } from './state';
  import type { TableColumn } from '../../columnWindow';
  import { isTabKey } from '../../keyboard';
  import { protectRecycledActivation } from '../../recycledActivation';
  import { formatIsoCell } from '../../dateDisplay';
  import { tagSearch } from '../../tagSearch';
  import RowTagsMenu from './RowTagsMenu.svelte';
  import { theme } from '../../theme';
  import { displayRowColor } from '../../skin';
  import { useWorkspace } from '../../workspaceContext';

  // Functions are stable. This row never subscribes to scroll or page stores.
  const { setFlag, editMemo, openCell, handleCellKeydown, toggleRowSelection, columnFormats, search } = useWorkspace();
  export let row: CachedRow | null;
  export let position: number;
  export let projectId: string;
  export let selected = false;
  export let largePage = false;
  export let showLargeNotice = false;
  export let columns: readonly TableColumn[];
  export let columnCount: number;
  export let gridTemplate: string;
  export let frozenOffsets: ReadonlyMap<string, number>;
  export let frozenPan: number;
  export let lastFrozen: string | null;
  export let focusColumn: (index: number) => void;
  export let openCellMenu: (event: MouseEvent, row: CachedRow, column: string) => void;
  export let columnTab: (event: KeyboardEvent, index: number, position: number) => void;
  const activationIdentity = (target: EventTarget | null) => ({
    projectId, rowIndex: row?.row_index ?? null,
    columnId: target instanceof Element ? target.closest<HTMLElement>('[data-column-id]')?.dataset.columnId ?? null : null,
  });

  let rowElement: HTMLDivElement | null = null;
  let memoButton: HTMLButtonElement | null = null;
  let tagButton: HTMLButtonElement | null = null;
  let tagMenu: { projectId: string; rowIndex: number; tags: string[]; trigger: HTMLButtonElement } | null = null;
  const ownsTagMenu = (owner: NonNullable<typeof tagMenu>) =>
    owner.projectId === projectId && owner.rowIndex === row?.row_index;
  const toggleTags = () => {
    if (tagMenu) { tagMenu = null; return; }
    if (!row?.tags.length || !tagButton) return;
    tagMenu = { projectId, rowIndex: row.row_index, tags: [...row.tags], trigger: tagButton };
  };
  const closeTags = (detail: { restoreFocus: boolean; tab?: KeyboardEvent }) => {
    const owner = tagMenu;
    tagMenu = null;
    if (!owner || !ownsTagMenu(owner)) return;
    if (detail.tab) {
      if (detail.tab.shiftKey) memoButton?.focus({ preventScroll: true });
      else if (columnCount === 0) owner.trigger.focus({ preventScroll: true });
      else columnTab(detail.tab, -1, position);
    } else if (detail.restoreFocus && owner.trigger.isConnected) owner.trigger.focus({ preventScroll: true });
  };
  const searchTag = (tag: string) => {
    const owner = tagMenu;
    tagMenu = null;
    if (!owner || !ownsTagMenu(owner) || !row?.tags.includes(tag)) return;
    search.set(tagSearch(tag));
    document.querySelector<HTMLInputElement>('input[aria-label="Search rows"]')?.focus();
  };
  let renderedRowIndex = row?.row_index ?? null;
  let renderedProjectId = projectId;
  beforeUpdate(() => {
    const nextRowIndex = row?.row_index ?? null;
    if (tagMenu && (!ownsTagMenu(tagMenu) || tagMenu.tags.length !== row?.tags.length
      || tagMenu.tags.some((tag, index) => tag !== row?.tags[index]))) tagMenu = null;
    if ((nextRowIndex !== renderedRowIndex || projectId !== renderedProjectId)
      && document.activeElement instanceof HTMLElement && rowElement?.contains(document.activeElement)) {
      // A recycled control must never apply an annotation to a new evidence row.
      document.activeElement.blur();
    }
    renderedRowIndex = nextRowIndex;
    renderedProjectId = projectId;
  });
</script>

<div
  class="data-row"
  bind:this={rowElement}
  use:protectRecycledActivation={activationIdentity}
  data-position={position}
  data-row-index={row?.row_index}
  class:alt-row={position % 2 === 1}
  class:loading={!row && !largePage}
  class:bookmarked={row?.bookmarked === true}
  style={`grid-template-columns: ${gridTemplate}; --row-height: ${ROW_HEIGHT}px;${
    row?.color
      ? ` --row-color: ${displayRowColor(row.color, $theme)};`
      : ''
  }`}
>
  {#if largePage}
    <div class="cell select-cell sticky sticky-select" aria-hidden="true"></div>
    <div class="cell index sticky sticky-index" aria-label="Original row not loaded">…</div>
    {#if position % PAGE_SIZE === 0 || showLargeNotice}
      <div
        class="cell large-page-notice"
        title="This page exceeds the page-cache byte limit and is not retained in memory."
      >
        Page too large to cache. Scroll to another page or narrow the results.
      </div>
    {:else}
      <div class="large-page-blank" aria-hidden="true"></div>
    {/if}
  {:else}
    <div class="cell select-cell sticky sticky-select">
      {#if row}
        <input
          type="checkbox"
          checked={selected}
          on:keydown={(event) => {
            if (isTabKey(event) && event.shiftKey) columnTab(event, columnCount, position - 1);
          }}
          on:change={() => row && toggleRowSelection(row.row_index)}
          aria-label={`Select row ${row.row_index + 1}`}
        />
      {/if}
    </div>
    <div class="cell index sticky sticky-index" title={row?.bookmarked ? 'Bookmarked row' : undefined}>{row ? row.row_index + 1 : '…'}</div>
    <div class="cell flag sticky sticky-flag">
      {#if row}
        <div class="flag-buttons">
          {#each FLAG_OPTIONS as option}
            <button
              type="button"
              class:selected={row ? normalizeFlag(row.flag) === option.value : false}
              class:flag-safe={option.value === 'safe'}
              class:flag-suspicious={option.value === 'suspicious'}
              class:flag-critical={option.value === 'critical'}
              title={option.label}
              aria-label={option.label}
              on:click={() => row && setFlag(row, option.value)}
            >
              {option.hint}
            </button>
          {/each}
        </div>
      {:else}
        <div class="flag-buttons loading-placeholder">…</div>
      {/if}
    </div>
    <div class="cell memo-cell sticky sticky-memo"
      class:frozenDataEdge={frozenOffsets.size === 0}
    >
    <button
      class="memo-button"
      bind:this={memoButton}
      class:has-memo={Boolean(row?.memo?.trim())}
      aria-label={row?.memo?.trim() ? `Edit note for row ${row.row_index + 1}: ${row.memo}` : row ? `Add note for row ${row.row_index + 1}` : 'Loading note'}
      on:click={() =>
        row &&
        projectId &&
        editMemo(row, projectId)}
      title={
        row
          ? row.memo && row.memo.trim().length
            ? row.memo
            : 'Add note'
          : 'Loading…'
      }
      disabled={!row}
      on:keydown={(event) => {
        if (isTabKey(event) && !event.shiftKey && !row?.tags.length) columnTab(event, -1, position);
      }}
    >
      {#if row && row.memo && row.memo.trim().length}
        <svg class="memo-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m16 3 5 5-12 12-6 1 1-6Z"/><path d="m14 5 5 5"/></svg>
        <span class="memo-text">{row.memo}</span>
      {:else if row}
        <svg class="memo-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m16 3 5 5-12 12-6 1 1-6Z"/><path d="m14 5 5 5"/></svg>
        <span class="memo-placeholder">Add note</span>
      {:else}
        <span class="memo-placeholder">Loading…</span>
      {/if}
    </button>
    {#if row && row.tags.length > 0}
      <button type="button" class="row-tags-button" bind:this={tagButton}
        aria-label={`Tags for row ${row.row_index + 1} (${row.tags.length})`}
        aria-haspopup="menu" aria-expanded={Boolean(tagMenu)}
        aria-controls={tagMenu ? `row-tags-${projectId}-${row.row_index}` : undefined}
        title="Show tags"
        on:click={toggleTags}
        on:keydown={(event) => { if (isTabKey(event) && !event.shiftKey) columnTab(event, -1, position); }}>
        <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round" aria-hidden="true"><path d="M20 13 11 22 2 13V2h11l9 9Z" /><circle cx="7.5" cy="7.5" r="1" /></svg>
        <span>{row.tags.length}</span>
      </button>
    {/if}
    </div>
    {#each columns as column (column.id)}
      {@const cellValue =
        row && column.id in row.displayCache
          ? row.displayCache[column.id]
          : null}
      {@const previewValue =
        row && column.id in row.previewCache
          ? row.previewCache[column.id]
          : ''}
      {@const formatted = $columnFormats[column.id] === 'iso' && row ? formatIsoCell(row.cells[column.id]) : null}
      <button
        type="button"
        class="cell"
        class:sticky={column.frozen}
        class:stickyData={column.frozen}
        class:lastFrozenData={lastFrozen === column.id}
        class:datetime-error={Boolean(formatted?.error)}
        data-column-id={column.id}
        data-frozen={column.frozen}
        style={`grid-column: ${column.index + 5};${column.frozen ? ` --sticky-data-left: ${(frozenOffsets.get(column.id) ?? 0) - frozenPan}px;` : ''}`}
        on:focus={() => focusColumn(column.index)}
        on:mousedown={(event) => {
          // A right-click must not pan a partly visible column;
          // its queued scroll event would dismiss the new menu.
          if (event.button === 2) event.preventDefault();
        }}
        title={
          !row || cellValue === null
            ? 'Loading…'
            : formatted?.error ? `${cellValue}\n${formatted.error}` : cellValue
        }
        on:click={() => {
          if (!row || !projectId) return;
          openCell(projectId, column.id, previewValue, row);
        }}
        on:contextmenu={(event) => {
          if (!row) return;
          openCellMenu(event, row, column.id);
        }}
        on:keydown={(event) => {
          columnTab(event, column.index, position);
          if (event.defaultPrevented || !row || !projectId) return;
          handleCellKeydown(
            event,
            projectId,
            column.id,
            previewValue,
            row
          );
        }}
        disabled={!row}
      >
        <span class="cell-text">
          {#if !row || cellValue === null}
            Loading…
          {:else}
            {formatted?.text ?? cellValue}
          {/if}
        </span>
      </button>
    {/each}
  {/if}
</div>

{#if tagMenu}
  {#key `${tagMenu.projectId}:${tagMenu.rowIndex}`}
    <RowTagsMenu projectId={tagMenu.projectId} rowIndex={tagMenu.rowIndex} tags={tagMenu.tags}
      trigger={tagMenu.trigger} menuId={`row-tags-${tagMenu.projectId}-${tagMenu.rowIndex}`}
      on:close={(event) => closeTags(event.detail)} on:search={(event) => searchTag(event.detail.tag)} />
  {/key}
{/if}
