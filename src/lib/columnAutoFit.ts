/** Fit the text already painted in the viewport; never fetch hidden row values. */
export function visibleColumnWidth(
  viewport: HTMLElement,
  heading: HTMLElement,
  columnId: string | null,
  minimum: number,
  maximum: number,
): number {
  const range = heading.ownerDocument.createRange();
  const pixels = (value: string) => Number.parseFloat(value) || 0;
  const box = (element: HTMLElement) => {
    const style = getComputedStyle(element);
    return pixels(style.paddingLeft) + pixels(style.paddingRight)
      + pixels(style.borderLeftWidth) + pixels(style.borderRightWidth);
  };
  const gap = (element: HTMLElement) => pixels(getComputedStyle(element).columnGap);
  const text = (element: Element | null): number => {
    if (!element) return 0;
    range.selectNodeContents(element);
    return range.getBoundingClientRect().width;
  };
  const outerWidth = (element: HTMLElement | null) => {
    if (!element) return 0;
    const style = getComputedStyle(element);
    return element.getBoundingClientRect().width + pixels(style.marginLeft) + pixels(style.marginRight);
  };

  const sort = heading.querySelector<HTMLElement>('.column-sort');
  const title = heading.querySelector<HTMLElement>('.column-title');
  const kind = heading.querySelector('.column-type');
  const indicator = heading.querySelector('.sort-indicator');
  let width = box(heading) + text(heading.querySelector('.header-label,.note-label')) + text(kind);
  if (title && kind) width += gap(title);
  const actions = heading.querySelector<HTMLElement>('.column-actions-trigger');
  if (sort) width += box(sort) + text(indicator) + (indicator ? gap(sort) : 0)
    + outerWidth(actions) + (actions ? gap(heading) : 0);

  const bounds = viewport.getBoundingClientRect();
  const top = bounds.top + viewport.clientTop;
  const bottom = top + viewport.clientHeight;
  for (const row of viewport.querySelectorAll<HTMLElement>('.data-row:not(.loading)[data-row-index]')) {
    const rowBounds = row.getBoundingClientRect();
    if (rowBounds.bottom <= top || rowBounds.top >= bottom) continue;
    if (columnId !== null) {
      const cell = [...row.querySelectorAll<HTMLElement>('[data-column-id]')]
        .find(element => element.dataset.columnId === columnId);
      if (cell) width = Math.max(width, text(cell.querySelector('.cell-text')) + box(cell));
    } else {
      const cell = row.querySelector<HTMLElement>('.memo-cell');
      const button = cell?.querySelector<HTMLElement>('.memo-button');
      if (!cell || !button) continue;
      const label = button.querySelector('.memo-text,.memo-placeholder');
      const icon = button.querySelector<HTMLElement>('.memo-icon');
      const tags = cell.querySelector<HTMLElement>('.row-tags-button');
      const content = text(label) + outerWidth(icon) + (icon && label ? gap(button) : 0);
      width = Math.max(width, box(cell) + box(button) + content + outerWidth(tags) + (tags ? gap(cell) : 0));
    }
  }
  // A small rounding allowance keeps fractional glyph bounds off the ellipsis edge.
  return Math.min(maximum, Math.max(minimum, Math.ceil(width + 2)));
}
