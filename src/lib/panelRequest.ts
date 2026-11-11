/**
 * Whether an auxiliary panel should send its request.
 *
 * Closed panels must not read data. The function is pure so the gate can be
 * tested without mounting a Svelte component.
 */
export type PanelRequestState = {
  open: boolean;
  projectId: string;
  column: string;
};

export const shouldRequestPanel = ({
  open,
  projectId,
  column,
}: PanelRequestState): boolean => open && projectId.length > 0 && column.length > 0;
