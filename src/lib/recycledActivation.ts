export interface ActivationIdentity {
  projectId: string;
  rowIndex: number | null;
  columnId: string | null;
}

/** A gesture belongs to the evidence pressed, even if its DOM slot is reused. */
export function createPointerActivationGuard() {
  let pressed: ActivationIdentity | null = null;
  return {
    press(identity: ActivationIdentity) { pressed = { ...identity }; },
    permits(identity: ActivationIdentity, pointer: boolean, consume = true): boolean {
      if (!pointer) return true;
      const origin = pressed;
      if (consume) pressed = null;
      return origin === null || (origin.projectId === identity.projectId
        && origin.rowIndex === identity.rowIndex && origin.columnId === identity.columnId);
    },
  };
}

/** Transparent capture guard; child buttons keep their usual input behavior. */
export function protectRecycledActivation(
  node: HTMLElement, identity: (target: EventTarget | null) => ActivationIdentity,
) {
  const guard = createPointerActivationGuard();
  const press = (event: MouseEvent) => guard.press(identity(event.target));
  const block = (event: MouseEvent, context: boolean) => {
    if (guard.permits(identity(event.target), context ? event.button === 2 : event.detail > 0, !context)) return;
    event.preventDefault();
    event.stopImmediatePropagation();
  };
  const click = (event: MouseEvent) => block(event, false);
  const context = (event: MouseEvent) => block(event, true);
  node.addEventListener('mousedown', press, true);
  node.addEventListener('click', click, true);
  node.addEventListener('contextmenu', context, true);
  return { destroy() {
    node.removeEventListener('mousedown', press, true);
    node.removeEventListener('click', click, true);
    node.removeEventListener('contextmenu', context, true);
  } };
}
