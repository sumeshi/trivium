import { getContext, setContext } from "svelte";
import type { WorkspaceController } from "./workspaceController";

const WORKSPACE = Symbol("trivium.workspace");

export function provideWorkspace(workspace: WorkspaceController): void {
  setContext(WORKSPACE, workspace);
}

export function useWorkspace(): WorkspaceController {
  const workspace = getContext<WorkspaceController | undefined>(WORKSPACE);
  if (!workspace) throw new Error("Investigation component requires a workspace controller.");
  return workspace;
}
