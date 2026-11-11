import { derived, get, writable } from "svelte/store";
import type { Backend } from "../backend";
import type { LoadProjectResponse, ProjectSummary } from "../types";

export interface ProjectsState {
  projects: ProjectSummary[];
  openProjectIds: string[];
  selectedProjectId: string | null;
  projectDetail: LoadProjectResponse | null;
  isLoadingProjects: boolean;
  isLoadingDetail: boolean;
  creating: boolean;
  projectsLoaded: boolean;
  pendingDescription: string;
  pendingFilePath: string | null;
  pendingFileName: string;
  pendingDelimiter: string;
}

export function createProjectController(backend: Backend) {
  // Only the active detail is retained; inactive tabs have IDs and UI snapshots.
  let detailGeneration = 0;
  let listGeneration = 0;
  const state = writable<ProjectsState>({
    projects: [], openProjectIds: [], selectedProjectId: null, projectDetail: null,
    isLoadingProjects: false, isLoadingDetail: false, creating: false,
    projectsLoaded: false, pendingDescription: "", pendingFilePath: null,
    pendingFileName: "", pendingDelimiter: "",
  });
  const canCreateProject = derived(state, ($state) => Boolean($state.pendingFilePath));

  function setPendingDescription(pendingDescription: string) {
    state.update((value) => ({ ...value, pendingDescription }));
  }

  function setPendingDelimiter(pendingDelimiter: string) {
    state.update((value) => ({ ...value, pendingDelimiter }));
  }

  function setPendingFile(pendingFilePath: string | null) {
    state.update((value) => ({
      ...value, pendingFilePath,
      pendingFileName: pendingFilePath?.split(/[/\\]/).at(-1) ?? "",
    }));
  }

  function resetPending() {
    state.update((value) => ({
      ...value, pendingDescription: "", pendingFilePath: null,
      pendingFileName: "", pendingDelimiter: "",
    }));
  }

  async function loadProjects(force = false) {
    if (get(state).projectsLoaded && !force) return;
    const generation = ++listGeneration;
    state.update((value) => ({ ...value, isLoadingProjects: true }));
    try {
      const projects = await backend.listProjects();
      if (generation !== listGeneration) return;
      const existing = new Set(projects.map((project) => project.meta.id));
      const current = get(state);
      const openProjectIds = current.openProjectIds.filter((id) => existing.has(id));
      const selectedExists = !!current.selectedProjectId && existing.has(current.selectedProjectId);
      const nextId = selectedExists ? current.selectedProjectId : openProjectIds[0] ?? null;
      if (nextId !== current.selectedProjectId) detailGeneration += 1;
      state.update((value) => ({
        ...value, projects, openProjectIds, projectsLoaded: true, isLoadingProjects: false,
        selectedProjectId: nextId,
        projectDetail: selectedExists ? value.projectDetail : null,
        isLoadingDetail: selectedExists ? value.isLoadingDetail : !!nextId,
      }));
      if (!selectedExists && nextId) await loadProjectDetail(nextId);
    } catch (error) {
      if (generation !== listGeneration) return;
      state.update((value) => ({ ...value, isLoadingProjects: false }));
      throw error;
    }
  }

  async function loadProjectDetail(projectId: string) {
    const generation = ++detailGeneration;
    state.update((value) => ({
      ...value, selectedProjectId: projectId, projectDetail: null, isLoadingDetail: true,
      openProjectIds: value.openProjectIds.includes(projectId)
        ? value.openProjectIds : [...value.openProjectIds, projectId],
    }));
    const isCurrent = () => generation === detailGeneration && get(state).selectedProjectId === projectId;
    try {
      const loaded = await backend.loadProject(projectId);
      if (!isCurrent()) return;
      // ProjectView intentionally fetches fresh pages. Keeping the import seed
      // here would retain another row payload without ever displaying it.
      const detail = { ...loaded, initial_rows: [] };
      state.update((value) => ({ ...value, projectDetail: detail, isLoadingDetail: false }));
    } catch (error) {
      if (!isCurrent()) return;
      state.update((value) => ({ ...value, projectDetail: null, isLoadingDetail: false }));
      throw error;
    }
  }

  async function selectProject(projectId: string) {
    const current = get(state);
    if (current.selectedProjectId === projectId && (current.projectDetail || current.isLoadingDetail)) return;
    await loadProjectDetail(projectId);
  }

  /** Closing a tab only changes this workspace; it never deletes a project. */
  async function closeProject(projectId: string) {
    const current = get(state);
    const index = current.openProjectIds.indexOf(projectId);
    if (index < 0) return;
    const openProjectIds = current.openProjectIds.filter((id) => id !== projectId);
    if (current.selectedProjectId !== projectId) {
      state.update((value) => ({ ...value, openProjectIds }));
      return;
    }
    detailGeneration += 1;
    const nextId = openProjectIds[Math.min(index, openProjectIds.length - 1)] ?? null;
    state.update((value) => ({
      ...value, openProjectIds, selectedProjectId: nextId,
      projectDetail: null, isLoadingDetail: !!nextId,
    }));
    if (nextId) await loadProjectDetail(nextId);
  }

  async function refreshSelected() {
    const projectId = get(state).selectedProjectId;
    if (projectId) await loadProjectDetail(projectId);
    await loadProjects(true);
  }

  async function createProject() {
    const current = get(state);
    if (!current.pendingFilePath) throw new Error("Select a CSV, JSON, JSONL, or NDJSON file first.");
    const navigationAtStart = detailGeneration;
    state.update((value) => ({ ...value, creating: true }));
    try {
      const summary = await backend.createProject({
        path: current.pendingFilePath, description: current.pendingDescription || null,
        delimiter: current.pendingDelimiter,
      });
      resetPending();
      await loadProjects(true);
      // A user may have switched tabs while import was running. Its late
      // response may add the new tab, but must not replace that newer choice.
      state.update((value) => ({
        ...value, openProjectIds: value.openProjectIds.includes(summary.meta.id)
          ? value.openProjectIds : [...value.openProjectIds, summary.meta.id],
      }));
      if (navigationAtStart === detailGeneration) await loadProjectDetail(summary.meta.id);
      state.update((value) => ({ ...value, creating: false }));
      return summary;
    } catch (error) {
      state.update((value) => ({ ...value, creating: false }));
      throw error;
    }
  }

  async function deleteProject(projectId: string) {
    await backend.deleteProject(projectId);
    await closeProject(projectId);
    await loadProjects(true);
  }

  function updateSummary(payload: {
    projectId?: string; flagged: number; iocApplied: number; hiddenColumns: string[];
  }) {
    state.update((value) => {
      if (!value.projectDetail || !value.selectedProjectId ||
        (payload.projectId && payload.projectId !== value.selectedProjectId)) return value;
      const meta = {
        ...value.projectDetail.project.meta, flagged_records: payload.flagged,
        ioc_applied_records: payload.iocApplied, hidden_columns: payload.hiddenColumns,
      };
      return {
        ...value,
        projectDetail: { ...value.projectDetail, project: { meta }, hidden_columns: payload.hiddenColumns },
        projects: value.projects.map((item) => item.meta.id === meta.id ? { meta } : item),
      };
    });
  }

  return {
    state: { subscribe: state.subscribe }, canCreateProject, loadProjects,
    loadProjectDetail, selectProject, closeProject, refreshSelected, createProject,
    deleteProject, setPendingDescription, setPendingDelimiter, setPendingFile,
    resetPending, updateSummary,
  };
}
