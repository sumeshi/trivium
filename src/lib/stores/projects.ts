import { derived, get, writable } from "svelte/store";
import type { Backend } from "../backend";
import type { LoadProjectResponse, ProjectSummary } from "../types";

export interface ProjectsState {
  projects: ProjectSummary[];
  selectedProjectId: string | null;
  projectDetail: LoadProjectResponse | null;
  isLoadingProjects: boolean;
  isLoadingDetail: boolean;
  creating: boolean;
  projectsLoaded: boolean;
  pendingDescription: string;
  pendingFilePath: string | null;
  pendingFileName: string;
}

interface LoadDetailOptions {
  force?: boolean;
  skipLoadingState?: boolean;
}

export function createProjectController(backend: Backend) {
  const projectCache = new Map<string, LoadProjectResponse>();

  const initialState: ProjectsState = {
    projects: [],
    selectedProjectId: null,
    projectDetail: null,
    isLoadingProjects: false,
    isLoadingDetail: false,
    creating: false,
    projectsLoaded: false,
    pendingDescription: "",
    pendingFilePath: null,
    pendingFileName: "",
  };

  const state = writable<ProjectsState>(initialState);
  const canCreateProject = derived(state, ($state) =>
    Boolean($state.pendingFilePath)
  );

  function setPendingDescription(description: string) {
    state.update((current) => ({
      ...current,
      pendingDescription: description,
    }));
  }

  function setPendingFile(path: string | null) {
    state.update((current) => ({
      ...current,
      pendingFilePath: path,
      pendingFileName: path ? extractFileName(path) : "",
    }));
  }

  function resetPending() {
    state.update((current) => ({
      ...current,
      pendingDescription: "",
      pendingFilePath: null,
      pendingFileName: "",
    }));
  }

  async function loadProjects(force = false) {
    const current = get(state);
    if (current.projectsLoaded && !force) {
      return;
    }

    state.update((value) => ({
      ...value,
      isLoadingProjects: true,
    }));

    try {
      const projects = await backend.listProjects();
      state.update((value) => {
        const stillExists = value.selectedProjectId
          ? projects.some((item) => item.meta.id === value.selectedProjectId)
          : false;

        if (!stillExists && value.selectedProjectId) {
          projectCache.delete(value.selectedProjectId);
        }

        return {
          ...value,
          projects,
          projectsLoaded: true,
          isLoadingProjects: false,
          selectedProjectId: stillExists ? value.selectedProjectId : null,
          projectDetail: stillExists ? value.projectDetail : null,
        };
      });
    } catch (error) {
      state.update((value) => ({
        ...value,
        isLoadingProjects: false,
      }));
      throw error;
    }
  }

  async function loadProjectDetail(
    projectId: string,
    options: LoadDetailOptions = {}
  ) {
    const { force = false, skipLoadingState = false } = options;

    if (!force) {
      const cached = projectCache.get(projectId);
      if (cached) {
        state.update((value) => ({
          ...value,
          selectedProjectId: projectId,
          projectDetail: cached,
          isLoadingDetail: false,
        }));
        return;
      }
    }

    if (!skipLoadingState) {
      state.update((value) => ({
        ...value,
        selectedProjectId: projectId,
        projectDetail: force ? null : value.projectDetail,
        isLoadingDetail: true,
      }));
    }

    try {
      const detail = await backend.loadProject(projectId);
      projectCache.set(projectId, detail);
      state.update((value) => ({
        ...value,
        projectDetail: detail,
        selectedProjectId: projectId,
        isLoadingDetail: false,
      }));
    } catch (error) {
      state.update((value) => ({
        ...value,
        isLoadingDetail: false,
      }));
      throw error;
    }
  }

  async function selectProject(projectId: string) {
    projectCache.delete(projectId);
    state.update((value) => ({
      ...value,
      selectedProjectId: projectId,
      projectDetail: null,
      isLoadingDetail: true,
    }));

    try {
      await loadProjectDetail(projectId, { force: true, skipLoadingState: true });
    } catch (error) {
      state.update((value) => ({
        ...value,
        isLoadingDetail: false,
      }));
      throw error;
    }
  }

  async function refreshSelected() {
    const current = get(state);
    if (current.selectedProjectId) {
      projectCache.delete(current.selectedProjectId);
      await loadProjectDetail(current.selectedProjectId, { force: true });
    }
    await loadProjects(true);
  }

  async function createProject() {
    const current = get(state);
    if (!current.pendingFilePath) {
      throw new Error("Select a CSV file first.");
    }

    state.update((value) => ({
      ...value,
      creating: true,
    }));

    try {
      const summary = await backend.createProject({
        path: current.pendingFilePath,
        description: current.pendingDescription || null,
      });

      resetPending();
      await loadProjects(true);
      await loadProjectDetail(summary.meta.id, { force: true });

      state.update((value) => ({
        ...value,
        creating: false,
      }));

      return summary;
    } catch (error) {
      state.update((value) => ({
        ...value,
        creating: false,
      }));
      throw error;
    }
  }

  async function deleteProject(projectId: string) {
    await backend.deleteProject(projectId);
    projectCache.delete(projectId);

    state.update((value) => {
      const isActive = value.selectedProjectId === projectId;
      return {
        ...value,
        selectedProjectId: isActive ? null : value.selectedProjectId,
        projectDetail: isActive ? null : value.projectDetail,
      };
    });

    await loadProjects(true);
  }

  function updateSummary(payload: {
    flagged: number;
    iocApplied: number;
    hiddenColumns: string[];
  }) {
    state.update((value) => {
      if (!value.projectDetail || !value.selectedProjectId) {
        return value;
      }

      const newMeta = {
        ...value.projectDetail.project.meta,
        flagged_records: payload.flagged,
        ioc_applied_records: payload.iocApplied,
        hidden_columns: payload.hiddenColumns,
      };

      const updatedDetail: LoadProjectResponse = {
        ...value.projectDetail,
        project: {
          meta: newMeta,
        },
        hidden_columns: payload.hiddenColumns,
      };

      projectCache.set(value.selectedProjectId, updatedDetail);

      return {
        ...value,
        projectDetail: updatedDetail,
        projects: value.projects.map((item) =>
          item.meta.id === value.selectedProjectId ? { meta: newMeta } : item
        ),
      };
    });
  }

  function extractFileName(path: string) {
    const parts = path.split(/[/\\]/);
    return parts[parts.length - 1] ?? "selected.csv";
  }

  return {
    state: { subscribe: state.subscribe },
    canCreateProject,
    loadProjects,
    loadProjectDetail,
    selectProject,
    refreshSelected,
    createProject,
    deleteProject,
    setPendingDescription,
    setPendingFile,
    resetPending,
    updateSummary,
  };
}
