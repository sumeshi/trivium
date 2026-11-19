import { derived, get, writable } from "svelte/store";
import { createOwnedRequests } from "../workspaceRequests.ts";
import { errorMessage } from "../errors.ts";
import type { Backend } from "../backend";
import type { IocEntry, LoadProjectResponse, ProjectSummary } from "../types";

export interface ProjectsState {
  projects: ProjectSummary[];
  /** Generated CSVs reuse the normal viewer but never enter Recent. */
  temporaryProjectIds: string[];
  openProjectIds: string[];
  selectedProjectId: string | null;
  projectDetail: LoadProjectResponse | null;
  isLoadingProjects: boolean;
  projectsError: string;
  isLoadingDetail: boolean;
  creating: boolean;
  projectsLoaded: boolean;
  pendingDescription: string;
  pendingFilePaths: string[];
  pendingDelimiter: string;
  pendingRestoreAnnotations: boolean;
  importProgress: { index: number; total: number; fileName: string } | null;
  importFailures: ProjectImportFailure[];
}

export interface ProjectImportFailure {
  path: string;
  fileName: string;
  message: string;
}

export interface ProjectImportResult {
  imported: ProjectSummary[];
  failures: ProjectImportFailure[];
  openError?: string;
}

const fileName = (path: string) => path.split(/[/\\]/).at(-1) ?? path;
const isCsv = (path: string) => /\.csv$/i.test(path);

export function createProjectController(backend: Backend) {
  // Only the active detail is retained; inactive tabs have IDs and UI snapshots.
  let disposed = false;
  const reads = createOwnedRequests();
  let detailGeneration = 0;
  let listGeneration = 0;
  const deletingProjects = new Set<string>();
  const temporaryProjects = new Map<string, ProjectSummary>();
  const state = writable<ProjectsState>({
    projects: [], temporaryProjectIds: [], openProjectIds: [], selectedProjectId: null, projectDetail: null,
    isLoadingProjects: false, projectsError: "", isLoadingDetail: false, creating: false,
    projectsLoaded: false, pendingDescription: "", pendingFilePaths: [],
    pendingDelimiter: "", pendingRestoreAnnotations: false,
    importProgress: null, importFailures: [],
  });
  const canCreateProject = derived(state, ($state) => $state.pendingFilePaths.length > 0);

  function setPendingDescription(pendingDescription: string) {
    state.update((value) => ({ ...value, pendingDescription }));
  }

  function setPendingDelimiter(pendingDelimiter: string) {
    state.update((value) => ({ ...value, pendingDelimiter }));
  }

  function setPendingRestoreAnnotations(restore: boolean) {
    state.update((value) => ({ ...value,
      pendingRestoreAnnotations: restore && value.pendingFilePaths.some(isCsv),
    }));
  }

  function setPendingFiles(paths: string[]) {
    const pendingFilePaths = [...new Set(paths.filter((path) => path.length > 0))];
    state.update((value) => ({
      ...value, pendingFilePaths,
      pendingRestoreAnnotations: false, importFailures: [],
    }));
  }

  function resetPending() {
    state.update((value) => ({
      ...value, pendingDescription: "", pendingFilePaths: [],
      pendingDelimiter: "", pendingRestoreAnnotations: false, importFailures: [],
    }));
  }

  async function loadProjects(force = false) {
    if (disposed) return;
    if (get(state).projectsLoaded && !force) return;
    const generation = ++listGeneration;
    state.update((value) => ({ ...value, isLoadingProjects: true, projectsError: "" }));
    let savedProjects: ProjectSummary[];
    try {
      savedProjects = await backend.listProjects();
    } catch (error) {
      if (disposed || generation !== listGeneration) return;
      state.update((value) => ({ ...value, isLoadingProjects: false,
        projectsError: errorMessage(error, "Could not load saved projects.") }));
      throw error;
    }
    if (disposed || generation !== listGeneration) return;
    const projects = [...savedProjects.filter(project => !temporaryProjects.has(project.meta.id)), ...temporaryProjects.values()];
    const existing = new Set(projects.map((project) => project.meta.id));
    const current = get(state);
    const openProjectIds = current.openProjectIds.filter((id) => existing.has(id));
    const selectedExists = !!current.selectedProjectId && existing.has(current.selectedProjectId);
    const nextId = selectedExists ? current.selectedProjectId : openProjectIds[0] ?? null;
    if (nextId !== current.selectedProjectId) {
      detailGeneration += 1;
      reads.cancelAll();
    }
    state.update((value) => ({
      ...value, projects, openProjectIds, projectsLoaded: true, isLoadingProjects: false,
      selectedProjectId: nextId,
      projectDetail: selectedExists ? value.projectDetail : null,
      isLoadingDetail: selectedExists ? value.isLoadingDetail : !!nextId,
    }));
    if (!selectedExists && nextId) await loadProjectDetail(nextId);
  }

  async function loadProjectDetail(projectId: string) {
    if (disposed) return;
    const generation = ++detailGeneration;
    state.update((value) => ({
      ...value, selectedProjectId: projectId, projectDetail: null, isLoadingDetail: true,
      openProjectIds: value.openProjectIds.includes(projectId)
        ? value.openProjectIds : [...value.openProjectIds, projectId],
    }));
    const isCurrent = () => !disposed && generation === detailGeneration && get(state).selectedProjectId === projectId;
    try {
      const loaded = await reads.run("detail", backend, (requestId) => backend.loadProject(projectId, requestId));
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
    if (deletingProjects.has(projectId)) throw new Error("This project is being deleted. Wait for it to finish.");
    const current = get(state);
    if (current.selectedProjectId === projectId && (current.projectDetail || current.isLoadingDetail)) return;
    await loadProjectDetail(projectId);
  }

  /** A generated CSV is already imported; opening it needs no second import. */
  async function adoptTemporaryProject(summary: ProjectSummary, activate = true) {
    const id = summary.meta.id;
    if (disposed) { await backend.deleteProject(id); return; }
    const snapshot = structuredClone(summary);
    temporaryProjects.set(id, snapshot);
    // A list read started before adoption must not prune the new tab.
    listGeneration += 1;
    state.update(value => ({ ...value, projects: [snapshot, ...value.projects.filter(project => project.meta.id !== id)],
      temporaryProjectIds: [...temporaryProjects.keys()], isLoadingProjects: false,
      openProjectIds: value.openProjectIds.includes(id) ? value.openProjectIds : [...value.openProjectIds, id] }));
    if (activate) await selectProject(id);
  }

  const forgetTemporaryProject = (projectId: string) => {
    if (!temporaryProjects.delete(projectId)) return;
    state.update(value => ({ ...value, temporaryProjectIds: [...temporaryProjects.keys()],
      projects: value.projects.filter(project => project.meta.id !== projectId) }));
  };

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
    reads.cancelAll();
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

  async function createProjects(canActivate: () => boolean = () => true): Promise<ProjectImportResult> {
    const current = get(state);
    if (!current.pendingFilePaths.length) throw new Error("Select a CSV, JSON, JSONL, or NDJSON file first.");
    if (current.creating) throw new Error("Files are already being imported.");
    const navigationAtStart = detailGeneration;
    const paths = [...current.pendingFilePaths];
    const result: ProjectImportResult = { imported: [], failures: [] };
    state.update((value) => ({ ...value, creating: true, importFailures: [] }));
    try {
      for (const [index, path] of paths.entries()) {
        if (disposed) break;
        state.update((value) => ({ ...value, importProgress: { index: index + 1, total: paths.length, fileName: fileName(path) } }));
        try {
          const summary = await backend.createProject({
            path, description: current.pendingDescription || null,
            delimiter: isCsv(path) ? current.pendingDelimiter : "",
            restoreAnnotations: current.pendingRestoreAnnotations && isCsv(path),
          });
          result.imported.push(summary);
          if (disposed) continue;
          // A list read started before this import may omit its new project.
          // Supersede it and refresh the complete list once the batch finishes.
          listGeneration += 1;
          // Each completed import needs only its summary. Inactive tabs never
          // load pages or retain an extra detail payload during the batch.
          state.update((value) => ({
            ...value, projects: [summary, ...value.projects.filter((item) => item.meta.id !== summary.meta.id)],
            isLoadingProjects: false,
            openProjectIds: value.openProjectIds.includes(summary.meta.id)
              ? value.openProjectIds : [...value.openProjectIds, summary.meta.id],
          }));
        } catch (error) {
          const failure = { path, fileName: fileName(path), message: errorMessage(error, "Failed to import file.") };
          result.failures.push(failure);
          if (!disposed) state.update((value) => ({ ...value, importFailures: [...result.failures] }));
        }
      }
      if (disposed) return result;
      if (result.failures.length) {
        const pendingFilePaths = result.failures.map((failure) => failure.path);
        state.update((value) => ({
          ...value, pendingFilePaths,
          pendingRestoreAnnotations: current.pendingRestoreAnnotations && pendingFilePaths.some(isCsv),
        }));
      } else resetPending();
      // A user may navigate or start a note while importing. Keep the new tabs
      // available without replacing a newer choice or a view with an unsaved draft.
      const last = result.imported.at(-1);
      if (last) {
        if (navigationAtStart === detailGeneration && canActivate()) {
          try { await loadProjectDetail(last.meta.id); }
          catch (error) { result.openError = errorMessage(error, "Could not open the imported project."); }
        }
        try { await loadProjects(true); }
        catch (error) { result.openError ??= errorMessage(error, "Could not refresh the project list."); }
      }
      return result;
    } finally {
      if (!disposed) state.update((value) => ({ ...value, creating: false, importProgress: null }));
    }
  }

  async function deleteProject(projectId: string, afterClose: () => Promise<void> = async () => {}) {
    if (disposed) return;
    if (deletingProjects.has(projectId)) throw new Error("This project is already being deleted.");
    const before = get(state);
    const index = before.openProjectIds.indexOf(projectId);
    const wasSelected = before.selectedProjectId === projectId;
    deletingProjects.add(projectId);
    let closedGeneration = detailGeneration;
    let adjacentLoadFailed = false;
    try {
      const closing = closeProject(projectId);
      closedGeneration = detailGeneration;
      // closeProject has already removed this tab. A neighbor's failed read
      // must not prevent deleting the intended case; report it separately.
      try { await closing; }
      catch { adjacentLoadFailed = true; }
      // The app passes tick so the old ProjectView enqueues its final save.
      await afterClose();
      await backend.deleteProject(projectId);
      forgetTemporaryProject(projectId);
    } catch (error) {
      if (!disposed && index >= 0) {
        state.update((value) => {
          if (value.openProjectIds.includes(projectId)) return value;
          const openProjectIds = [...value.openProjectIds];
          openProjectIds.splice(Math.min(index, openProjectIds.length), 0, projectId);
          return { ...value, openProjectIds };
        });
        // A failed delete preserves its saved case. Restore focus only if this
        // operation still owns navigation; another selected tab stays selected.
        if (wasSelected && detailGeneration === closedGeneration) {
          try { await loadProjectDetail(projectId); }
          catch { /* Keep the original deletion error and the reopenable tab. */ }
        }
      }
      throw error;
    } finally {
      deletingProjects.delete(projectId);
    }
    await loadProjects(true);
    return { adjacentLoadFailed };
  }

  /** The close caller has already flushed and prepared its active view.
   * Delete inactive CSVs first and never mount a neighboring case during exit.
   * A failed removal keeps that case and its tab available for an explicit retry.
   */
  async function cleanupTemporaryProjects(afterClose: () => Promise<void> = async () => {}) {
    const active = get(state).selectedProjectId;
    const ids = [...temporaryProjects.keys()].sort((a, b) => Number(a === active) - Number(b === active));
    await afterClose();
    for (const id of ids) {
      await backend.deleteProject(id);
      temporaryProjects.delete(id);
      const wasSelected = get(state).selectedProjectId === id;
      if (wasSelected) { detailGeneration += 1; reads.cancelAll(); }
      state.update(value => ({ ...value, temporaryProjectIds: [...temporaryProjects.keys()],
        projects: value.projects.filter(project => project.meta.id !== id),
        openProjectIds: value.openProjectIds.filter(projectId => projectId !== id),
        ...(value.selectedProjectId === id ? { selectedProjectId: null, projectDetail: null, isLoadingDetail: false } : {}) }));
      await afterClose();
    }
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
      if (temporaryProjects.has(meta.id)) temporaryProjects.set(meta.id, { meta });
      return {
        ...value,
        projectDetail: { ...value.projectDetail, project: { meta }, hidden_columns: payload.hiddenColumns },
        projects: value.projects.map((item) => item.meta.id === meta.id ? { meta } : item),
      };
    });
  }

  function dispose() {
    if (disposed) return;
    disposed = true;
    detailGeneration += 1;
    listGeneration += 1;
    reads.dispose();
    temporaryProjects.clear();
    state.update((value) => ({
      ...value, projects: [], temporaryProjectIds: [], openProjectIds: [], selectedProjectId: null,
      projectDetail: null, isLoadingProjects: false, isLoadingDetail: false,
      pendingFilePaths: [], pendingRestoreAnnotations: false,
      creating: false, importProgress: null, importFailures: [],
    }));
  }

  function updateIocs(projectId: string, entries: readonly IocEntry[]) {
    state.update(value => value.projectDetail?.project.meta.id === projectId
      ? { ...value, projectDetail: { ...value.projectDetail, iocs: structuredClone([...entries]) } } : value);
  }

  return {
    dispose,
    state: { subscribe: state.subscribe }, canCreateProject, loadProjects, updateIocs,
    loadProjectDetail, selectProject, closeProject, refreshSelected, createProjects,
    deleteProject, adoptTemporaryProject, cleanupTemporaryProjects,
    setPendingDescription, setPendingDelimiter, setPendingFiles, setPendingRestoreAnnotations,
    resetPending, updateSummary,
  };
}
