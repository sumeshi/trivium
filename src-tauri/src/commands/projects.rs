use tauri::State;

use crate::commands::job::{run_blocking, run_job};
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::models::{LoadProjectResponse, ProjectSummary};
use trivium_engine::ops;
use trivium_engine::ops::projects::{CreateProjectPayload, ProjectRequest};

/// Lists saved projects ordered by creation time (newest first).
#[tauri::command]
pub fn list_projects(state: State<AppState>) -> Result<Vec<ProjectSummary>, String> {
    ops::projects::list_projects(&state.projects).map_err(String::from)
}

/// Creates a new project from a CSV/JSON file on the blocking pool.
#[tauri::command]
pub async fn create_project(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: CreateProjectPayload,
) -> Result<ProjectSummary, String> {
    let store = state.projects.clone();
    run_job(app, JobKind::Import, None, move |job| {
        ops::projects::create_project_with_job(&store, payload, &job)
    })
    .await
}

/// Removes a project directory and clears related caches.
#[tauri::command]
pub async fn delete_project(
    state: State<'_, AppState>,
    request: ProjectRequest,
) -> Result<(), String> {
    let store = state.projects.clone();
    run_blocking(move || ops::projects::delete_project(&store, request)).await
}

/// Loads project metadata, initial rows, IOC entries, and column metrics.
#[tauri::command]
pub async fn load_project(
    state: State<'_, AppState>,
    request: ProjectRequest,
) -> Result<LoadProjectResponse, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::projects::load_project(&store, request)).await
}
