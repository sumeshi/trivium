use tauri::State;

use crate::commands::job::{run_blocking, run_job_with_request};
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
    request_id: Option<String>,
) -> Result<ProjectSummary, String> {
    let store = state.projects.clone();
    run_job_with_request(app, JobKind::Import, None, request_id, move |job| {
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
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    request: ProjectRequest,
    metadata_only: Option<bool>,
    request_id: Option<String>,
) -> Result<LoadProjectResponse, String> {
    let store = state.projects.clone();
    let project_id = Some(request.project_id);
    run_job_with_request(app, JobKind::Load, project_id, request_id, move |job| {
        if metadata_only.unwrap_or(false) {
            ops::projects::load_project_metadata_with_job(&store, request, &job)
        } else {
            ops::projects::load_project_with_job(&store, request, &job)
        }
    })
    .await
}
