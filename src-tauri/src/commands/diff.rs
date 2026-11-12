use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::diff::{DiffProjectsPayload, DiffResult};

/// Diffs two projects by key columns, preserving duplicate counts.
#[tauri::command]
pub async fn diff_projects(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: DiffProjectsPayload,
    request_id: Option<String>,
) -> Result<DiffResult, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id_a);
    let kind = if payload.destination.is_some() {
        JobKind::Export
    } else {
        JobKind::Analysis
    };
    run_job_with_request(app, kind, project_id, request_id, move |job| {
        ops::diff::diff_projects_with_job(&store, payload, &job)
    })
    .await
}
