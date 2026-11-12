use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::cross::{CrossSearchHit, CrossSearchPayload};

/// One condition applied to each project. Each hit count matches a single-project query.
#[tauri::command]
pub async fn search_projects(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: CrossSearchPayload,
    request_id: Option<String>,
) -> Result<Vec<CrossSearchHit>, String> {
    let store = state.projects.clone();
    let project_id = None;
    run_job_with_request(app, JobKind::Analysis, project_id, request_id, move |job| {
        ops::cross::search_projects_with_job(&store, payload, &job)
    })
    .await
}
