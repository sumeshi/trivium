use tauri::State;

use crate::{commands::job::run_job_with_request, state::AppState};
use trivium_engine::{
    jobs::JobKind,
    models::ProjectSummary,
    ops::extract::{self, ExtractPayload},
};

/// Scan into a generated CSV and open it through the ordinary project pipeline.
#[tauri::command]
pub async fn extract_values(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: ExtractPayload,
    request_id: Option<String>,
) -> Result<ProjectSummary, String> {
    let store = state.projects.clone();
    let project = Some(payload.project_id);
    run_job_with_request(app, JobKind::Analysis, project, request_id, move |job| {
        extract::extract_values(&store, payload, &job)
    })
    .await
}
