use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::dupes::{DupesPayload, DupesResponse};

/// Returns every row whose key is repeated, or every row whose key appears once.
#[tauri::command]
pub async fn duplicate_rows(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: DupesPayload,
    request_id: Option<String>,
) -> Result<DupesResponse, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Analysis, project_id, request_id, move |job| {
        ops::dupes::duplicate_rows_with_job(&store, payload, &job)
    })
    .await
}
