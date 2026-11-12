use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::cell::{GetCellPayload, GetCellResponse};

/// Reads one projected cell through the common cancellable worker queue.
#[tauri::command]
pub async fn get_cell(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: GetCellPayload,
    request_id: Option<String>,
) -> Result<GetCellResponse, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Query, project_id, request_id, move |job| {
        ops::cell::get_cell_with_job(&store, payload, &job)
    })
    .await
}
