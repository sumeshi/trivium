use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::surround::{SurroundingRowsPayload, SurroundingRowsResponse};

/// Rows around one anchor, ordered by a datetime column. Hidden columns stay in the row.
#[tauri::command]
pub async fn surrounding_rows(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: SurroundingRowsPayload,
    request_id: Option<String>,
) -> Result<SurroundingRowsResponse, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Query, project_id, request_id, move |job| {
        ops::surround::surrounding_rows_with_job(&store, payload, &job)
    })
    .await
}
