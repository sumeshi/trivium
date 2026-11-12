use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::export::{ExportProjectPayload, ExportResult};

/// Exports the project data with derived trivium columns to a CSV file.
///
/// The export and its row resolution run on the blocking pool. The result
/// reports the written row count, output headers, and non-fatal warnings.
#[tauri::command]
pub async fn export_project(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: ExportProjectPayload,
    request_id: Option<String>,
) -> Result<ExportResult, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Export, project_id, request_id, move |job| {
        ops::export::export_project_with_job(&store, payload, &job)
    })
    .await
}
