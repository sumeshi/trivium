use tauri::State;

use crate::commands::job::run_job;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::export::ExportProjectPayload;

/// Exports the project data with derived trivium columns to a CSV file.
///
/// The export and its row resolution run on the blocking pool.
#[tauri::command]
pub async fn export_project(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: ExportProjectPayload,
) -> Result<(), String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job(app, JobKind::Export, project_id, move |job| {
        ops::export::export_project_with_job(&store, payload, &job)
    })
    .await
}
