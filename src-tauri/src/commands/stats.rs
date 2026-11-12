use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::analysis::stats::ColumnStats;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::stats::ColumnStatsPayload;

/// Returns kind, missing count, distinct count, top values, and rare values.
#[tauri::command]
pub async fn column_stats(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: ColumnStatsPayload,
    request_id: Option<String>,
) -> Result<ColumnStats, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Analysis, project_id, request_id, move |job| {
        ops::stats::column_stats_with_job(&store, payload, &job)
    })
    .await
}
