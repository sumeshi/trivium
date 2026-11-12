use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::analysis::time::TimeAnalysis;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::time::TimeAnalysisPayload;

/// Gaps, sessions, and bursts for a datetime column over the filtered rows.
#[tauri::command]
pub async fn time_analysis(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: TimeAnalysisPayload,
    request_id: Option<String>,
) -> Result<TimeAnalysis, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Analysis, project_id, request_id, move |job| {
        ops::time::time_analysis_with_job(&store, payload, &job)
    })
    .await
}
