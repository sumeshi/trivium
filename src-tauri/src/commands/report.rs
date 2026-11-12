use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::report::{BuildReportPayload, ReportMeta};

/// Builds a static HTML report of the current filter results.
#[tauri::command]
pub async fn build_report(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: BuildReportPayload,
    request_id: Option<String>,
) -> Result<ReportMeta, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Export, project_id, request_id, move |job| {
        ops::report::build_report_with_job(&store, payload, &job)
    })
    .await
}
