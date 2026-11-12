use tauri::State;

use crate::commands::job::run_job_with_request;
use crate::state::AppState;
use trivium_engine::analysis::histogram::Histogram;
use trivium_engine::jobs::JobKind;
use trivium_engine::ops;
use trivium_engine::ops::histogram::HistogramPayload;

/// Equal-width buckets for a numeric or datetime column over the filtered rows.
#[tauri::command]
pub async fn histogram(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    payload: HistogramPayload,
    request_id: Option<String>,
) -> Result<Histogram, String> {
    let store = state.projects.clone();
    let project_id = Some(payload.project_id);
    run_job_with_request(app, JobKind::Analysis, project_id, request_id, move |job| {
        ops::histogram::histogram_with_job(&store, payload, &job)
    })
    .await
}
