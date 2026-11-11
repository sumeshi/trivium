use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::report::{BuildReportPayload, ReportMeta};

/// Builds a static HTML report of the current filter results.
#[tauri::command]
pub async fn build_report(
    state: State<'_, AppState>,
    payload: BuildReportPayload,
) -> Result<ReportMeta, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::report::build_report(&store, payload)).await
}
