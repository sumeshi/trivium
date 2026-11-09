use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::report::{BuildReportPayload, ReportMeta};

/// Builds a static HTML report of the current filter results.
#[tauri::command]
pub fn build_report(
    state: State<AppState>,
    payload: BuildReportPayload,
) -> Result<ReportMeta, String> {
    ops::report::build_report(&state.projects, payload).map_err(String::from)
}
