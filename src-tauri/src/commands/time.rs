use tauri::State;

use crate::state::AppState;
use trivium_engine::analysis::time::TimeAnalysis;
use trivium_engine::ops;
use trivium_engine::ops::time::TimeAnalysisPayload;

/// Gaps, sessions, and bursts for a datetime column over the filtered rows.
#[tauri::command]
pub fn time_analysis(
    state: State<AppState>,
    payload: TimeAnalysisPayload,
) -> Result<TimeAnalysis, String> {
    ops::time::time_analysis(&state.projects, payload).map_err(String::from)
}
