use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::analysis::time::TimeAnalysis;
use trivium_engine::ops;
use trivium_engine::ops::time::TimeAnalysisPayload;

/// Gaps, sessions, and bursts for a datetime column over the filtered rows.
#[tauri::command]
pub async fn time_analysis(
    state: State<'_, AppState>,
    payload: TimeAnalysisPayload,
) -> Result<TimeAnalysis, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::time::time_analysis(&store, payload)).await
}
