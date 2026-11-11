use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::analysis::stats::ColumnStats;
use trivium_engine::ops;
use trivium_engine::ops::stats::ColumnStatsPayload;

/// Returns kind, missing count, distinct count, top values, and rare values.
#[tauri::command]
pub async fn column_stats(
    state: State<'_, AppState>,
    payload: ColumnStatsPayload,
) -> Result<ColumnStats, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::stats::column_stats(&store, payload)).await
}
