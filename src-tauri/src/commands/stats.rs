use tauri::State;

use crate::state::AppState;
use trivium_engine::analysis::stats::ColumnStats;
use trivium_engine::ops;
use trivium_engine::ops::stats::ColumnStatsPayload;

/// Returns kind, missing count, distinct count, top values, and rare values.
#[tauri::command]
pub fn column_stats(
    state: State<AppState>,
    payload: ColumnStatsPayload,
) -> Result<ColumnStats, String> {
    ops::stats::column_stats(&state.projects, payload).map_err(String::from)
}
