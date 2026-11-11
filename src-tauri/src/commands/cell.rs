use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::cell::{GetCellPayload, GetCellResponse};

/// Full value of one cell, identified by project, original row id, and column.
///
/// The read is one row with a column projection, so it stays synchronous
/// instead of using the background-job registry.
#[tauri::command]
pub fn get_cell(
    state: State<AppState>,
    payload: GetCellPayload,
) -> Result<GetCellResponse, String> {
    ops::cell::get_cell(&state.projects, payload).map_err(String::from)
}
