use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::surround::{SurroundingRowsPayload, SurroundingRowsResponse};

/// Rows around one anchor, ordered by a datetime column. Hidden columns stay in the row.
#[tauri::command]
pub fn surrounding_rows(
    state: State<AppState>,
    payload: SurroundingRowsPayload,
) -> Result<SurroundingRowsResponse, String> {
    ops::surround::surrounding_rows(&state.projects, payload).map_err(String::from)
}
