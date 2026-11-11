use tauri::State;

use crate::commands::job::run_blocking;
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::surround::{SurroundingRowsPayload, SurroundingRowsResponse};

/// Rows around one anchor, ordered by a datetime column. Hidden columns stay in the row.
#[tauri::command]
pub async fn surrounding_rows(
    state: State<'_, AppState>,
    payload: SurroundingRowsPayload,
) -> Result<SurroundingRowsResponse, String> {
    let store = state.projects.clone();
    run_blocking(move || ops::surround::surrounding_rows(&store, payload)).await
}
