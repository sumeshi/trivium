use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::dupes::{DupesPayload, DupesResponse};

/// Returns every row whose key is repeated, or every row whose key appears once.
#[tauri::command]
pub fn duplicate_rows(
    state: State<AppState>,
    payload: DupesPayload,
) -> Result<DupesResponse, String> {
    ops::dupes::duplicate_rows(&state.projects, payload).map_err(String::from)
}
