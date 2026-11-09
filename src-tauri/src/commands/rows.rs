use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::rows::{QueryRowsPayload, QueryRowsResponse};

/// Streams project rows with filtering, sorting, IOC application, and pagination.
#[tauri::command]
pub fn query_project_rows(
    state: State<AppState>,
    payload: QueryRowsPayload,
) -> Result<QueryRowsResponse, String> {
    ops::rows::query_project_rows(&state.projects, payload).map_err(String::from)
}
