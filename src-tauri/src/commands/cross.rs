use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::cross::{CrossSearchHit, CrossSearchPayload};

/// One condition applied to each project. Each hit count matches a single-project query.
#[tauri::command]
pub fn search_projects(
    state: State<AppState>,
    payload: CrossSearchPayload,
) -> Result<Vec<CrossSearchHit>, String> {
    ops::cross::search_projects(&state.projects, payload).map_err(String::from)
}
