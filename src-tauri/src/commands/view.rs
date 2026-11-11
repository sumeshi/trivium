use tauri::State;

use crate::state::AppState;
use trivium_engine::models::ViewState;
use trivium_engine::ops;
use trivium_engine::ops::view::{SaveViewStatePayload, ViewStateRequest};

/// Loads the rebuildable view state for a project. Legacy projects have none.
#[tauri::command]
pub fn load_view_state(
    state: State<AppState>,
    request: ViewStateRequest,
) -> Result<Option<ViewState>, String> {
    ops::view::load_view_state(&state.projects, request).map_err(String::from)
}

/// Persists the rebuildable view state separately from annotations.
#[tauri::command]
pub fn save_view_state(
    state: State<AppState>,
    payload: SaveViewStatePayload,
) -> Result<(), String> {
    ops::view::save_view_state(&state.projects, payload).map_err(String::from)
}
