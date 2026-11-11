use tauri::State;

use crate::state::AppState;
use trivium_engine::models::UpdateFlagResponse;
use trivium_engine::ops;
use trivium_engine::ops::flags::{HiddenColumnsPayload, UpdateFlagPayload};

/// Applies or clears a user flag for a single row and updates counters.
#[tauri::command]
pub fn update_flag(
    state: State<AppState>,
    payload: UpdateFlagPayload,
) -> Result<UpdateFlagResponse, String> {
    ops::flags::update_flag(&state.projects, payload).map_err(String::from)
}

/// Persists the set of hidden columns for a project and resets search cache.
#[tauri::command]
pub fn set_hidden_columns(
    state: State<AppState>,
    payload: HiddenColumnsPayload,
) -> Result<(), String> {
    ops::flags::set_hidden_columns(&state.projects, payload).map_err(String::from)
}
