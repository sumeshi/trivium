use tauri::State;

use crate::commands::storage_worker::run_storage;
use crate::state::AppState;
use trivium_engine::models::UpdateFlagResponse;
use trivium_engine::ops;
use trivium_engine::ops::flags::{HiddenColumnsPayload, UpdateFlagPayload};

/// Applies or clears a user flag for a single row and updates counters.
#[tauri::command]
pub async fn update_flag(
    state: State<'_, AppState>,
    payload: UpdateFlagPayload,
) -> Result<UpdateFlagResponse, String> {
    let store = state.projects.clone();
    run_storage(payload.project_id, move || {
        ops::flags::update_flag(&store, payload)
    })
    .await
}

/// Persists the set of hidden columns for a project and resets search cache.
#[tauri::command]
pub async fn set_hidden_columns(
    state: State<'_, AppState>,
    payload: HiddenColumnsPayload,
) -> Result<(), String> {
    let store = state.projects.clone();
    run_storage(payload.project_id, move || {
        ops::flags::set_hidden_columns(&store, payload)
    })
    .await
}
