use tauri::State;

use crate::commands::storage_worker::{run_storage, run_storage_read};
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::view::{SaveViewStatePayload, ViewStateLoad, ViewStateRequest};

/// Loads the rebuildable view state for a project. Legacy projects have none.
#[tauri::command]
pub async fn load_view_state(
    state: State<'_, AppState>,
    request: ViewStateRequest,
) -> Result<ViewStateLoad, String> {
    let store = state.projects.clone();
    run_storage_read(request.project_id, move || {
        ops::view::load_view_state(&store, request)
    })
    .await
}

/// Persists the rebuildable view state separately from annotations.
#[tauri::command]
pub async fn save_view_state(
    state: State<'_, AppState>,
    payload: SaveViewStatePayload,
) -> Result<(), String> {
    let store = state.projects.clone();
    run_storage(payload.project_id, move || {
        ops::view::save_view_state(&store, payload)
    })
    .await
}
