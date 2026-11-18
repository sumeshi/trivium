use tauri::State;

use crate::commands::storage_worker::{run_storage, run_storage_read};
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::projects::ProjectRequest;
use trivium_engine::ops::rules::{ColorRulesResponse, SaveColorRulesPayload};

/// Saves conditional color rules for a project.
#[tauri::command]
pub async fn save_color_rules(
    state: State<'_, AppState>,
    payload: SaveColorRulesPayload,
) -> Result<ColorRulesResponse, String> {
    let store = state.projects.clone();
    run_storage(payload.project_id, move || {
        ops::rules::save_color_rules(&store, payload)
    })
    .await
}

/// Returns the conditional color rules of a project.
#[tauri::command]
pub async fn color_rules(
    state: State<'_, AppState>,
    request: ProjectRequest,
) -> Result<ColorRulesResponse, String> {
    let store = state.projects.clone();
    run_storage_read(request.project_id, move || {
        ops::rules::color_rules(&store, request)
    })
    .await
}
