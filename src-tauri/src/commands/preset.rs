use tauri::State;

use crate::commands::storage_worker::{run_storage, run_storage_read};
use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::preset::{LoadPresetPayload, PresetData, SavePresetPayload};

/// Writes the current view as a portable preset file.
#[tauri::command]
pub async fn save_preset(
    state: State<'_, AppState>,
    payload: SavePresetPayload,
) -> Result<(), String> {
    let store = state.projects.clone();
    run_storage(payload.project_id, move || {
        ops::preset::save_preset(&store, payload)
    })
    .await
}

/// Reads a preset file and reports columns unknown to the selected project.
#[tauri::command]
pub async fn load_preset(
    state: State<'_, AppState>,
    payload: LoadPresetPayload,
) -> Result<PresetData, String> {
    let store = state.projects.clone();
    run_storage_read(payload.project_id.unwrap_or_default(), move || {
        ops::preset::load_preset(&store, payload)
    })
    .await
}
