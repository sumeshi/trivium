use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::preset::{LoadPresetPayload, PresetData, SavePresetPayload};

/// Writes the current view as a portable preset file.
#[tauri::command]
pub fn save_preset(state: State<AppState>, payload: SavePresetPayload) -> Result<(), String> {
    ops::preset::save_preset(&state.projects, payload).map_err(String::from)
}

/// Reads a preset file and reports columns unknown to the selected project.
#[tauri::command]
pub fn load_preset(state: State<AppState>, payload: LoadPresetPayload) -> Result<PresetData, String> {
    ops::preset::load_preset(&state.projects, payload).map_err(String::from)
}
