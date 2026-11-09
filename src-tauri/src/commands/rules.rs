use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::projects::ProjectRequest;
use trivium_engine::ops::rules::{ColorRulesResponse, SaveColorRulesPayload};

/// Saves conditional color rules for a project.
#[tauri::command]
pub fn save_color_rules(
    state: State<AppState>,
    payload: SaveColorRulesPayload,
) -> Result<ColorRulesResponse, String> {
    ops::rules::save_color_rules(&state.projects, payload).map_err(String::from)
}

/// Returns the conditional color rules of a project.
#[tauri::command]
pub fn color_rules(
    state: State<AppState>,
    request: ProjectRequest,
) -> Result<ColorRulesResponse, String> {
    ops::rules::color_rules(&state.projects, request).map_err(String::from)
}
