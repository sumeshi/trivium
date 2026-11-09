use tauri::State;

use crate::state::AppState;
use trivium_engine::ops;
use trivium_engine::ops::export::ExportProjectPayload;

/// Exports the project data with derived trivium columns to a CSV file.
#[tauri::command]
pub fn export_project(state: State<AppState>, payload: ExportProjectPayload) -> Result<(), String> {
    ops::export::export_project(&state.projects, payload).map_err(String::from)
}
