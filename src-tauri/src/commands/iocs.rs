use tauri::State;

use crate::state::AppState;
use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::iocs::{ExportIocsPayload, ImportIocsPayload, SaveIocsPayload};

/// Normalizes and persists IOC definitions, updating cached counts.
#[tauri::command]
pub fn save_iocs(state: State<AppState>, payload: SaveIocsPayload) -> Result<(), String> {
    ops::iocs::save_iocs(&state.projects, payload).map_err(String::from)
}

/// Imports IOC rules from a CSV, replacing the current set.
#[tauri::command]
pub fn import_iocs(
    state: State<AppState>,
    payload: ImportIocsPayload,
) -> Result<Vec<IocEntry>, String> {
    ops::iocs::import_iocs(&state.projects, payload).map_err(String::from)
}

/// Writes the current IOC set to a destination CSV file.
#[tauri::command]
pub fn export_iocs(state: State<AppState>, payload: ExportIocsPayload) -> Result<(), String> {
    ops::iocs::export_iocs(&state.projects, payload).map_err(String::from)
}
