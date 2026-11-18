use tauri::State;

use crate::commands::storage_worker::run_storage;
use crate::state::AppState;
use trivium_engine::models::IocEntry;
use trivium_engine::ops;
use trivium_engine::ops::iocs::{ExportIocsPayload, ImportIocsPayload, SaveIocsPayload};

/// Normalizes and persists IOC definitions, updating cached counts.
#[tauri::command]
pub async fn save_iocs(state: State<'_, AppState>, payload: SaveIocsPayload) -> Result<(), String> {
    let store = state.projects.clone();
    run_storage(payload.project_id, move || {
        ops::iocs::save_iocs(&store, payload)
    })
    .await
}

/// Imports IOC rules from a CSV, replacing the current set.
#[tauri::command]
pub async fn import_iocs(
    state: State<'_, AppState>,
    payload: ImportIocsPayload,
) -> Result<Vec<IocEntry>, String> {
    let store = state.projects.clone();
    run_storage(payload.project_id, move || {
        ops::iocs::import_iocs(&store, payload)
    })
    .await
}

/// Writes the current IOC set to a destination CSV file.
#[tauri::command]
pub async fn export_iocs(
    state: State<'_, AppState>,
    payload: ExportIocsPayload,
) -> Result<(), String> {
    let store = state.projects.clone();
    run_storage(payload.project_id, move || {
        ops::iocs::export_iocs(&store, payload)
    })
    .await
}
