//! Named format catalog commands operate only inside the managed directory.
use tauri::State;
use trivium_engine::formats::{
    self, CompiledFormat, DeleteFormatPayload, FormatCatalogEntry, FormatDefinition,
    SaveFormatPayload,
};
use uuid::Uuid;

use crate::{
    commands::{
        job::run_blocking,
        storage_worker::{run_storage, run_storage_read},
    },
    state::AppState,
};

#[tauri::command]
pub async fn list_formats(state: State<'_, AppState>) -> Result<Vec<FormatCatalogEntry>, String> {
    let directory = state.projects.formats_dir();
    run_storage_read(Uuid::nil(), move || formats::list_formats(&directory)).await
}

#[tauri::command]
pub async fn validate_format(definition: FormatDefinition) -> Result<(), String> {
    run_blocking(move || CompiledFormat::new(&definition).map(|_| ())).await
}

#[tauri::command]
pub async fn save_format(
    state: State<'_, AppState>,
    payload: SaveFormatPayload,
) -> Result<FormatCatalogEntry, String> {
    let directory = state.projects.formats_dir();
    run_storage(Uuid::nil(), move || {
        formats::save_format(&directory, &payload.definition)
    })
    .await
}

#[tauri::command]
pub async fn delete_format(
    state: State<'_, AppState>,
    payload: DeleteFormatPayload,
) -> Result<(), String> {
    let directory = state.projects.formats_dir();
    run_storage(Uuid::nil(), move || {
        formats::delete_format(&directory, &payload.id)
    })
    .await
}
