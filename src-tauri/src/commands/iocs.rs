use std::path::PathBuf;

use serde::Deserialize;
use tauri::State;
use uuid::Uuid;

use crate::{
    error::AppError,
    ioc::{
        calculate_ioc_applied_records, load_ioc_entries, prepare_ioc_entries, read_ioc_csv,
        save_ioc_entries, write_ioc_csv,
    },
    models::IocEntry,
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct SaveIocsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub entries: Vec<IocEntry>,
}

#[derive(Debug, Deserialize)]
pub struct ImportIocsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct ExportIocsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub destination: String,
}

/// Normalizes and persists IOC definitions, updating cached counts.
#[tauri::command]
pub fn save_iocs(state: State<AppState>, payload: SaveIocsPayload) -> Result<(), String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let entries = prepare_ioc_entries(payload.entries);
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;

    state.ioc_flag_cache.lock().remove(&payload.project_id);

    let ioc_applied_records =
        calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state
        .projects
        .update_ioc_applied_records(&payload.project_id, ioc_applied_records)
        .map_err(AppError::from)?;

    Ok(())
}

/// Imports IOC rules from a CSV, replacing the current set.
#[tauri::command]
pub fn import_iocs(
    state: State<AppState>,
    payload: ImportIocsPayload,
) -> Result<Vec<IocEntry>, String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let source = PathBuf::from(payload.path);
    if !source.exists() {
        return Err(AppError::Message("Selected file does not exist.".into()).into());
    }
    let entries = prepare_ioc_entries(read_ioc_csv(&source).map_err(AppError::from)?);
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;

    state.ioc_flag_cache.lock().remove(&payload.project_id);

    let ioc_applied_records =
        calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state
        .projects
        .update_ioc_applied_records(&payload.project_id, ioc_applied_records)
        .map_err(AppError::from)?;

    let final_entries = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    Ok(final_entries)
}

/// Writes the current IOC set to a destination CSV file.
#[tauri::command]
pub fn export_iocs(state: State<AppState>, payload: ExportIocsPayload) -> Result<(), String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let entries = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    let destination = PathBuf::from(payload.destination);
    write_ioc_csv(&entries, &destination).map_err(AppError::from)?;
    Ok(())
}
