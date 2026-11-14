use std::path::PathBuf;

use serde::Deserialize;
use uuid::Uuid;

use crate::{
    error::AppError,
    export::atomic::{
        ensure_destination_is_not_managed, ensure_destination_is_not_source, write_atomically,
    },
    ioc::{
        load_ioc_entries, prepare_ioc_entries, read_ioc_csv, save_ioc_entries,
        write_ioc_csv_to_writer,
    },
    jobs::JobContext,
    models::IocEntry,
    store::ProjectsStore,
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
    /// Unsaved editor entries; absent exports the stored definitions.
    #[serde(default)]
    pub entries: Option<Vec<IocEntry>>,
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub destination: String,
}

/// Normalizes and persists IOC definitions.
///
/// Saving a definition never changes row annotations. The UI applies a chosen
/// rule through the ordinary bulk preview/update/undo transaction.
pub fn save_iocs(store: &ProjectsStore, payload: SaveIocsPayload) -> Result<(), AppError> {
    let Some(meta) = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
    else {
        return Err(AppError::Message("Project not found.".into()));
    };
    let project_dir = store.project_dir(&meta.id);
    let entries = prepare_ioc_entries(payload.entries);
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;

    Ok(())
}

/// Imports IOC rules from a CSV, replacing the current saved definitions.
pub fn import_iocs(
    store: &ProjectsStore,
    payload: ImportIocsPayload,
) -> Result<Vec<IocEntry>, AppError> {
    let Some(meta) = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
    else {
        return Err(AppError::Message("Project not found.".into()));
    };
    let project_dir = store.project_dir(&meta.id);
    let source = PathBuf::from(payload.path);
    if !source.exists() {
        return Err(AppError::Message("Selected file does not exist.".into()));
    }
    let entries = prepare_ioc_entries(read_ioc_csv(&source).map_err(AppError::from)?);
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;

    let final_entries = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    Ok(final_entries)
}

/// Writes the current IOC set to a destination CSV file.
pub fn export_iocs(store: &ProjectsStore, payload: ExportIocsPayload) -> Result<(), AppError> {
    let Some(meta) = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
    else {
        return Err(AppError::Message("Project not found.".into()));
    };
    let project_dir = store.project_dir(&meta.id);
    let entries = match payload.entries {
        Some(entries) => prepare_ioc_entries(entries),
        None => load_ioc_entries(&project_dir).map_err(AppError::from)?,
    };
    let destination = PathBuf::from(payload.destination);
    write_atomically(
        &destination,
        &JobContext::none(),
        || {
            ensure_destination_is_not_source(&destination, &meta)?;
            ensure_destination_is_not_managed(&destination, store)
        },
        |_, writer| write_ioc_csv_to_writer(&entries, writer).map_err(AppError::from),
    )
}
