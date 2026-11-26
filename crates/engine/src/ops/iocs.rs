use std::path::PathBuf;

use serde::Deserialize;
use uuid::Uuid;

use crate::{
    error::AppError,
    export::atomic::{validate_destination, write_atomically},
    ioc::{
        load_ioc_entries, prepare_ioc_entries, read_ioc_csv, save_prepared_ioc_entries,
        validate_cell_columns, write_ioc_csv_to_writer,
    },
    jobs::JobContext,
    models::{IocEntry, ProjectMeta},
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
    #[serde(default, rename = "allowOverwrite")]
    pub allow_overwrite: bool,
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
    let entries = prepare_project_entries(&meta, payload.entries)?;
    save_prepared_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;

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
    let entries = read_ioc_csv(&source).map_err(AppError::from)?;
    let entries = prepare_csv_entries(&meta, entries, CsvColumns::Import)?;
    save_prepared_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;

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
        Some(entries) => entries,
        None => load_ioc_entries(&project_dir).map_err(AppError::from)?,
    };
    let entries = prepare_csv_entries(&meta, entries, CsvColumns::Export)?;
    let destination = PathBuf::from(payload.destination);
    write_atomically(
        &destination,
        &JobContext::none(),
        || validate_destination(&destination, store, payload.allow_overwrite),
        |_, writer| write_ioc_csv_to_writer(&entries, writer).map_err(AppError::from),
    )
}

/// Validate all criteria and source IDs before replacing settings or outputs.
fn prepare_project_entries(
    meta: &ProjectMeta,
    entries: Vec<IocEntry>,
) -> Result<Vec<IocEntry>, AppError> {
    let entries = prepare_ioc_entries(entries).map_err(AppError::from)?;
    if entries.iter().any(|entry| entry.cell.is_some()) {
        validate_cell_columns(&entries, &meta.columns).map_err(AppError::from)?;
    }
    Ok(entries)
}

enum CsvColumns {
    Import,
    Export,
}

/// The CSV uses portable source names/pointers; saved and API rules use IDs.
fn prepare_csv_entries(
    meta: &ProjectMeta,
    entries: Vec<IocEntry>,
    direction: CsvColumns,
) -> Result<Vec<IocEntry>, AppError> {
    let mut entries = prepare_ioc_entries(entries).map_err(AppError::from)?;
    if !entries.iter().any(|entry| entry.cell.is_some()) {
        return Ok(entries);
    }
    let columns = &meta.columns;
    for (index, entry) in entries.iter_mut().enumerate() {
        let Some(cell) = &mut entry.cell else {
            continue;
        };
        let source = match direction {
            CsvColumns::Import => cell.column.as_str(),
            CsvColumns::Export => columns
                .iter()
                .find(|column| column.id == cell.column)
                .ok_or_else(|| {
                    AppError::Message(format!(
                        "IOC rule {}: column {:?} was not found in this project.",
                        index + 1,
                        cell.column
                    ))
                })?
                .source
                .as_str(),
        };
        if source.trim().is_empty() {
            return Err(AppError::Message(format!(
                "IOC rule {}: an unnamed source column cannot be shared through CSV.",
                index + 1
            )));
        }
        let mut matches = columns.iter().filter(|column| column.source == source);
        let resolved = matches.next().ok_or_else(|| {
            AppError::Message(format!(
                "IOC rule {}: source column {source:?} was not found in this project.",
                index + 1
            ))
        })?;
        if matches.next().is_some() {
            return Err(AppError::Message(format!(
                "IOC rule {}: source column {source:?} is ambiguous in this project.",
                index + 1
            )));
        }
        cell.column = match direction {
            CsvColumns::Import => resolved.id.clone(),
            CsvColumns::Export => source.to_string(),
        };
    }
    Ok(entries)
}
