use serde::Deserialize;
use tauri::State;
use uuid::Uuid;

use crate::{
    error::AppError,
    ioc::calculate_ioc_applied_records,
    models::{FlagEntry, ProjectRow},
    project_io::read_project_dataframe,
    state::AppState,
    storage::{load_flags, save_flags},
};

use super::utils::collect_row_record;

#[derive(Debug, Deserialize)]
pub struct UpdateFlagPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub row_index: usize,
    pub flag: String,
    pub memo: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct HiddenColumnsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub hidden_columns: Vec<String>,
}

/// Applies or clears a user flag for a single row and updates counters.
#[tauri::command]
pub fn update_flag(
    state: State<AppState>,
    payload: UpdateFlagPayload,
) -> Result<ProjectRow, String> {
    let Some(_) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&payload.project_id);
    let flags_path = project_dir.join("flags.json");
    let mut flags = load_flags(&flags_path).map_err(AppError::from)?;

    if payload.flag.trim().is_empty()
        && payload
            .memo
            .as_ref()
            .map(|m| m.trim().is_empty())
            .unwrap_or(true)
    {
        flags.remove(&payload.row_index);
    } else {
        flags.insert(
            payload.row_index,
            FlagEntry {
                flag: payload.flag.clone(),
                memo: payload.memo.clone(),
            },
        );
    }

    save_flags(&flags_path, &flags).map_err(AppError::from)?;

    let flagged_records = flags
        .values()
        .filter(|entry| !entry.flag.trim().is_empty())
        .count();
    state
        .projects
        .update_flagged_records(&payload.project_id, flagged_records)
        .map_err(AppError::from)?;

    let ioc_applied_records =
        calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state
        .projects
        .update_ioc_applied_records(&payload.project_id, ioc_applied_records)
        .map_err(AppError::from)?;

    let parquet_path = project_dir.join("data.parquet");
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let column_names: Vec<String> = df
        .get_column_names()
        .iter()
        .filter(|name| **name != "__rowid")
        .map(|name| name.to_string())
        .collect();
    let record = collect_row_record(&df, &column_names, payload.row_index);

    Ok(ProjectRow {
        row_index: payload.row_index,
        data: record,
        flag: payload.flag,
        memo: payload.memo,
    })
}

/// Persists the set of hidden columns for a project and resets search cache.
#[tauri::command]
pub fn set_hidden_columns(
    state: State<AppState>,
    payload: HiddenColumnsPayload,
) -> Result<(), String> {
    let Some(_) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    state
        .projects
        .update_hidden_columns(&payload.project_id, payload.hidden_columns)
        .map_err(AppError::from)?;
    state.searchable_cache.lock().remove(&payload.project_id);
    Ok(())
}
