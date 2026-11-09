use serde::Deserialize;
use uuid::Uuid;

use crate::{
    error::AppError,
    models::{FlagEntry, ProjectRow},
    project_io::read_project_dataframe,
    storage::{
        clear_ioc_flag_cache, clear_searchable_cache, count_flagged, remove_flag, upsert_flag,
    },
    store::ProjectsStore,
};

use super::util::collect_row_record;

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
pub fn update_flag(
    store: &ProjectsStore,
    payload: UpdateFlagPayload,
) -> Result<ProjectRow, AppError> {
    let Some(_) = store.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = store.project_dir(&payload.project_id);
    let flags_path = project_dir.join("flags.json");
    let should_clear = payload.flag.trim().is_empty()
        && payload
            .memo
            .as_ref()
            .map(|m| m.trim().is_empty())
            .unwrap_or(true);

    if should_clear {
        remove_flag(&flags_path, payload.row_index).map_err(AppError::from)?;
    } else {
        let entry = FlagEntry {
            flag: payload.flag.clone(),
            memo: payload.memo.clone(),
        };
        upsert_flag(&flags_path, payload.row_index, &entry).map_err(AppError::from)?;
    }

    if let Err(err) = clear_ioc_flag_cache(&project_dir) {
        eprintln!(
            "[cache] failed to clear IOC cache for {:?}: {:?}",
            project_dir, err
        );
    }

    let flagged_records = count_flagged(&flags_path).map_err(AppError::from)?;
    store
        .update_flagged_records(&payload.project_id, flagged_records)
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
pub fn set_hidden_columns(
    store: &ProjectsStore,
    payload: HiddenColumnsPayload,
) -> Result<(), AppError> {
    let Some(_) = store.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = store.project_dir(&payload.project_id);
    store
        .update_hidden_columns(&payload.project_id, payload.hidden_columns)
        .map_err(AppError::from)?;
    if let Err(err) = clear_searchable_cache(&project_dir) {
        eprintln!(
            "[cache] failed to clear searchable cache for {:?}: {:?}",
            project_dir, err
        );
    }
    Ok(())
}
