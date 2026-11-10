use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::analysis::dupes::matching_row_indices;
use crate::error::AppError;
use crate::models::ProjectRow;
use crate::project_io::{is_internal_column, read_project_dataframe};
use crate::storage::load_flags;
use crate::store::ProjectsStore;

use super::util::materialize_rows;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DupeMode {
    Duplicates,
    Unique,
}

#[derive(Debug, Deserialize)]
pub struct DupesPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub columns: Vec<String>,
    pub mode: DupeMode,
}

#[derive(Debug, Serialize)]
pub struct DupesResponse {
    pub rows: Vec<ProjectRow>,
}

/// Rows whose key is repeated, or rows whose key appears once. Repeated keys keep every row.
pub fn duplicate_rows(
    store: &ProjectsStore,
    payload: DupesPayload,
) -> Result<DupesResponse, AppError> {
    if payload.columns.is_empty()
        || payload
            .columns
            .iter()
            .any(|column| is_internal_column(column))
    {
        return Err(AppError::Message("Column not found.".into()));
    }
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let mut series = Vec::with_capacity(payload.columns.len());
    for column in &payload.columns {
        let found = df
            .column(column)
            .map_err(|_| AppError::Message("Column not found.".into()))?;
        series.push(found);
    }
    let data_columns = super::query::data_columns(&df);
    let flags = load_flags(&project_dir.join("flags.json")).map_err(AppError::from)?;
    let indices = matching_row_indices(&series, payload.mode == DupeMode::Duplicates);
    Ok(DupesResponse {
        rows: materialize_rows(&df, &data_columns, indices.into_iter(), &flags),
    })
}
