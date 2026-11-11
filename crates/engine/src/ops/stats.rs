use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::stats::{summarize_series, ColumnStats};
use crate::error::AppError;
use crate::project_io::{is_internal_column, read_project_dataframe};
use crate::store::ProjectsStore;

#[derive(Debug, Deserialize)]
pub struct ColumnStatsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub column: String,
    #[serde(default = "default_rare_max", rename = "rareMax")]
    pub rare_max: u64,
}

fn default_rare_max() -> u64 {
    1
}

/// Column kind, missing count, distinct count, top 20 values, and up to 20 rare values.
pub fn column_stats(
    store: &ProjectsStore,
    payload: ColumnStatsPayload,
) -> Result<ColumnStats, AppError> {
    if is_internal_column(&payload.column) {
        return Err(AppError::Message("Column not found.".into()));
    }
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let parquet_path = store.project_dir(&meta.id).join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let series = df
        .column(&payload.column)
        .map_err(|_| AppError::Message("Column not found.".into()))?;
    Ok(summarize_series(&payload.column, series, payload.rare_max))
}
