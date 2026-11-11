use polars::prelude::*;
use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::stats::{summarize_series, ColumnStats};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::project_io::{is_internal_column, read_project_dataframe};
use crate::storage::load_flags;
use crate::store::ProjectsStore;

use super::query::{data_columns, resolve_target, QuerySpec};

#[derive(Debug, Deserialize)]
pub struct ColumnStatsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub column: String,
    #[serde(default = "default_rare_max", rename = "rareMax")]
    pub rare_max: u64,
    /// Applied QuerySpec. Stats describe the current result, not the whole
    /// project, when a search/filter is active.
    #[serde(flatten)]
    pub spec: QuerySpec,
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
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let columns = data_columns(&df);
    let flags = load_flags(&project_dir.join("flags.json")).map_err(AppError::from)?;
    let target = resolve_target(&df, &payload.spec, &columns, &flags, &JobContext::none())?;
    let take_idx = UInt32Chunked::from_iter_values(
        "take_idx",
        target.filtered.iter().map(|index| *index as u32),
    );
    let filtered = df.take(&take_idx).map_err(|err| AppError::Other(err.into()))?;
    let series = filtered
        .column(&payload.column)
        .map_err(|_| AppError::Message("Column not found.".into()))?;
    Ok(summarize_series(&payload.column, series, payload.rare_max))
}
