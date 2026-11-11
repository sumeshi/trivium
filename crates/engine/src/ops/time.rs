use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::time::{analyze_series, TimeAnalysis};
use crate::condition::Node;
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::project_io::{is_internal_column_for_version, read_project_dataframe};
use crate::storage::load_flags;
use crate::store::ProjectsStore;

use super::query::{resolve_target, QuerySpec};

#[derive(Debug, Deserialize)]
pub struct TimeAnalysisPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub column: String,
    #[serde(default = "default_gap_seconds", rename = "gapSeconds")]
    pub gap_seconds: u64,
    #[serde(default = "default_session_minutes", rename = "sessionMinutes")]
    pub session_minutes: u64,
    #[serde(default = "default_burst_window", rename = "burstWindowSeconds")]
    pub burst_window_seconds: u64,
    #[serde(default = "default_burst_min", rename = "burstMin")]
    pub burst_min: u64,
    #[serde(default)]
    pub search: Option<String>,
    /// Deprecated visible-column field kept for old callers. New callers send
    /// `searchColumns`; `searchColumns` wins when both are present.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "searchColumns")]
    pub search_columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default, rename = "bookmarkedOnly")]
    pub bookmarked_only: Option<bool>,
    #[serde(default)]
    pub condition: Option<Node>,
}

fn default_gap_seconds() -> u64 {
    60
}

fn default_session_minutes() -> u64 {
    30
}

fn default_burst_window() -> u64 {
    60
}

fn default_burst_min() -> u64 {
    10
}

/// Gaps, sessions, and bursts for one datetime column over the filtered rows.
pub fn time_analysis(
    store: &ProjectsStore,
    payload: TimeAnalysisPayload,
) -> Result<TimeAnalysis, AppError> {
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    if is_internal_column_for_version(&payload.column, meta.storage_version) {
        return Err(AppError::Message("Column not found.".into()));
    }
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let columns = super::query::data_columns(&df, meta.storage_version);
    let series = df
        .column(&payload.column)
        .map_err(|_| AppError::Message("Column not found.".into()))?;
    let flags = load_flags(&store.flags_path(&meta.id)).map_err(AppError::from)?;
    let spec = QuerySpec {
        search: payload.search.clone(),
        search_columns: payload
            .search_columns
            .clone()
            .or_else(|| payload.columns.clone()),
        condition: payload.condition.clone(),
        flag_filter: payload.flag_filter.clone(),
        bookmarked_only: payload.bookmarked_only,
        sort_key: None,
        sort_direction: None,
    };
    let filtered = resolve_target(&df, &spec, &columns, &flags, &JobContext::none())?.filtered;
    analyze_series(
        &payload.column,
        series,
        &filtered,
        payload.gap_seconds,
        payload.session_minutes,
        payload.burst_window_seconds,
        payload.burst_min,
    )
    .map_err(AppError::Message)
}
