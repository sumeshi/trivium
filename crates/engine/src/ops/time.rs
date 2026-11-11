use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::time::{analyze_series, TimeAnalysis};
use crate::condition::Node;
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::project_io::{is_internal_column, read_project_dataframe};
use crate::storage::load_flags;
use crate::store::ProjectsStore;

use super::query::{filtered_indices, prepare_search, user_flags};

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
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
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
    let columns = super::query::data_columns(&df);
    let series = df
        .column(&payload.column)
        .map_err(|_| AppError::Message("Column not found.".into()))?;
    let search_state = prepare_search(
        &df,
        payload.columns.as_deref(),
        &columns,
        payload.search.as_deref(),
        payload.condition.as_ref(),
        &JobContext::none(),
    )?;
    let flags = load_flags(&project_dir.join("flags.json")).map_err(AppError::from)?;
    let final_flag_vec = user_flags(df.height(), &flags);
    let bookmarked: Vec<bool> = (0..df.height())
        .map(|idx| flags.get(&idx).is_some_and(|entry| entry.bookmarked))
        .collect();
    let ordered: Vec<usize> = (0..df.height()).collect();
    let (filtered, _) = filtered_indices(
        &ordered,
        &final_flag_vec,
        &bookmarked,
        None,
        payload.flag_filter.as_deref(),
        search_state.mask.as_deref(),
    );
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
