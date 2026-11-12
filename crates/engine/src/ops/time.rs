use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::time::{analyze_series_with_job, TimeAnalysis};
use crate::condition::Node;
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::store::ProjectsStore;

use super::analysis_data::AnalysisSource;
use super::query::QuerySpec;

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
    time_analysis_with_job(store, payload, &JobContext::none())
}

pub fn time_analysis_with_job(
    store: &ProjectsStore,
    payload: TimeAnalysisPayload,
    job: &JobContext,
) -> Result<TimeAnalysis, AppError> {
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
    let source = AnalysisSource::open(store, payload.project_id, &spec, job)?;
    let projection = source.columns(std::slice::from_ref(&payload.column), "time analysis")?;
    let frame = source.read_columns(&projection, job)?;
    let series = frame
        .column(&projection[0])
        .map_err(|error| AppError::Other(error.into()))?;
    analyze_series_with_job(
        &payload.column,
        series,
        &source.target.ids,
        payload.gap_seconds,
        payload.session_minutes,
        payload.burst_window_seconds,
        payload.burst_min,
        job,
    )
    .map_err(AppError::Message)
}
