use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::histogram::{histogram_series_annotated_with_job, Histogram, DEFAULT_BUCKETS};
use crate::condition::Node;
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::store::ProjectsStore;

use super::analysis_data::AnalysisSource;
use super::query::QuerySpec;

#[derive(Debug, Deserialize)]
pub struct HistogramPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub column: String,
    #[serde(default = "default_buckets")]
    pub buckets: u32,
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
    #[serde(default)]
    pub range: Option<HistogramRange>,
    #[serde(default)]
    pub focus: bool,
}

#[derive(Debug, Deserialize)]
pub struct HistogramRange {
    pub min: String,
    pub max: String,
}

fn default_buckets() -> u32 {
    DEFAULT_BUCKETS as u32
}

/// Equal-width buckets for one numeric or datetime column over the filtered rows.
pub fn histogram(store: &ProjectsStore, payload: HistogramPayload) -> Result<Histogram, AppError> {
    histogram_with_job(store, payload, &JobContext::none())
}

pub fn histogram_with_job(
    store: &ProjectsStore,
    payload: HistogramPayload,
    job: &JobContext,
) -> Result<Histogram, AppError> {
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
    let projection = source.columns(std::slice::from_ref(&payload.column), "histogram")?;
    let frame = source.read_columns(&projection, job)?;
    let series = frame
        .column(&projection[0])
        .map_err(|error| AppError::Other(error.into()))?;
    let bucket_count = if payload.buckets == 0 {
        DEFAULT_BUCKETS
    } else {
        payload.buckets as usize
    };
    let range = payload
        .range
        .as_ref()
        .map(|range| (range.min.as_str(), range.max.as_str()));
    histogram_series_annotated_with_job(
        &payload.column,
        series,
        &source.target.ids,
        bucket_count,
        range,
        &source.target.flags,
        payload.focus,
        job,
    )
    .map_err(AppError::Message)
}
