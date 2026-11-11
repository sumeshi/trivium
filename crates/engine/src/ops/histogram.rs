use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::histogram::{histogram_series_ranged, Histogram, DEFAULT_BUCKETS};
use crate::condition::Node;
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::project_io::{is_internal_column_for_version, read_project_dataframe};
use crate::storage::load_flags;
use crate::store::ProjectsStore;

use super::query::{resolve_target, QuerySpec};

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
    let flags = load_flags(&project_dir.join("flags.json")).map_err(AppError::from)?;
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
    let bucket_count = if payload.buckets == 0 {
        DEFAULT_BUCKETS
    } else {
        payload.buckets as usize
    };
    let range = payload
        .range
        .as_ref()
        .map(|range| (range.min.as_str(), range.max.as_str()));
    histogram_series_ranged(&payload.column, series, &filtered, bucket_count, range)
        .map_err(AppError::Message)
}
