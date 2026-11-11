use serde::Deserialize;
use uuid::Uuid;

use crate::analysis::histogram::{histogram_series_ranged, Histogram, DEFAULT_BUCKETS};
use crate::condition::Node;
use crate::error::AppError;
use crate::ioc::{cached_ioc_outcomes, load_ioc_entries};
use crate::jobs::JobContext;
use crate::project_io::{is_internal_column, read_project_dataframe};
use crate::storage::load_flags;
use crate::store::ProjectsStore;

use super::query::{filtered_indices, final_flags, prepare_search};

#[derive(Debug, Deserialize)]
pub struct HistogramPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub column: String,
    #[serde(default = "default_buckets")]
    pub buckets: u32,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
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
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| !is_internal_column(name))
        .map(|name| name.to_string())
        .collect();
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
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);
    let final_flag_vec = final_flags(df.height(), &flags, &ioc_outcomes);
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
        .map_err(|message| AppError::Message(message))
}
