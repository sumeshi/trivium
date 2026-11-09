mod ioc;
mod page;
mod search;
mod sort;

pub(crate) use ioc::final_flags;
pub(crate) use page::{filtered_indices, materialize_page};
pub(crate) use search::prepare_search;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError,
    ioc::{cached_ioc_outcomes, load_ioc_entries},
    models::ProjectRow,
    project_io::read_project_dataframe,
    storage::load_flags,
    store::ProjectsStore,
};

use super::util::DEFAULT_PAGE_SIZE;

#[derive(Debug, Deserialize)]
pub struct QueryRowsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(rename = "sortKey", default)]
    pub sort_key: Option<String>,
    #[serde(rename = "sortDirection", default)]
    pub sort_direction: Option<String>,
    /// Structured filter. Combined with `search` by AND. Absent means no extra filter.
    #[serde(default)]
    pub condition: Option<crate::condition::Node>,
}

#[derive(Debug, Serialize)]
pub struct QueryRowsResponse {
    pub rows: Vec<ProjectRow>,
    pub total_flagged: usize,
    pub total_rows: usize,
    pub total_filtered_rows: usize,
    pub offset: usize,
}

/// Streams project rows with filtering, sorting, IOC application, and pagination.
pub fn query_project_rows(
    store: &ProjectsStore,
    payload: QueryRowsPayload,
) -> Result<QueryRowsResponse, AppError> {
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()).into());
    }

    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| name != &"__rowid")
        .map(|name| name.to_string())
        .collect();

    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let offset = payload.offset.unwrap_or(0);
    let limit = payload.limit.unwrap_or(DEFAULT_PAGE_SIZE).max(1);
    let total_rows = df.height();

    let search_state = search::prepare_search(
        &df,
        &project_dir,
        payload.columns.as_deref(),
        &columns,
        payload.search.as_deref(),
        payload.condition.as_ref(),
    );
    let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);
    let final_flag_vec = ioc::final_flags(df.height(), &flags, &ioc_outcomes);
    let ordered = sort::ordered_row_indices(
        &df,
        payload.sort_key.as_deref(),
        payload.sort_direction.as_deref(),
    );
    let (filtered_indices, total_flagged) = page::filtered_indices(
        &ordered,
        &final_flag_vec,
        payload.flag_filter.as_deref(),
        search_state.mask.as_deref(),
    );
    let selected_indices: Vec<usize> = filtered_indices
        .iter()
        .skip(offset)
        .take(limit)
        .copied()
        .collect();
    let rows = page::materialize_page(
        &df,
        &selected_indices,
        &columns,
        &flags,
        &final_flag_vec,
        &ioc_outcomes,
        !iocs.is_empty(),
    )?;

    search::persist_search_cache(&project_dir, &search_state);

    Ok(QueryRowsResponse {
        rows,
        total_flagged,
        total_rows,
        total_filtered_rows: filtered_indices.len(),
        offset,
    })
}
