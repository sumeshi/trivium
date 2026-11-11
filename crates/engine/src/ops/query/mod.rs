mod ioc;
mod page;
mod search;
mod sort;
mod target;

pub(crate) use ioc::final_flags;
pub(crate) use page::{filtered_indices, materialize_page};
pub(crate) use search::{persist_search_cache, prepare_search};
pub(crate) use sort::ordered_row_indices;
pub use target::{
    data_columns, resolve_target, spec_token_json, QuerySpec, ResolvedTarget,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError,
    ioc::{cached_ioc_outcomes, load_ioc_entries},
    jobs::JobContext,
    models::ProjectRow,
    project_io::{read_project_dataframe, ProjectColumns},
    storage::{load_color_rules, load_flags},
    store::ProjectsStore,
};

use super::util::{matched_rule_color, DEFAULT_PAGE_SIZE};

/// QuerySpec fields (shared contract) plus project selection and paging.
///
/// The flattened `spec` keeps the existing IPC keys (`search`,
/// `searchColumns`, `flagFilter`, `bookmarkedOnly`, `sortKey`,
/// `sortDirection`, `condition`) while sharing one resolution path.
#[derive(Debug, Deserialize)]
pub struct QueryRowsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    #[serde(flatten)]
    pub spec: QuerySpec,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct QueryRowsResponse {
    pub rows: Vec<ProjectRow>,
    /// Annotated rows in the whole project (user flags and IOC matches).
    pub annotated_total: usize,
    /// Annotated rows inside the current result set.
    pub annotated_filtered: usize,
    /// Rows in the whole project.
    pub total_rows: usize,
    /// Rows matching the current QuerySpec.
    pub total_filtered_rows: usize,
    pub offset: usize,
}

/// Streams project rows with filtering, sorting, IOC application, and pagination.
pub fn query_project_rows(
    store: &ProjectsStore,
    payload: QueryRowsPayload,
) -> Result<QueryRowsResponse, AppError> {
    query_project_rows_with_job(store, payload, &JobContext::none())
}

/// [`query_project_rows`] with cancellation checkpoints and progress reporting.
///
/// The search and sort stages are single library calls in T15, so cancellation
/// is checked around them. Batch-level interruption is added by T18.
pub fn query_project_rows_with_job(
    store: &ProjectsStore,
    payload: QueryRowsPayload,
    job: &JobContext,
) -> Result<QueryRowsResponse, AppError> {
    job.check_cancelled()?;
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()).into());
    }

    job.report_progress(0, Some(3), Some("Reading project data".into()));
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    job.check_cancelled()?;
    let columns = data_columns(&df);
    let project_columns = ProjectColumns::from_meta(&meta, &df);

    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let offset = payload.offset.unwrap_or(0);
    let limit = payload.limit.unwrap_or(DEFAULT_PAGE_SIZE).max(1);
    let total_rows = df.height();

    let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);
    job.report_progress(1, Some(3), Some("Evaluating query".into()));
    let target = resolve_target(
        &df,
        &project_dir,
        &payload.spec,
        &columns,
        &flags,
        &ioc_outcomes,
    )?;
    job.check_cancelled()?;
    let selected_indices: Vec<usize> = target
        .filtered
        .iter()
        .skip(offset)
        .take(limit)
        .copied()
        .collect();
    job.report_progress(2, Some(3), Some("Building page".into()));
    let mut rows = page::materialize_page(
        &df,
        &selected_indices,
        project_columns.columns(),
        &flags,
        &ioc::final_flags(df.height(), &flags, &ioc_outcomes),
        &ioc_outcomes,
        !iocs.is_empty(),
    )?;

    // Colors are decorative: a missing or malformed colors.json must never
    // fail a query, so fall back to "no rules" instead of propagating.
    let color_rules = match load_color_rules(&project_dir) {
        Ok(rules) => rules.unwrap_or_default(),
        Err(err) => {
            eprintln!(
                "[colors] failed to load color rules for {:?}: {:?}",
                project_dir, err
            );
            Vec::new()
        }
    };
    for row in &mut rows {
        row.color = matched_rule_color(&row.data, &color_rules)?;
    }

    job.report_progress(3, Some(3), Some("Rows ready".into()));

    Ok(QueryRowsResponse {
        rows,
        annotated_total: target.annotated_total,
        annotated_filtered: target.annotated_filtered,
        total_rows,
        total_filtered_rows: target.filtered.len(),
        offset,
    })
}
