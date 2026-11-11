mod ioc;
mod page;
mod result_cache;
mod search;
mod sort;
mod target;

use std::collections::HashMap;
use std::path::Path;

pub(crate) use ioc::final_flags;
pub(crate) use page::{filtered_indices, materialize_page};
pub(crate) use result_cache::cache_remove_project;
pub use result_cache::{clear_result_cache, result_cache_stats, ResultCacheStats};
pub(crate) use search::prepare_search;
pub(crate) use sort::ordered_row_indices;
pub use target::{
    data_columns, reset_resolve_count, resolve_count, resolve_target, spec_token_json, QuerySpec,
    ResolvedTarget,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError,
    ioc::{cached_ioc_outcomes, load_ioc_entries},
    jobs::JobContext,
    models::{FlagEntry, IocEntry, ProjectRow},
    project_io::{read_project_dataframe, ProjectColumns},
    storage::{load_color_rules, load_flag, load_flags},
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
/// A repeated page request for the same normalized spec reuses the resolved
/// original row ids. Search and sort run only when the cache misses.
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
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let offset = payload.offset.unwrap_or(0);
    let limit = payload.limit.unwrap_or(DEFAULT_PAGE_SIZE).max(1);
    let total_rows = df.height();

    // The annotation revision is part of the key only when the resolved ids
    // depend on annotations. IOC definitions also feed flag-filter results, so
    // their signature joins the annotation token when it is present.
    let annotation_token = if result_cache::spec_depends_on_annotations(&payload.spec) {
        Some(format!(
            "{}|{}",
            store.current_revision(&meta.id),
            ioc_signature(&iocs)
        ))
    } else {
        None
    };
    let cache_key = result_cache::query_cache_key(
        meta.id,
        &payload.spec,
        &parquet_path,
        annotation_token,
    );

    if let Some(cached) = cache_key.as_ref().and_then(result_cache::cache_lookup) {
        job.report_progress(1, Some(3), Some("Reusing query result".into()));
        let selected_indices: Vec<usize> = cached
            .ids
            .iter()
            .skip(offset)
            .take(limit)
            .copied()
            .collect();
        // Only the page's annotations are read on a hit; no project-wide flag
        // or bookmark vector is rebuilt.
        let page_flags = load_page_flags(&flags_path, &selected_indices)?;
        let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);
        job.check_cancelled()?;
        job.report_progress(2, Some(3), Some("Building page".into()));
        let mut rows = page::materialize_page(
            &df,
            &selected_indices,
            project_columns.columns(),
            &page_flags,
            &ioc_outcomes,
            !iocs.is_empty(),
        )?;
        apply_colors(&mut rows, &project_dir)?;

        // Without IOC definitions the project's flagged count is exactly the
        // number of annotated rows, and `update_flag` keeps it current. With
        // IOCs, the cached count from the miss is retained (see the T19 notes
        // in docs/manual-review-progress.md).
        let annotated_total = if iocs.is_empty() {
            meta.flagged_records
        } else {
            cached.annotated_total
        };
        // A result that contains every row has the same annotated count as the
        // project. This keeps the common unfiltered table consistent with
        // `annotated_total` after a flag update; a filtered data-only result
        // cannot be recounted without loading every annotation, so it keeps
        // the value from the miss.
        let annotated_filtered = if cached.ids.len() == total_rows {
            annotated_total
        } else {
            cached.annotated_filtered
        };

        job.report_progress(3, Some(3), Some("Rows ready".into()));
        return Ok(QueryRowsResponse {
            rows,
            annotated_total,
            annotated_filtered,
            total_rows,
            total_filtered_rows: cached.ids.len(),
            offset,
        });
    }

    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);
    job.report_progress(1, Some(3), Some("Evaluating query".into()));
    let target = resolve_target(
        &df,
        &payload.spec,
        &columns,
        &flags,
        &ioc_outcomes,
        job,
    )?;
    job.check_cancelled()?;

    let cached = result_cache::CachedResult::new(
        target.filtered,
        target.annotated_filtered,
        target.annotated_total,
    );
    let filtered_ids = std::sync::Arc::clone(&cached.ids);
    let annotated_filtered = cached.annotated_filtered;
    let annotated_total = cached.annotated_total;
    if let Some(key) = cache_key {
        result_cache::cache_store(key, cached);
    }

    let selected_indices: Vec<usize> = filtered_ids
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
        &ioc_outcomes,
        !iocs.is_empty(),
    )?;
    apply_colors(&mut rows, &project_dir)?;

    job.report_progress(3, Some(3), Some("Rows ready".into()));

    Ok(QueryRowsResponse {
        rows,
        annotated_total,
        annotated_filtered,
        total_rows,
        total_filtered_rows: filtered_ids.len(),
        offset,
    })
}

fn load_page_flags(
    flags_path: &Path,
    row_indices: &[usize],
) -> Result<HashMap<usize, FlagEntry>, AppError> {
    let mut flags = HashMap::with_capacity(row_indices.len());
    for &row_index in row_indices {
        if let Some(entry) = load_flag(flags_path, row_index).map_err(AppError::from)? {
            flags.insert(row_index, entry);
        }
    }
    Ok(flags)
}

fn apply_colors(rows: &mut [ProjectRow], project_dir: &Path) -> Result<(), AppError> {
    // Colors are decorative: a missing or malformed colors.json must never
    // fail a query, so fall back to "no rules" instead of propagating.
    let color_rules = match load_color_rules(project_dir) {
        Ok(rules) => rules.unwrap_or_default(),
        Err(err) => {
            eprintln!(
                "[colors] failed to load color rules for {:?}: {:?}",
                project_dir, err
            );
            Vec::new()
        }
    };
    for row in rows.iter_mut() {
        row.color = matched_rule_color(&row.data, &color_rules)?;
    }
    Ok(())
}

fn ioc_signature(iocs: &[IocEntry]) -> String {
    serde_json::to_string(iocs).unwrap_or_default()
}
