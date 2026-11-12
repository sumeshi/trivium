mod ioc;
mod labels;
mod locate;
mod page;
mod page_read;
mod project_target;
mod result_cache;
mod search;
mod singleflight;
mod sort;
mod target;

use std::collections::HashMap;

pub use locate::{
    locate_project_row, locate_project_row_with_job, LocateRowPayload, LocateRowResponse,
};
pub(crate) use page::materialize_page_rows;
pub use project_target::{resolve_project_target, ResolvedProjectTarget};
pub(crate) use result_cache::cache_remove_project;
pub use result_cache::{clear_result_cache, result_cache_stats, ResultCacheStats};
pub use target::{
    reset_resolve_count, resolve_count, resolve_target, spec_token_json, QuerySpec, ResolvedTarget,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError,
    jobs::JobContext,
    models::{ColumnMeta, ProjectRow},
    project_io::{
        is_internal_column_for_version, read_page_dataframe_with_job,
        read_page_rows_dataframe_with_job, read_project_row_count, read_project_schema_columns,
        ProjectColumns, INTERNAL_ORIGINAL, INTERNAL_ROW_ID,
    },
    storage::load_page_flag_snapshot,
    store::ProjectsStore,
};

use self::page_read::{
    apply_page_colors, collect_node_columns, is_unconditional_page, load_page_color_rules,
};
use super::util::DEFAULT_PAGE_SIZE;

/// QuerySpec fields (shared contract) plus project selection and paging.
///
/// The flattened `spec` keeps the existing IPC keys (`search`,
/// `searchColumns`, `flagFilter`, `bookmarkedOnly`, `sortKey`,
/// `sortDirection`, `condition`) while sharing one resolution path.
#[derive(Debug, Deserialize, ts_rs::TS)]
#[ts(optional_fields = nullable)]
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

#[derive(Debug, Serialize, ts_rs::TS)]
pub struct QueryRowsResponse {
    pub rows: Vec<ProjectRow>,
    /// Annotated rows in the whole project (user flags only).
    pub annotated_total: usize,
    /// Annotated rows inside the current result set.
    pub annotated_filtered: usize,
    /// Rows in the whole project.
    pub total_rows: usize,
    /// Rows matching the current QuerySpec.
    pub total_filtered_rows: usize,
    pub offset: usize,
}

/// Streams project rows with filtering, sorting, user annotations, and pagination.
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
    mut payload: QueryRowsPayload,
    job: &JobContext,
) -> Result<QueryRowsResponse, AppError> {
    job.check_cancelled()?;
    let meta = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    payload.spec.resolve_search_labels(&meta.columns)?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }

    job.report_progress(0, Some(3), Some("Reading project metadata".into()));
    let schema_columns = read_project_schema_columns(&parquet_path).map_err(AppError::from)?;
    let total_rows = read_project_row_count(&parquet_path).map_err(AppError::from)?;
    let columns: Vec<String> = schema_columns
        .iter()
        .filter(|name| !is_internal_column_for_version(name, meta.storage_version))
        .cloned()
        .collect();
    let has_row_id = schema_columns.iter().any(|name| name == INTERNAL_ROW_ID);
    let has_original = schema_columns.iter().any(|name| name == INTERNAL_ORIGINAL);
    // CSV originals are arrays of source strings and CSV cells are never null,
    // so the page does not need `__original` to classify them. JSON pages do:
    // the original object separates null from a missing key and carries the
    // per-cell JSON type.
    let needs_original = has_original && meta.source_format.as_deref() != Some("csv");
    let project_columns = ProjectColumns::from_meta_and_names(&meta, &schema_columns);
    // Hidden columns are presentation-only. The page captures the visible
    // columns, then `__original` (for missing/null cell kinds) and `__rowid`
    // (to keep a non-empty projection when every data column is hidden).
    let visible_columns: Vec<ColumnMeta> = project_columns
        .columns()
        .iter()
        .filter(|column| schema_columns.iter().any(|name| name == &column.id))
        .filter(|column| {
            !meta
                .hidden_columns
                .iter()
                .any(|hidden| hidden == &column.id)
        })
        .cloned()
        .collect();

    let flags_path = store.flags_path(&meta.id);
    let color_rules = load_page_color_rules(&project_dir);
    let mut page_projection: Vec<String> = visible_columns
        .iter()
        .map(|column| column.id.clone())
        .collect();
    if needs_original {
        page_projection.push(INTERNAL_ORIGINAL.to_string());
    }
    // Color rules may read a hidden column. Add only rule columns that are not
    // already part of the visible projection; they are evaluated separately
    // and never become row data or cells.
    for rule in &color_rules {
        collect_node_columns(&rule.condition, &mut |column| {
            if columns.iter().any(|name| name.as_str() == column)
                && !page_projection.iter().any(|known| known.as_str() == column)
            {
                page_projection.push(column.to_string());
            }
        });
    }
    if has_row_id {
        page_projection.push(INTERNAL_ROW_ID.to_string());
    }

    let offset = payload.offset.unwrap_or(0);
    let limit = payload.limit.unwrap_or(DEFAULT_PAGE_SIZE).max(1);

    // An unconditional, unsorted table has a known order: original row ids.
    // It needs neither search/sort resolution nor a full frame read.
    if is_unconditional_page(&payload.spec) {
        job.report_progress(1, Some(3), Some("Reading page".into()));
        let start = offset.min(total_rows);
        let end = start.saturating_add(limit).min(total_rows);
        let row_ids: Vec<usize> = (start..end).collect();
        let page_df =
            read_page_dataframe_with_job(&parquet_path, &page_projection, start, limit, job)
                .map_err(AppError::from)?;

        let (page_flags, annotated_total, _) =
            load_page_flag_snapshot(&flags_path, &row_ids).map_err(AppError::from)?;

        job.check_cancelled()?;
        job.report_progress(2, Some(3), Some("Building page".into()));
        let mut rows = page::materialize_page_rows(
            &page_df,
            &row_ids,
            &visible_columns,
            &page_flags,
            &HashMap::new(),
            false,
        )?;
        apply_page_colors(&mut rows, &page_df, &color_rules)?;

        job.report_progress(3, Some(3), Some("Rows ready".into()));
        return Ok(QueryRowsResponse {
            rows,
            annotated_total,
            annotated_filtered: annotated_total,
            total_rows,
            total_filtered_rows: total_rows,
            offset,
        });
    }

    // The annotation revision is part of the key only when the resolved ids
    // depend on annotations. Data-only results keep their ids but compare the
    // revision on a hit to refresh the result-internal annotated count.
    let annotation_revision =
        crate::storage::project_revision(&project_dir).map_err(AppError::from)?;
    let annotation_token = if result_cache::spec_depends_on_annotations(&payload.spec) {
        Some(annotation_revision.to_string())
    } else {
        None
    };
    let cache_key =
        result_cache::query_cache_key(meta.id, &payload.spec, &parquet_path, annotation_token);

    if let Some(cached) = cache_key
        .as_ref()
        .and_then(result_cache::cache_lookup_if_present)
    {
        job.report_progress(1, Some(3), Some("Reusing query result".into()));
        let row_ids: Vec<usize> = cached
            .ids
            .iter()
            .skip(offset)
            .take(limit)
            .copied()
            .collect();
        let (page_flags, annotated_total, page_revision) =
            load_page_flag_snapshot(&flags_path, &row_ids).map_err(AppError::from)?;
        // A concurrent annotation edit must not combine old membership/counts
        // with new page flags. The shared snapshot resolver handles that case.
        let membership_current = !result_cache::spec_depends_on_annotations(&payload.spec)
            || page_revision == annotation_revision;
        if membership_current
            && (cached.ids.len() == total_rows || cached.annotation_revision == page_revision)
        {
            let page_df =
                read_page_rows_dataframe_with_job(&parquet_path, &page_projection, &row_ids, job)
                    .map_err(AppError::from)?;
            job.check_cancelled()?;
            job.report_progress(2, Some(3), Some("Building page".into()));
            let mut rows = page::materialize_page_rows(
                &page_df,
                &row_ids,
                &visible_columns,
                &page_flags,
                &HashMap::new(),
                false,
            )?;
            apply_page_colors(&mut rows, &page_df, &color_rules)?;

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
    }

    let target = resolve_project_target(store, payload.project_id, &payload.spec, job)?;
    job.check_cancelled()?;
    let filtered_ids = target.ids;
    let annotated_filtered = target.annotated_filtered;
    let annotated_total = target.annotated_total;

    let row_ids: Vec<usize> = filtered_ids
        .iter()
        .skip(offset)
        .take(limit)
        .copied()
        .collect();
    job.report_progress(2, Some(3), Some("Building page".into()));
    let page_df = read_page_rows_dataframe_with_job(&parquet_path, &page_projection, &row_ids, job)
        .map_err(AppError::from)?;
    let mut rows = page::materialize_page_rows(
        &page_df,
        &row_ids,
        &visible_columns,
        &target.flags,
        &HashMap::new(),
        false,
    )?;
    apply_page_colors(&mut rows, &page_df, &color_rules)?;

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
