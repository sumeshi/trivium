use std::{collections::HashMap, sync::Arc};

use polars::prelude::{DataFrame, DataType, Series};
use uuid::Uuid;

use super::{
    page::{flag_population, is_unrestricted_flag_filter},
    page_read::{is_source_order_unsearched, is_unconditional_page, resolution_projection},
    result_cache::{self, CachedResult},
    singleflight,
    target::{resolve_target, QuerySpec},
};
use crate::{
    error::AppError,
    jobs::JobContext,
    models::FlagEntry,
    project_io::{
        is_internal_column_for_version, read_project_columns_with_job, read_project_row_count,
        read_project_schema_columns, INTERNAL_ROW_ID,
    },
    storage::load_flag_snapshot,
    store::ProjectsStore,
};

/// An operation-scoped snapshot. Only IDs/counts enter the bounded result
/// cache; full annotation snapshots live only as long as the caller needs them.
pub struct ResolvedProjectTarget {
    pub total_rows: usize,
    pub columns: Vec<String>,
    pub ids: Arc<Vec<usize>>,
    pub annotated_total: usize,
    pub annotated_filtered: usize,
    pub flags: Arc<HashMap<usize, FlagEntry>>,
    pub annotation_revision: u64,
}

pub fn resolve_project_target(
    store: &ProjectsStore,
    project_id: Uuid,
    spec: &QuerySpec,
    job: &JobContext,
) -> Result<ResolvedProjectTarget, AppError> {
    job.check_cancelled()?;
    let meta = store
        .try_find(&project_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let mut spec = spec.clone();
    spec.resolve_search_labels(&meta.columns)?;
    let path = store.project_dir(&project_id).join("data.parquet");
    if !path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let schema = read_project_schema_columns(&path).map_err(AppError::from)?;
    let total_rows = read_project_row_count(&path).map_err(AppError::from)?;
    let columns: Vec<String> = schema
        .iter()
        .filter(|name| !is_internal_column_for_version(name, meta.storage_version))
        .cloned()
        .collect();
    let (flags, revision) =
        load_flag_snapshot(&store.flags_path(&project_id)).map_err(AppError::from)?;
    let flags = Arc::new(flags);
    let annotated_total = flags
        .iter()
        .filter(|(index, entry)| **index < total_rows && flag_population(&entry.flag) != 8)
        .count();
    if is_unconditional_page(&spec) {
        job.check_cancelled()?;
        return Ok(ResolvedProjectTarget {
            total_rows,
            columns,
            ids: Arc::new((0..total_rows).collect()),
            annotated_total,
            annotated_filtered: annotated_total,
            flags,
            annotation_revision: revision,
        });
    }
    let token = result_cache::spec_depends_on_annotations(&spec).then(|| revision.to_string());
    let key = result_cache::query_cache_key(project_id, &spec, &path, token);
    let result = singleflight::resolve_once(key.clone(), job, || {
        let mut projection = resolution_projection(
            &spec,
            &columns,
            schema.iter().any(|name| name == INTERNAL_ROW_ID),
        );
        job.report_progress(1, Some(3), Some("Evaluating query".into()));
        let frame = if is_source_order_unsearched(&spec) {
            // Flags/bookmarks are sparse metadata. A null anchor retains the
            // row count without reading or allocating any N-row source column.
            DataFrame::new(vec![Series::full_null(
                INTERNAL_ROW_ID,
                total_rows,
                &DataType::Null,
            )])
            .map_err(|error| AppError::from(anyhow::Error::from(error)))?
        } else {
            // Genuine legacy Parquet can omit __rowid; even a constant empty
            // group predicate still needs an existing column as row anchor.
            if projection.is_empty() {
                projection.extend(schema.first().cloned());
            }
            read_project_columns_with_job(&path, &projection, job).map_err(AppError::from)?
        };
        let target = resolve_target(&frame, &spec, &columns, &flags, job)?;
        job.check_cancelled()?;
        if store
            .try_find(&project_id)
            .map_err(AppError::from)?
            .is_none()
        {
            return Err(AppError::Message("Project not found.".into()));
        }
        Ok(CachedResult::new(
            target.filtered,
            target.annotated_filtered,
            target.annotated_total,
            revision,
        ))
    })?;
    let annotated_filtered = if result.ids.len() == total_rows
        && is_unrestricted_flag_filter(spec.flag_filter.as_deref())
    {
        annotated_total
    } else if result.annotation_revision == revision {
        result.annotated_filtered
    } else {
        result
            .ids
            .iter()
            .filter(|index| {
                flags
                    .get(index)
                    .is_some_and(|entry| flag_population(&entry.flag) != 8)
            })
            .count()
    };
    if result.annotation_revision != revision {
        if let Some(key) = key.as_ref() {
            result_cache::cache_refresh_counts(key, revision, annotated_total, annotated_filtered);
        }
    }
    Ok(ResolvedProjectTarget {
        total_rows,
        columns,
        ids: result.ids,
        annotated_total,
        annotated_filtered,
        flags,
        annotation_revision: revision,
    })
}
