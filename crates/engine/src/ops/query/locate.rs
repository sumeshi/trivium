//! Resolve an original row id inside the same ordered population as paging.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError,
    jobs::JobContext,
    project_io::{
        is_internal_column_for_version, read_project_columns, read_project_row_count,
        read_project_schema_columns, INTERNAL_ROW_ID,
    },
    storage::load_flags,
    store::ProjectsStore,
};

use super::{
    page_read::{is_unconditional_page, resolution_projection},
    result_cache::{self, CachedResult},
    target::{resolve_target, QuerySpec},
};

#[derive(Debug, Deserialize)]
pub struct LocateRowPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    /// Zero-based original row id, matching the table's one-based `#` minus 1.
    #[serde(rename = "rowIndex")]
    pub row_index: usize,
    #[serde(flatten)]
    pub spec: QuerySpec,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct LocateRowResponse {
    /// Zero-based position inside the current filtered/sorted result, if any.
    pub position: Option<usize>,
    pub total_rows: usize,
    pub total_filtered_rows: usize,
}

pub fn locate_project_row(
    store: &ProjectsStore,
    payload: LocateRowPayload,
) -> Result<LocateRowResponse, AppError> {
    locate_project_row_with_job(store, payload, &JobContext::none())
}

/// Lookup transfers only a position/counts, never page data or all resolved ids.
pub fn locate_project_row_with_job(
    store: &ProjectsStore,
    payload: LocateRowPayload,
    job: &JobContext,
) -> Result<LocateRowResponse, AppError> {
    job.check_cancelled()?;
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }

    job.report_progress(0, Some(3), Some("Reading project metadata".into()));
    let total_rows = read_project_row_count(&parquet_path).map_err(AppError::from)?;
    // The ordinary source-order view needs no schema scan or resolved-id vector.
    if is_unconditional_page(&payload.spec) {
        job.check_cancelled()?;
        job.report_progress(3, Some(3), Some("Row location ready".into()));
        return Ok(LocateRowResponse {
            position: (payload.row_index < total_rows).then_some(payload.row_index),
            total_rows,
            total_filtered_rows: total_rows,
        });
    }

    let revision = store.current_revision(&meta.id);
    let annotation_token =
        result_cache::spec_depends_on_annotations(&payload.spec).then(|| revision.to_string());
    let cache_key =
        result_cache::query_cache_key(meta.id, &payload.spec, &parquet_path, annotation_token);
    let result = match cache_key.as_ref().and_then(result_cache::cache_lookup) {
        Some(cached) => cached,
        None => {
            // Exactly the same projection and resolver used by query_project_rows.
            // A miss warms the shared page cache without materializing any rows.
            job.report_progress(1, Some(3), Some("Evaluating query".into()));
            let schema = read_project_schema_columns(&parquet_path).map_err(AppError::from)?;
            let columns: Vec<String> = schema
                .iter()
                .filter(|name| !is_internal_column_for_version(name, meta.storage_version))
                .cloned()
                .collect();
            let has_row_id = schema.iter().any(|name| name == INTERNAL_ROW_ID);
            let projection = resolution_projection(&payload.spec, &columns, has_row_id);
            let frame = read_project_columns(&parquet_path, &projection).map_err(AppError::from)?;
            let flags = load_flags(&store.flags_path(&meta.id)).map_err(AppError::from)?;
            let target = resolve_target(&frame, &payload.spec, &columns, &flags, job)?;
            job.check_cancelled()?;
            let cached = CachedResult::new(
                target.filtered,
                target.annotated_filtered,
                target.annotated_total,
                revision,
            );
            if let Some(key) = cache_key {
                result_cache::cache_store(key, cached.clone());
            }
            cached
        }
    };

    job.check_cancelled()?;
    job.report_progress(2, Some(3), Some("Finding original row in result".into()));
    // Reuse the cached Arc<Vec<usize>> rather than retaining a second inverse
    // map. Chunk checkpoints also keep an already cached long scan cancellable.
    let mut position = None;
    if payload.row_index < total_rows {
        for (chunk_index, chunk) in result.ids.chunks(4096).enumerate() {
            job.check_cancelled()?;
            if let Some(index) = chunk.iter().position(|id| *id == payload.row_index) {
                position = Some(chunk_index * 4096 + index);
                break;
            }
        }
    }
    job.check_cancelled()?;
    job.report_progress(3, Some(3), Some("Row location ready".into()));
    Ok(LocateRowResponse {
        position,
        total_rows,
        total_filtered_rows: result.ids.len(),
    })
}
