//! Resolve an original row id inside the same ordered population as paging.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError, jobs::JobContext, project_io::read_project_row_count, store::ProjectsStore,
};

use super::{
    page_read::is_unconditional_page, resolve_project_target, result_cache, target::QuerySpec,
};

#[derive(Debug, Deserialize, ts_rs::TS)]
pub struct LocateRowPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    /// Zero-based original row id, matching the table's one-based `#` minus 1.
    #[serde(rename = "rowIndex")]
    pub row_index: usize,
    #[serde(flatten)]
    pub spec: QuerySpec,
}

#[derive(Debug, Serialize, PartialEq, Eq, ts_rs::TS)]
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
    mut payload: LocateRowPayload,
    job: &JobContext,
) -> Result<LocateRowResponse, AppError> {
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

    let revision = crate::storage::project_revision(&project_dir).map_err(AppError::from)?;
    let annotation_token =
        result_cache::spec_depends_on_annotations(&payload.spec).then(|| revision.to_string());
    let cache_key =
        result_cache::query_cache_key(meta.id, &payload.spec, &parquet_path, annotation_token);
    let ids = match cache_key
        .as_ref()
        .and_then(result_cache::cache_lookup_if_present)
    {
        Some(cached) => cached.ids,
        None => {
            let target = resolve_project_target(store, meta.id, &payload.spec, job)?;
            job.check_cancelled()?;
            target.ids
        }
    };

    job.check_cancelled()?;
    job.report_progress(2, Some(3), Some("Finding original row in result".into()));
    // Reuse the cached Arc<Vec<usize>> rather than retaining a second inverse
    // map. Chunk checkpoints also keep an already cached long scan cancellable.
    let mut position = None;
    if payload.row_index < total_rows {
        for (chunk_index, chunk) in ids.chunks(4096).enumerate() {
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
        total_filtered_rows: ids.len(),
    })
}
