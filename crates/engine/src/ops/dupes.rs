use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::analysis::dupes::DuplicateIndex;
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::ProjectRow;
use crate::project_io::{
    is_internal_column, read_project_schema_columns, ProjectBatchReader, ProjectColumns,
};
use crate::storage::load_flags;
use crate::store::ProjectsStore;
use polars::prelude::*;

use super::query::materialize_page_rows;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DupeMode {
    Duplicates,
    Unique,
}

#[derive(Debug, Deserialize)]
pub struct DupesPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub columns: Vec<String>,
    pub mode: DupeMode,
}

#[derive(Debug, Serialize)]
pub struct DupesResponse {
    pub rows: Vec<ProjectRow>,
}

/// Rows whose key is repeated, or rows whose key appears once. Repeated keys keep every row.
pub fn duplicate_rows(
    store: &ProjectsStore,
    payload: DupesPayload,
) -> Result<DupesResponse, AppError> {
    duplicate_rows_with_job(store, payload, &JobContext::none())
}

pub fn duplicate_rows_with_job(
    store: &ProjectsStore,
    payload: DupesPayload,
    job: &JobContext,
) -> Result<DupesResponse, AppError> {
    job.check_cancelled()?;
    if payload.columns.is_empty()
        || payload
            .columns
            .iter()
            .any(|column| is_internal_column(column))
    {
        return Err(AppError::Message("Column not found.".into()));
    }
    let meta = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let schema = read_project_schema_columns(&parquet_path).map_err(AppError::from)?;
    if payload.columns.iter().any(|name| !schema.contains(name)) {
        return Err(AppError::Message("Column not found.".into()));
    }
    let mut projection = payload.columns.clone();
    projection.sort();
    projection.dedup();
    let mut keys = ProjectBatchReader::open(&parquet_path, &projection).map_err(AppError::from)?;
    let mut index = DuplicateIndex::default();
    while let Some(batch) = keys.next_with_job(job) {
        let batch = batch.map_err(AppError::from)?;
        let columns = payload
            .columns
            .iter()
            .map(|name| batch.frame.column(name).expect("validated key column"))
            .collect::<Vec<_>>();
        index.push(&columns, batch.offset, job)?;
        job.report_progress(
            (batch.offset + batch.frame.height()) as u64,
            Some(keys.total_rows() as u64),
            Some("Finding duplicate keys".into()),
        );
    }
    let indices = index.finish(payload.mode == DupeMode::Duplicates, job)?;
    let project_columns = ProjectColumns::from_meta_and_names(&meta, &schema);
    let flags = load_flags(&store.flags_path(&meta.id)).map_err(AppError::from)?;
    // Keys use only projected columns. Full row payloads are decoded one bounded
    // batch at a time and retain their original source IDs.
    let mut reader =
        ProjectBatchReader::for_rows(&parquet_path, &schema, &indices).map_err(AppError::from)?;
    let mut rows = Vec::with_capacity(indices.len());
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch.map_err(AppError::from)?;
        let start = indices.partition_point(|id| *id < batch.offset);
        let end = indices.partition_point(|id| *id < batch.offset + batch.frame.height());
        let selected = &indices[start..end];
        if !selected.is_empty() {
            let local = selected
                .iter()
                .map(|id| (id - batch.offset) as u32)
                .collect::<Vec<_>>();
            let frame = batch
                .frame
                .take(&UInt32Chunked::from_vec("rows", local))
                .map_err(|error| AppError::from(anyhow::Error::from(error)))?;
            rows.extend(materialize_page_rows(
                &frame,
                selected,
                project_columns.columns(),
                &flags,
                &HashMap::new(),
                false,
            )?);
        }
        if indices[end..]
            .first()
            .is_none_or(|id| *id >= reader.current_row_group_end())
        {
            reader.skip_current_row_group();
        }
    }
    job.check_cancelled()?;
    Ok(DupesResponse { rows })
}
