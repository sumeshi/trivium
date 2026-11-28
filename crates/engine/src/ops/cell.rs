//! On-demand full-cell fetch.
//!
//! List pages carry truncated previews. The detail dialog and Copy call this
//! operation with the project id, original row id, and column id to obtain the
//! untruncated value without reading the whole frame.

use serde::Deserialize;
use uuid::Uuid;

use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::Cell;
use crate::project_io::{is_internal_column, read_page_rows_dataframe_with_job, INTERNAL_ORIGINAL};
use crate::store::ProjectsStore;
use crate::value_utils::{build_full_cell, original_record_at};

#[derive(Debug, Deserialize)]
pub struct GetCellPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    /// Original row id, not a position in the current query result.
    #[serde(rename = "rowIndex")]
    pub row_index: usize,
    #[serde(rename = "columnId")]
    pub column_id: String,
}

/// Full cell value. The shape matches the list `Cell` DTO so the frontend can
/// reuse its display/copy rules; `text` is never truncated here.
pub type GetCellResponse = Cell;

/// Reads one cell with its full text.
///
/// Only the requested column and, for JSON, `__original` are projected.
/// Hidden data columns are valid; internal storage columns are not.
pub fn get_cell(
    store: &ProjectsStore,
    payload: GetCellPayload,
) -> Result<GetCellResponse, AppError> {
    get_cell_with_job(store, payload, &JobContext::none())
}

pub fn get_cell_with_job(
    store: &ProjectsStore,
    payload: GetCellPayload,
    job: &JobContext,
) -> Result<GetCellResponse, AppError> {
    job.check_cancelled()?;
    let meta = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    if is_internal_column(&payload.column_id) {
        return Err(AppError::Message("Column not found.".into()));
    }
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    if payload.row_index >= meta.total_records {
        return Err(AppError::Message("Row not found.".into()));
    }

    let column = meta
        .columns
        .iter()
        .find(|column| column.id == payload.column_id)
        .ok_or_else(|| AppError::Message("Column not found.".into()))?;

    let mut projection = vec![payload.column_id.clone()];
    if meta.source_format.as_deref() != Some("csv") {
        projection.push(INTERNAL_ORIGINAL.to_string());
    }
    let df =
        read_page_rows_dataframe_with_job(&parquet_path, &projection, &[payload.row_index], job)
            .map_err(AppError::from)?;
    if df.height() != 1 {
        return Err(AppError::Message("Row not found.".into()));
    }

    let value = df
        .column(&payload.column_id)
        .ok()
        .and_then(|series| series.get(0).ok())
        .unwrap_or(polars::prelude::AnyValue::Null);
    let original = original_record_at(
        df.column(INTERNAL_ORIGINAL)
            .ok()
            .map(|column| column.as_materialized_series()),
        0,
    );
    let cell = build_full_cell(&value, original.as_ref(), &column.source);
    job.check_cancelled()?;
    Ok(cell)
}
