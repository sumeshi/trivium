//! On-demand full-cell fetch for T20c.
//!
//! List pages carry truncated previews. The detail dialog and Copy call this
//! operation with the project id, original row id, and column id to obtain the
//! untruncated value without reading the whole frame.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::AppError;
use crate::models::Cell;
use crate::project_io::{
    is_internal_column_for_version, read_page_rows_dataframe, read_project_schema_columns,
    ProjectColumns, INTERNAL_ORIGINAL,
};
use crate::store::ProjectsStore;
use crate::value_utils::{original_record_at, CellColumns};

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
#[derive(Debug, Clone, Serialize)]
pub struct GetCellResponse {
    pub text: Option<String>,
    pub kind: String,
    pub truncated: bool,
    pub length: Option<usize>,
}

impl From<Cell> for GetCellResponse {
    fn from(cell: Cell) -> Self {
        Self {
            text: cell.text,
            kind: cell.kind,
            truncated: cell.truncated,
            length: cell.length,
        }
    }
}

/// Reads one cell with its full text.
///
/// Only the requested column and `__original` are projected, and only the row
/// range up to `row_index` is read through the page reader. Hidden data
/// columns are valid; internal storage columns are not.
pub fn get_cell(
    store: &ProjectsStore,
    payload: GetCellPayload,
) -> Result<GetCellResponse, AppError> {
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    if is_internal_column_for_version(&payload.column_id, meta.storage_version) {
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

    // The footer is enough to validate the column; no rows are materialized.
    let schema_columns = read_project_schema_columns(&parquet_path).map_err(AppError::from)?;
    let project_columns = ProjectColumns::from_meta_and_names(&meta, &schema_columns);
    let column = project_columns
        .columns()
        .iter()
        .find(|column| column.id == payload.column_id)
        .filter(|column| schema_columns.iter().any(|name| name == &column.id))
        .ok_or_else(|| AppError::Message("Column not found.".into()))?;

    let has_original = schema_columns.iter().any(|name| name == INTERNAL_ORIGINAL);
    let mut projection = vec![payload.column_id.clone()];
    if has_original {
        projection.push(INTERNAL_ORIGINAL.to_string());
    }
    let df = read_page_rows_dataframe(&parquet_path, &projection, &[payload.row_index])
        .map_err(AppError::from)?;
    if df.height() != 1 {
        return Err(AppError::Message("Row not found.".into()));
    }

    let columns = std::slice::from_ref(column);
    let cell_columns = CellColumns::new(&df, columns);
    let value = df
        .column(&payload.column_id)
        .ok()
        .and_then(|series| series.get(0).ok())
        .unwrap_or(polars::prelude::AnyValue::Null);
    let original = original_record_at(&df, 0);
    let cell = cell_columns.build_full(&payload.column_id, &value, original.as_ref());
    Ok(cell.into())
}
