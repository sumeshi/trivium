use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::analysis::infer::{dtype_kind, parse_datetime_nanos, ColumnKind, KindFlags};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::ProjectRow;
use crate::project_io::{
    is_internal_column, read_page_rows_dataframe_with_job, read_project_schema_columns,
    ProjectBatchReader, ProjectColumns,
};
use crate::storage::load_flags;
use crate::store::ProjectsStore;
use crate::value_utils::anyvalue_to_json;

use super::query::materialize_page_rows;

/// `neighbors` takes `count` parsed rows before and after the anchor in time order.
/// `window` takes parsed rows within `count` seconds of the anchor, inclusive.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SurroundMode {
    Neighbors,
    Window,
}

#[derive(Debug, Deserialize)]
pub struct SurroundingRowsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    #[serde(rename = "rowIndex")]
    pub row_index: usize,
    pub column: String,
    pub mode: SurroundMode,
    pub count: u64,
}

#[derive(Debug, Serialize)]
pub struct SurroundingRowsResponse {
    pub rows: Vec<ProjectRow>,
}

struct TimedRow {
    index: usize,
    nanos: i64,
}

/// Full rows around one anchor, including hidden columns, ordered by `column`.
pub fn surrounding_rows(
    store: &ProjectsStore,
    payload: SurroundingRowsPayload,
) -> Result<SurroundingRowsResponse, AppError> {
    surrounding_rows_with_job(store, payload, &JobContext::none())
}

pub fn surrounding_rows_with_job(
    store: &ProjectsStore,
    payload: SurroundingRowsPayload,
    job: &JobContext,
) -> Result<SurroundingRowsResponse, AppError> {
    job.check_cancelled()?;
    if is_internal_column(&payload.column) {
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
    if !schema.contains(&payload.column) {
        return Err(AppError::Message("Column not found.".into()));
    }
    let mut reader = ProjectBatchReader::open(&parquet_path, std::slice::from_ref(&payload.column))
        .map_err(AppError::from)?;
    if payload.row_index >= reader.total_rows() {
        return Err(AppError::Message("Row not found.".into()));
    }
    let project_columns = ProjectColumns::from_meta_and_names(&meta, &schema);
    let mut timed = Vec::new();
    let mut kind = None;
    let mut inferred = KindFlags::default();
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch.map_err(AppError::from)?;
        let series = batch
            .frame
            .column(&payload.column)
            .expect("validated time column");
        kind = dtype_kind(series.dtype());
        for index in 0..series.len() {
            if index % 1024 == 0 {
                job.check_cancelled()?;
            }
            if kind.is_none() {
                if let Ok(value) = series.get(index) {
                    inferred.observe(&value);
                }
            }
            if let Some(nanos) = nanos_at(series, index) {
                timed.push(TimedRow {
                    index: batch.offset + index,
                    nanos,
                });
            }
        }
        job.report_progress(
            (batch.offset + batch.frame.height()) as u64,
            Some(reader.total_rows() as u64),
            Some("Reading surrounding timestamps".into()),
        );
    }
    if kind.unwrap_or_else(|| inferred.finish()) != ColumnKind::Datetime {
        return Err(AppError::Message("Column is not datetime.".into()));
    }
    job.check_cancelled()?;
    timed.sort_by(|left, right| {
        left.nanos
            .cmp(&right.nanos)
            .then(left.index.cmp(&right.index))
    });
    job.check_cancelled()?;
    let anchor_at = timed
        .iter()
        .position(|row| row.index == payload.row_index)
        .ok_or_else(|| AppError::Message("Anchor row has no datetime.".into()))?;
    let anchor_nanos = timed[anchor_at].nanos;
    let selected: Vec<usize> = match payload.mode {
        SurroundMode::Neighbors => {
            let count = usize::try_from(payload.count).unwrap_or(usize::MAX);
            let start = anchor_at.saturating_sub(count);
            let end = anchor_at.saturating_add(count).min(timed.len() - 1);
            timed[start..=end].iter().map(|row| row.index).collect()
        }
        SurroundMode::Window => {
            let window = i128::from(payload.count) * 1_000_000_000;
            timed
                .iter()
                .filter(|row| (i128::from(row.nanos) - i128::from(anchor_nanos)).abs() <= window)
                .map(|row| row.index)
                .collect()
        }
    };
    job.check_cancelled()?;
    let selected_frame = read_page_rows_dataframe_with_job(&parquet_path, &schema, &selected, job)
        .map_err(AppError::from)?;
    let flags = load_flags(&store.flags_path(&meta.id)).map_err(AppError::from)?;
    let mut rows = Vec::with_capacity(selected.len());
    for (batch, ids) in selected
        .chunks(crate::project_io::PROJECT_BATCH_ROWS)
        .enumerate()
    {
        job.check_cancelled()?;
        let frame = selected_frame.slice(
            (batch * crate::project_io::PROJECT_BATCH_ROWS) as i64,
            ids.len(),
        );
        rows.extend(materialize_page_rows(
            &frame,
            ids,
            project_columns.columns(),
            &flags,
            &HashMap::new(),
            false,
        )?);
    }
    job.check_cancelled()?;
    Ok(SurroundingRowsResponse { rows })
}

fn nanos_at(series: &polars::prelude::Series, index: usize) -> Option<i64> {
    let value = series.get(index).ok()?;
    let text = match anyvalue_to_json(&value) {
        serde_json::Value::Null => return None,
        serde_json::Value::String(text) => text,
        serde_json::Value::Number(number) => number.to_string(),
        other => other.to_string(),
    };
    parse_datetime_nanos(text.trim())
}
