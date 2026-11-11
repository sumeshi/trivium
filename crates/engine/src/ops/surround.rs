use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::analysis::infer::{parse_datetime_nanos, series_kind, ColumnKind};
use crate::error::AppError;
use crate::ioc::{cached_ioc_outcomes, load_ioc_entries};
use crate::models::ProjectRow;
use crate::project_io::{is_internal_column, read_project_dataframe, ProjectColumns};
use crate::storage::load_flags;
use crate::store::ProjectsStore;
use crate::value_utils::anyvalue_to_json;

use super::query::{final_flags, materialize_page};

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
    if is_internal_column(&payload.column) {
        return Err(AppError::Message("Column not found.".into()));
    }
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    if payload.row_index >= df.height() {
        return Err(AppError::Message("Row not found.".into()));
    }
    let project_columns = ProjectColumns::from_meta(&meta, &df);
    let series = df
        .column(&payload.column)
        .map_err(|_| AppError::Message("Column not found.".into()))?;
    if series_kind(series) != ColumnKind::Datetime {
        return Err(AppError::Message("Column is not datetime.".into()));
    }
    let mut timed = Vec::new();
    for index in 0..series.len() {
        if let Some(nanos) = nanos_at(series, index) {
            timed.push(TimedRow { index, nanos });
        }
    }
    timed.sort_by(|left, right| left.nanos.cmp(&right.nanos).then(left.index.cmp(&right.index)));
    let anchor_at = timed
        .iter()
        .position(|row| row.index == payload.row_index)
        .ok_or_else(|| AppError::Message("Anchor row has no datetime.".into()))?;
    let anchor_nanos = timed[anchor_at].nanos;
    let selected: Vec<usize> = match payload.mode {
        SurroundMode::Neighbors => {
            let count = payload.count as usize;
            let start = anchor_at.saturating_sub(count);
            let end = (anchor_at + count).min(timed.len() - 1);
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
    let flags = load_flags(&project_dir.join("flags.json")).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);
    let final_flag_vec = final_flags(df.height(), &flags, &ioc_outcomes);
    let rows = materialize_page(
        &df,
        &selected,
        project_columns.columns(),
        &flags,
        &final_flag_vec,
        &ioc_outcomes,
        !iocs.is_empty(),
    )?;
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
