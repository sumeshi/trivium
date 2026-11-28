pub mod csv;
pub mod diagnostics;
pub mod json;
mod json_source;
pub mod jsonl;
pub mod options;
pub(crate) mod record_limit;
mod strict_json;
pub mod text_encoding;
mod utf16;

pub(crate) use json_source::{value_to_cell, visit_leaves};
pub(crate) use strict_json::from_reader as parse_json_value;

use polars::prelude::{DataFrame, NamedFrom, Series};

use crate::error::AppError;
use crate::models::ColumnMeta;
use crate::project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD};

/// Internal column id for a source column position.
pub(crate) fn column_id(position: usize) -> String {
    format!("c{:06}", position + 1)
}

/// Empty frame with the same schema used by every import batch.
pub(crate) fn empty_frame(columns: &[ColumnMeta]) -> Result<DataFrame, AppError> {
    let mut series: Vec<Series> = columns
        .iter()
        .map(|column| Series::new(column.id.as_str().into(), Vec::<String>::new()))
        .collect();
    series.push(Series::new(INTERNAL_ORIGINAL.into(), Vec::<String>::new()));
    series.push(Series::new(INTERNAL_ROW_ID.into(), Vec::<i64>::new()));
    series.push(Series::new(
        INTERNAL_SOURCE_RECORD.into(),
        Vec::<i64>::new(),
    ));
    DataFrame::new_infer_height(series.into_iter().map(Into::into).collect())
        .map_err(|_| AppError::Message("Failed to build the dataset.".into()))
}
