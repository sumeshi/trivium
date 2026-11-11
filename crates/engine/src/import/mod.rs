pub mod csv;
pub mod json;
pub mod jsonl;
mod json_source;
mod utf16;

use polars::prelude::{DataFrame, NamedFrom, Series};

use crate::error::AppError;
use crate::models::ColumnMeta;
use crate::project_io::{
    INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD,
};

/// Internal column id for a source column position.
pub(crate) fn column_id(position: usize) -> String {
    format!("c{:06}", position + 1)
}

/// Empty frame with the same schema used by every import batch.
pub(crate) fn empty_frame(columns: &[ColumnMeta]) -> Result<DataFrame, AppError> {
    let mut series: Vec<Series> = columns
        .iter()
        .map(|column| Series::new(column.id.as_str(), Vec::<String>::new()))
        .collect();
    series.push(Series::new(INTERNAL_ORIGINAL, Vec::<String>::new()));
    series.push(Series::new(INTERNAL_ROW_ID, Vec::<i64>::new()));
    series.push(Series::new(
        INTERNAL_SOURCE_RECORD,
        Vec::<i64>::new(),
    ));
    DataFrame::new(series).map_err(|_| AppError::Message("Failed to build the dataset.".into()))
}
