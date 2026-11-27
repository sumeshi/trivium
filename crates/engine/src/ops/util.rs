use std::collections::HashMap;

use polars::prelude::Series;
use serde_json::Value;

use crate::condition::eval;
use crate::error::AppError;
use crate::models::ColorRule;
use crate::value_utils::anyvalue_to_json;

pub const DEFAULT_PAGE_SIZE: usize = 250;

/// Collects a row's indexed values using a precomputed series map.
///
/// Color rules are evaluated against this map; `ProjectRow` itself only keeps
/// the cell DTOs.
pub fn collect_row_record_from_series(
    series_map: &HashMap<&str, &Series>,
    column_names: &[String],
    row_idx: usize,
) -> HashMap<String, Value> {
    let mut record = HashMap::new();
    for column in column_names {
        if let Some(series) = series_map.get(column.as_str()) {
            if let Ok(value) = series.get(row_idx) {
                record.insert(column.clone(), anyvalue_to_json(&value));
            }
        }
    }
    record
}

/// Returns the color of the first matching rule, or `None` when nothing matches.
/// Row data is passed directly to the shared condition evaluator, so color
/// matching uses exactly the same predicate as search and filters.
pub fn matched_rule_color(
    row: &HashMap<String, Value>,
    rules: &[ColorRule],
) -> Result<Option<String>, AppError> {
    for rule in rules {
        if eval(&rule.condition, row)? {
            return Ok(Some(rule.color.clone()));
        }
    }
    Ok(None)
}
