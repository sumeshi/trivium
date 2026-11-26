use std::collections::HashMap;

use polars::prelude::*;
use serde_json::Value;

use crate::condition::eval;
use crate::error::AppError;
use crate::flags::normalize_flag_value;
use crate::models::{ColorRule, ColumnMeta, FlagEntry, ProjectRow};
use crate::value_utils::{anyvalue_to_json, build_row_cells, CellColumns};

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

pub fn materialize_rows(
    df: &DataFrame,
    columns: &[ColumnMeta],
    row_indices: impl Iterator<Item = usize>,
    flags: &HashMap<usize, FlagEntry>,
) -> Vec<ProjectRow> {
    let cell_columns = CellColumns::new(columns);
    let mut rows = Vec::new();
    for row_idx in row_indices {
        let cells = build_row_cells(df, row_idx, columns, &cell_columns);
        let flag_entry = flags.get(&row_idx);
        rows.push(ProjectRow {
            row_index: row_idx,
            cells,
            flag: flag_entry
                .as_ref()
                .map(|entry| normalize_flag_value(&entry.flag))
                .unwrap_or_default(),
            memo: flag_entry.and_then(|entry| entry.memo.clone()),
            tags: flag_entry
                .map(|entry| entry.tags.clone())
                .unwrap_or_default(),
            bookmarked: flag_entry.is_some_and(|entry| entry.bookmarked),
            color: None,
        });
    }
    rows
}
