use std::collections::HashMap;

use polars::prelude::*;
use serde_json::Value;

use crate::condition::eval;
use crate::error::AppError;
use crate::flags::normalize_flag_value;
use crate::models::{ColorRule, ColumnMeta, FlagEntry, ProjectRow};
use crate::value_utils::{
    anyvalue_to_json, anyvalue_to_search_string, build_row_cells, CellColumns,
};

pub const DEFAULT_PAGE_SIZE: usize = 250;

/// Maps requested column names (case-insensitive) to the frame's actual names.
pub fn resolve_columns(
    df: &DataFrame,
    requested: &[String],
    label: &str,
) -> Result<Vec<String>, AppError> {
    let mut lookup: HashMap<String, String> = HashMap::new();
    for name in df.get_column_names() {
        lookup.insert(name.to_lowercase(), name.to_string());
    }
    let mut resolved = Vec::with_capacity(requested.len());
    for name in requested {
        let actual = lookup.get(&name.to_lowercase()).ok_or_else(|| {
            AppError::Message(format!(
                "Column '{}' not found in project {}.",
                name, label
            ))
        })?;
        resolved.push(actual.clone());
    }
    Ok(resolved)
}

/// Collects a row's data into a JSON map using the provided column ordering.
pub fn collect_row_record(
    df: &DataFrame,
    column_names: &[String],
    row_idx: usize,
) -> HashMap<String, Value> {
    let series_map: HashMap<&str, &Series> = df
        .get_columns()
        .iter()
        .map(|series| (series.name(), series))
        .collect();
    collect_row_record_from_series(&series_map, column_names, row_idx)
}

/// Collects a row's data using a precomputed series map to avoid repeated lookups.
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

/// Ensures lowercase string caches exist for a column, returning the cached vector.
pub fn ensure_column_text_cache<'a>(
    column: &str,
    column_series_lower: &HashMap<String, &Series>,
    cache: &'a mut HashMap<String, Vec<String>>,
    row_count: usize,
) -> &'a Vec<String> {
    let key = column.to_lowercase();
    if !cache.contains_key(&key) {
        let mut col_vec: Vec<String> = vec![String::new(); row_count];
        if let Some(series) = column_series_lower.get(&key) {
            for row_idx in 0..row_count {
                if let Ok(value) = series.get(row_idx) {
                    if let Some(text) = anyvalue_to_search_string(&value) {
                        let lower = text.to_lowercase();
                        if !lower.is_empty() {
                            col_vec[row_idx] = lower;
                        }
                    }
                }
            }
        }
        cache.insert(key.clone(), col_vec);
    }
    cache.get(&key).expect("column cache must exist")
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
    let column_names: Vec<String> = columns.iter().map(|column| column.id.clone()).collect();
    let cell_columns = CellColumns::new(df, columns);
    let mut rows = Vec::new();
    for row_idx in row_indices {
        let record = collect_row_record(df, &column_names, row_idx);
        let cells = build_row_cells(df, row_idx, columns, &cell_columns);
        let flag_entry = flags.get(&row_idx);
        rows.push(ProjectRow {
            row_index: row_idx,
            data: record,
            cells,
            flag: flag_entry
                .as_ref()
                .map(|entry| normalize_flag_value(&entry.flag))
                .unwrap_or_default(),
            memo: flag_entry.and_then(|entry| entry.memo.clone()),
            tags: flag_entry.map(|entry| entry.tags.clone()).unwrap_or_default(),
            bookmarked: flag_entry.is_some_and(|entry| entry.bookmarked),
            color: None,
        });
    }
    rows
}
