use std::collections::HashMap;

use polars::prelude::*;
use serde_json::Value;

use crate::flags::normalize_flag_value;
use crate::models::{FlagEntry, ProjectRow};
use crate::value_utils::{anyvalue_to_json, anyvalue_to_search_string};

pub const DEFAULT_PAGE_SIZE: usize = 250;

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

/// Builds concatenated row text and per-column single-row caches.
pub fn build_row_search_text(
    column_names: &[String],
    column_series: &HashMap<&str, &Series>,
    row_idx: usize,
) -> (String, HashMap<String, Vec<String>>) {
    let mut row_text = String::new();
    let mut per_column: HashMap<String, Vec<String>> = HashMap::new();
    for column in column_names {
        if let Some(series) = column_series.get(column.as_str()) {
            if let Ok(value) = series.get(row_idx) {
                if let Some(text) = anyvalue_to_search_string(&value) {
                    let lower = text.to_lowercase();
                    if !lower.is_empty() {
                        if !row_text.is_empty() {
                            row_text.push(' ');
                        }
                        row_text.push_str(&lower);
                        per_column.insert(column.to_lowercase(), vec![lower]);
                    }
                }
            }
        }
    }
    (row_text, per_column)
}

pub fn materialize_rows(
    df: &DataFrame,
    columns: &[String],
    row_indices: impl Iterator<Item = usize>,
    flags: &HashMap<usize, FlagEntry>,
) -> Vec<ProjectRow> {
    let mut rows = Vec::new();
    for row_idx in row_indices {
        let record = collect_row_record(df, columns, row_idx);
        let flag_entry = flags.get(&row_idx);
        rows.push(ProjectRow {
            row_index: row_idx,
            data: record,
            flag: flag_entry
                .as_ref()
                .map(|entry| normalize_flag_value(&entry.flag))
                .unwrap_or_default(),
            memo: flag_entry.and_then(|entry| entry.memo.clone()),
        });
    }
    rows
}
