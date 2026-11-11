use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use polars::prelude::*;
use serde_json::Value;

use crate::error::AppError;
use crate::models::{ColumnMeta, FlagEntry, ImportedData};
use crate::project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID};

/// Internal column id assigned to a source path by its first appearance.
fn column_id(position: usize) -> String {
    format!("c{:06}", position + 1)
}

/// Escapes one JSON Pointer token (`~` -> `~0`, `/` -> `~1`).
fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

/// Display label derived from an absolute JSON Pointer. The leading slash is
/// dropped, but escapes are kept so `/a.b` and `/a/b` stay distinguishable.
fn pointer_label(pointer: &str) -> String {
    pointer.strip_prefix('/').unwrap_or(pointer).to_string()
}

/// Reads a JSONL file (one JSON object per line) into a frame with internal
/// column ids.
///
/// Nested object keys become JSON Pointer paths (`/a/b`), and `/a.b` is a
/// different column from `/a/b`. Arrays stay in one cell as a JSON string.
/// Columns are the union of all paths in first-appearance order. The original
/// object is kept per row in `__original`.
pub fn load(path: &Path) -> Result<ImportedData, AppError> {
    let file = File::open(path)
        .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let mut records: Vec<(usize, Value)> = Vec::new();

    for (line_no, line) in BufReader::new(file).lines().enumerate() {
        let line =
            line.map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed).map_err(|e| {
            AppError::Message(format!("Failed to parse JSONL line {}: {}", line_no + 1, e))
        })?;
        if !value.is_object() {
            return Err(AppError::Message(format!(
                "JSONL line {} is not a JSON object.",
                line_no + 1
            )));
        }
        // Keep the physical line number (empty lines included) for errors.
        records.push((line_no + 1, value));
    }

    build_import(&records, "JSONL")
}

/// Shared record-to-frame conversion for JSON objects (JSON arrays and JSONL).
///
/// `records` pairs each record with its source number (array element or
/// physical line) so failures can point at the input. Column union, JSON
/// Pointer paths, exact value text, and `__original` follow the JSONL rules.
pub(crate) fn build_import(
    records: &[(usize, Value)],
    source_format: &str,
) -> Result<ImportedData, AppError> {
    let mut flattened_rows: Vec<Vec<(String, Value)>> = Vec::new();
    let mut column_order: Vec<String> = Vec::new();
    let mut seen_columns: HashMap<String, usize> = HashMap::new();

    for (record_no, value) in records {
        if !value.is_object() {
            return Err(AppError::Message(format!(
                "{} record {} is not a JSON object.",
                source_format, record_no
            )));
        }
        let mut flattened: Vec<(String, Value)> = Vec::new();
        flatten_object(value, "", &mut flattened);
        for (pointer, _) in &flattened {
            if seen_columns.insert(pointer.clone(), 0).is_none() {
                column_order.push(pointer.clone());
            }
        }
        flattened_rows.push(flattened);
    }

    let columns: Vec<ColumnMeta> = column_order
        .iter()
        .enumerate()
        .map(|(order, pointer)| ColumnMeta {
            id: column_id(order),
            label: pointer_label(pointer),
            source: pointer.clone(),
            order,
        })
        .collect();

    let row_count = records.len();
    let mut series: Vec<Series> = Vec::with_capacity(columns.len());
    for (column, pointer) in columns.iter().zip(column_order.iter()) {
        let values: Vec<Option<&Value>> = flattened_rows
            .iter()
            .map(|row| {
                row.iter()
                    .find(|(key, _)| key == pointer)
                    .map(|(_, value)| value)
            })
            .collect();
        series.push(build_series(&column.id, &values));
    }
    // A file made only of empty objects has no data columns. The internal row
    // id keeps its record count instead of collapsing to zero rows.
    if series.is_empty() {
        let row_ids: Vec<i64> = (0..row_count).map(|idx| idx as i64).collect();
        series.push(Series::new(INTERNAL_ROW_ID, row_ids));
    }

    let mut df = DataFrame::new(series).map_err(|_| {
        AppError::Message(format!("Failed to build the {} dataset.", source_format))
    })?;
    if df.height() != row_count {
        return Err(AppError::Message(format!(
            "The {} record count changed while building the dataset.",
            source_format
        )));
    }

    let originals: Vec<String> = records.iter().map(|(_, value)| value.to_string()).collect();
    df.with_column(Series::new(INTERNAL_ORIGINAL, originals))
        .map_err(|_| AppError::Message("Failed to store the original JSON records.".into()))?;

    let row_ids: Vec<i64> = (0..row_count).map(|idx| idx as i64).collect();
    let row_id_series = Series::new(INTERNAL_ROW_ID, row_ids);
    df.with_column(row_id_series)
        .map_err(|_| AppError::Message("Failed to add row ids to the dataset.".into()))?;

    Ok(ImportedData {
        frame: df,
        columns,
        flags: HashMap::<usize, FlagEntry>::new(),
    })
}

/// Flattens nested objects into absolute JSON Pointer paths. Arrays and
/// scalars stay as-is, including explicit nulls.
fn flatten_object(value: &Value, prefix: &str, out: &mut Vec<(String, Value)>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let path = format!("{}/{}", prefix, escape_pointer_token(key));
                flatten_object(child, &path, out);
            }
        }
        other => out.push((prefix.to_string(), other.clone())),
    }
}

/// Builds a Utf8 Series for one column.
///
/// Numeric cells keep their source number text (`serde_json` arbitrary
/// precision), booleans become `true`/`false`, arrays and objects stay JSON
/// text in one cell, and null as well as an absent key stays a Polars null.
fn build_series(name: &str, values: &[Option<&Value>]) -> Series {
    let typed: Vec<Option<String>> = values
        .iter()
        .map(|value| value.and_then(|value| value_to_cell(value)))
        .collect();
    Series::new(name, typed)
}

/// Converts a present JSON value into its display cell text. Null is kept as
/// a Polars null, not an empty string.
fn value_to_cell(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}
