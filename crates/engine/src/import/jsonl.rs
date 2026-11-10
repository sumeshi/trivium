use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use polars::prelude::*;
use serde_json::Value;

use crate::error::AppError;
use crate::models::FlagEntry;

/// Reads a JSONL file (one JSON object per line) into a DataFrame.
/// Nested object keys are flattened into dotted column names (`a.b`),
/// and columns are the union of all keys across lines, in first-appearance order.
pub fn load(path: &Path) -> Result<(DataFrame, HashMap<usize, FlagEntry>), AppError> {
    let file = File::open(path)
        .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let mut records: Vec<Vec<(String, Value)>> = Vec::new();
    let mut column_order: Vec<String> = Vec::new();
    let mut seen_columns: HashMap<String, usize> = HashMap::new();

    for (line_no, line) in BufReader::new(file).lines().enumerate() {
        let line = line
            .map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
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
        let mut flattened: Vec<(String, Value)> = Vec::new();
        flatten_object(&value, "", &mut flattened);
        for (key, _) in &flattened {
            if seen_columns.insert(key.clone(), 0).is_none() {
                column_order.push(key.clone());
            }
        }
        records.push(flattened);
    }

    let mut columns: Vec<Series> = Vec::with_capacity(column_order.len());
    for column in &column_order {
        let values: Vec<Option<&Value>> = records
            .iter()
            .map(|row| {
                row.iter()
                    .find(|(key, _)| key == column)
                    .map(|(_, value)| value)
            })
            .collect();
        columns.push(build_series(column, &values)?);
    }

    let mut df = DataFrame::new(columns)
        .map_err(|_| AppError::Message("Failed to build the dataset.".into()))?;
    let row_ids: Vec<i64> = (0..df.height()).map(|idx| idx as i64).collect();
    let row_id_series = Series::new("__rowid", row_ids);
    df.with_column(row_id_series)
        .map_err(|_| AppError::Message("Failed to add row ids to the dataset.".into()))?;
    Ok((df, HashMap::new()))
}

/// Flattens nested objects into dotted key paths. Arrays and scalars stay as-is.
fn flatten_object(value: &Value, prefix: &str, out: &mut Vec<(String, Value)>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_object(child, &path, out);
            }
        }
        other => out.push((prefix.to_string(), other.clone())),
    }
}

/// Builds a Series for one column, inferring Int64 / Float64 / Boolean / Utf8.
fn build_series(
    name: &str,
    values: &[Option<&Value>],
) -> Result<Series, AppError> {
    let non_null: Vec<&Value> = values.iter().filter_map(|v| *v).collect();
    let is_empty = non_null.is_empty();
    if is_empty || non_null.iter().all(|v| v.is_i64()) {
        let typed: Vec<Option<i64>> = values
            .iter()
            .map(|v| v.and_then(|value| value.as_i64()))
            .collect();
        return Ok(Series::new(name, typed));
    }
    if non_null.iter().all(|v| v.is_number()) {
        let typed: Vec<Option<f64>> = values
            .iter()
            .map(|v| v.and_then(|value| value.as_f64()))
            .collect();
        return Ok(Series::new(name, typed));
    }
    if non_null.iter().all(|v| v.is_boolean()) {
        let typed: Vec<Option<bool>> = values
            .iter()
            .map(|v| v.and_then(|value| value.as_bool()))
            .collect();
        return Ok(Series::new(name, typed));
    }
    let typed: Vec<Option<String>> = values
        .iter()
        .map(|v| v.map(value_to_string))
        .collect();
    Ok(Series::new(name, typed))
}

/// Stringifies a non-null JSON value for a Utf8 column.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}
