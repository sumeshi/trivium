use std::collections::HashMap;

use polars::prelude::{AnyValue, DataFrame};
use serde_json::Value;

use crate::analysis::{infer_column_kinds, ColumnKind};
use crate::models::{Cell, ColumnMeta};
use crate::project_io::INTERNAL_ORIGINAL;

pub fn anyvalue_to_json(value: &AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(v) => Value::Bool(*v),
        AnyValue::Int8(v) => Value::from(*v),
        AnyValue::Int16(v) => Value::from(*v),
        AnyValue::Int32(v) => Value::from(*v),
        AnyValue::Int64(v) => Value::from(*v),
        AnyValue::UInt8(v) => Value::from(*v),
        AnyValue::UInt16(v) => Value::from(*v),
        AnyValue::UInt32(v) => Value::from(*v),
        AnyValue::UInt64(v) => Value::from(*v),
        AnyValue::Float32(v) => Value::from(f64::from(*v)),
        AnyValue::Float64(v) => Value::from(*v),
        AnyValue::String(v) => Value::String(v.to_string()),
        AnyValue::Date(v) => Value::String(v.to_string()),
        AnyValue::Datetime(v, _, _) => Value::String(v.to_string()),
        AnyValue::Time(v) => Value::String(v.to_string()),
        AnyValue::List(series) => {
            let values: Vec<Value> = series.iter().map(|v| anyvalue_to_json(&v)).collect();
            Value::Array(values)
        }
        other => Value::String(other.to_string()),
    }
}

pub fn value_display_length(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::String(text) => text.chars().count(),
        Value::Number(number) => number.to_string().chars().count(),
        Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value)
            .map(|text| text.chars().count())
            .unwrap_or(0),
    }
}

/// Column-level context for the per-cell IPC DTO.
///
/// Kinds are inferred once per column from the full frame and never per cell.
/// `sources` are the original CSV headers / JSON Pointers; they are only used
/// to tell an absent JSON key ("missing") from an explicit null.
pub struct CellColumns {
    kinds: HashMap<String, ColumnKind>,
    sources: HashMap<String, String>,
}

impl CellColumns {
    pub fn new(df: &DataFrame, columns: &[ColumnMeta]) -> Self {
        let kinds = infer_column_kinds(df);
        let sources = columns
            .iter()
            .map(|column| (column.id.clone(), column.source.clone()))
            .collect();
        Self { kinds, sources }
    }

    /// Builds one cell. `original` is the parsed row from `__original`.
    pub fn build(&self, column: &str, value: &AnyValue, original: Option<&Value>) -> Cell {
        let kind = self
            .kinds
            .get(column)
            .copied()
            .unwrap_or(ColumnKind::String);
        let source = self
            .sources
            .get(column)
            .map(String::as_str)
            .unwrap_or(column);
        build_cell(value, kind, original, source)
    }
}

/// Maps a column kind to the cell kind the frontend receives. Datetimes keep
/// their display text, so the cell kind is "string"; booleans are stored as
/// `true`/`false` text and therefore also arrive as "string".
pub fn cell_kind_name(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Int => "int",
        ColumnKind::Float => "float",
        ColumnKind::String | ColumnKind::Datetime | ColumnKind::Null => "string",
    }
}

/// Builds one cell DTO from the exact text and the column kind.
pub fn build_cell(
    value: &AnyValue,
    column_kind: ColumnKind,
    original: Option<&Value>,
    source: &str,
) -> Cell {
    let text = anyvalue_to_search_string(value);
    match text {
        Some(text) => Cell {
            text: Some(text),
            kind: cell_kind_name(column_kind).to_string(),
        },
        None => Cell {
            text: None,
            kind: null_cell_kind(original, source).to_string(),
        },
    }
}

/// A JSON object whose pointer is absent means "missing"; a present key with a
/// JSON null means "null". CSV originals are arrays, and legacy projects have
/// no `__original`, so those cells are null only.
fn null_cell_kind(original: Option<&Value>, source: &str) -> &'static str {
    match original {
        Some(Value::Object(_)) if original.and_then(|value| value.pointer(source)).is_none() => {
            "missing"
        }
        _ => "null",
    }
}

/// Parses the `__original` value for one row. Returns `None` for a Polars null
/// and for frames without the internal column (legacy projects).
pub fn original_record_at(df: &DataFrame, row_idx: usize) -> Option<Value> {
    let series = df.column(INTERNAL_ORIGINAL).ok()?;
    match series.get(row_idx).ok()? {
        AnyValue::Null => None,
        other => anyvalue_to_search_string(&other)
            .and_then(|text| serde_json::from_str(&text).ok()),
    }
}

/// Builds the exact cell map for one row. `cells` must have been built from the
/// same frame/columns; `df` may be a page-sized slice taken from it.
pub fn build_row_cells(
    df: &DataFrame,
    row_idx: usize,
    columns: &[ColumnMeta],
    cells: &CellColumns,
) -> HashMap<String, Cell> {
    let original = original_record_at(df, row_idx);
    let mut result = HashMap::with_capacity(columns.len());
    for column in columns {
        let value = df
            .column(&column.id)
            .ok()
            .and_then(|series| series.get(row_idx).ok());
        let cell = match value {
            Some(value) => cells.build(&column.id, &value, original.as_ref()),
            None => cells.build(&column.id, &AnyValue::Null, original.as_ref()),
        };
        result.insert(column.id.clone(), cell);
    }
    result
}

pub fn anyvalue_to_search_string(value: &AnyValue) -> Option<String> {
    match value {
        AnyValue::Null => None,
        AnyValue::Boolean(v) => Some(v.to_string()),
        AnyValue::Int8(v) => Some(v.to_string()),
        AnyValue::Int16(v) => Some(v.to_string()),
        AnyValue::Int32(v) => Some(v.to_string()),
        AnyValue::Int64(v) => Some(v.to_string()),
        AnyValue::UInt8(v) => Some(v.to_string()),
        AnyValue::UInt16(v) => Some(v.to_string()),
        AnyValue::UInt32(v) => Some(v.to_string()),
        AnyValue::UInt64(v) => Some(v.to_string()),
        AnyValue::Float32(v) => Some(f64::from(*v).to_string()),
        AnyValue::Float64(v) => Some(v.to_string()),
        AnyValue::String(v) => Some(v.to_string()),
        AnyValue::StringOwned(v) => Some(v.to_string()),
        AnyValue::Datetime(_, _, _) => Some(value.to_string()),
        AnyValue::Date(_) => Some(value.to_string()),
        AnyValue::Time(_) => Some(value.to_string()),
        AnyValue::List(series) => {
            let mut parts: Vec<String> = Vec::new();
            for inner in series.iter() {
                if let Some(text) = anyvalue_to_search_string(&inner) {
                    parts.push(text);
                }
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(","))
            }
        }
        other => Some(other.to_string()),
    }
}
