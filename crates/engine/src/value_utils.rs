use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use polars::prelude::{AnyValue, DataFrame};
use serde_json::Value;

use crate::analysis::{infer_column_kinds, ColumnKind};
use crate::models::{Cell, ColumnMeta};
use crate::project_io::INTERNAL_ORIGINAL;

/// Test instrumentation: how many times each column was converted to text
/// during condition/search evaluation.
///
/// This exists so tests can assert that a condition on one column does not
/// stringify unrelated columns. Production code only records into it from the
/// query evaluation path.
static STRINGIFIED_COLUMNS: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

fn stringified_columns() -> &'static Mutex<HashMap<String, u64>> {
    STRINGIFIED_COLUMNS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Records one test-instrumentation stringification for a column.
pub fn record_stringified_column(column: &str) {
    record_stringified_column_count(column, 1);
}

/// Batch recording keeps instrumentation allocation/locking out of hot loops.
pub fn record_stringified_column_count(column: &str, count: u64) {
    if count == 0 {
        return;
    }
    let mut counts = stringified_columns()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *counts.entry(column.to_string()).or_insert(0) += count;
}

/// Test instrumentation: stringifications recorded for one column.
pub fn stringified_column_count(column: &str) -> u64 {
    stringified_columns()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(column)
        .copied()
        .unwrap_or(0)
}

/// Clears the test-instrumentation counters.
pub fn reset_stringified_columns() {
    stringified_columns()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

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

/// Number of characters kept in a list-response cell preview.
///
/// The page DTO carries this many characters plus the original length; the
/// full value is fetched on demand by `ops::cell::get_cell`. Truncation is by
/// `char`, so a multi-byte character is never split.
pub const CELL_PREVIEW_CHARS: usize = 256;

/// Column-level context for the per-cell IPC DTO.
///
/// Kinds are inferred once per column from the given frame and never per cell.
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

    /// Builds one list cell with a preview. `original` is the parsed row from
    /// `__original`.
    pub fn build(&self, column: &str, value: &AnyValue, original: Option<&Value>) -> Cell {
        preview_cell(self.build_full(column, value, original))
    }

    /// Builds one cell with the full text, for the on-demand cell fetch.
    pub fn build_full(&self, column: &str, value: &AnyValue, original: Option<&Value>) -> Cell {
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
        build_full_cell(value, kind, original, source)
    }
}

/// Maps a column kind to the cell kind used when no source record is available
/// (legacy projects). Datetimes keep their display text, so the cell kind is
/// "string".
pub fn cell_kind_name(kind: ColumnKind) -> &'static str {
    match kind {
        ColumnKind::Int => "int",
        ColumnKind::Float => "float",
        ColumnKind::String | ColumnKind::Datetime | ColumnKind::Null => "string",
    }
}

/// Builds one list cell: full text reduced to a preview plus its original
/// length.
pub fn build_cell(
    value: &AnyValue,
    column_kind: ColumnKind,
    original: Option<&Value>,
    source: &str,
) -> Cell {
    preview_cell(build_full_cell(value, column_kind, original, source))
}

/// Builds one cell with the full source text and no truncation.
///
/// A JSON source record carries the per-cell JSON type. Reading it here keeps
/// `"123"` (string) distinct from `123` (number), preserves booleans, and
/// keeps arrays/objects in one cell. CSV originals are arrays of strings, and
/// legacy projects have no `__original`, so those fall back to the Polars
/// value's own type.
pub fn build_full_cell(
    value: &AnyValue,
    column_kind: ColumnKind,
    original: Option<&Value>,
    source: &str,
) -> Cell {
    let text = anyvalue_to_search_string(value);
    match text {
        Some(text) => {
            let length = text.chars().count();
            let kind = original_json_kind(original, source)
                .unwrap_or_else(|| anyvalue_kind(value, column_kind));
            Cell {
                text: Some(text),
                kind: kind.to_string(),
                truncated: false,
                length: Some(length),
            }
        }
        None => Cell {
            text: None,
            kind: null_cell_kind(original, source).to_string(),
            truncated: false,
            length: None,
        },
    }
}

/// Returns the JSON type for a key present in a JSON source record.
fn original_json_kind(original: Option<&Value>, source: &str) -> Option<&'static str> {
    let Some(Value::Object(_)) = original else {
        return None;
    };
    match original?.pointer(source)? {
        Value::Null => Some("null"),
        Value::Bool(_) => Some("bool"),
        Value::Number(number) if number.is_i64() || number.is_u64() => Some("int"),
        Value::Number(_) => Some("float"),
        Value::String(_) => Some("string"),
        Value::Array(_) => Some("array"),
        Value::Object(_) => Some("object"),
    }
}

/// Maps a Polars value to a cell kind when no JSON source type is available.
fn anyvalue_kind(value: &AnyValue, column_kind: ColumnKind) -> &'static str {
    match value {
        AnyValue::Boolean(_) => "bool",
        AnyValue::Int8(_)
        | AnyValue::Int16(_)
        | AnyValue::Int32(_)
        | AnyValue::Int64(_)
        | AnyValue::UInt8(_)
        | AnyValue::UInt16(_)
        | AnyValue::UInt32(_)
        | AnyValue::UInt64(_) => "int",
        AnyValue::Float32(_) | AnyValue::Float64(_) => "float",
        AnyValue::String(_) | AnyValue::StringOwned(_) => cell_kind_name(ColumnKind::String),
        AnyValue::List(_) => "array",
        _ => cell_kind_name(column_kind),
    }
}

/// Reduces a full cell to the list preview. Idempotent for already-short text.
fn preview_cell(cell: Cell) -> Cell {
    let Some(text) = cell.text.as_ref() else {
        return cell;
    };
    let length = cell.length.unwrap_or_else(|| text.chars().count());
    if length <= CELL_PREVIEW_CHARS {
        return cell;
    }
    let preview: String = text.chars().take(CELL_PREVIEW_CHARS).collect();
    Cell {
        text: Some(preview),
        truncated: true,
        length: Some(length),
        ..cell
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
        other => {
            anyvalue_to_search_string(&other).and_then(|text| serde_json::from_str(&text).ok())
        }
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
