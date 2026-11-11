use std::collections::HashMap;

use chrono::NaiveDateTime;
use polars::prelude::{AnyValue, DataFrame, DataType, Series};
use serde::{Deserialize, Serialize};

use crate::project_io::is_internal_column;
use crate::value_utils::anyvalue_to_search_string;

/// Column type used by the condition editor and, later, column stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnKind {
    Null,
    Int,
    Float,
    Datetime,
    String,
}

impl ColumnKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Int => "int",
            Self::Float => "float",
            Self::Datetime => "datetime",
            Self::String => "string",
        }
    }
}

/// One pass over the frame. Integer text is parsed as `i64` before `f64`.
pub fn infer_column_kinds(df: &DataFrame) -> HashMap<String, ColumnKind> {
    df.get_columns()
        .iter()
        .filter(|series| !is_internal_column(series.name()))
        .map(|series| (series.name().to_string(), series_kind(series)))
        .collect()
}

pub(crate) fn dtype_kind(dtype: &DataType) -> Option<ColumnKind> {
    match dtype {
        DataType::Null => Some(ColumnKind::Null),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some(ColumnKind::Int),
        DataType::Float32 | DataType::Float64 => Some(ColumnKind::Float),
        DataType::Date | DataType::Datetime(_, _) | DataType::Time => Some(ColumnKind::Datetime),
        _ => None,
    }
}

#[derive(Default)]
pub(crate) struct KindFlags {
    saw_int: bool,
    saw_float: bool,
    saw_datetime: bool,
    saw_string: bool,
}

impl KindFlags {
    pub(crate) fn observe(&mut self, value: &AnyValue) {
        match classify_any(value) {
            None | Some(ColumnKind::Null) => {}
            Some(ColumnKind::Int) => self.saw_int = true,
            Some(ColumnKind::Float) => self.saw_float = true,
            Some(ColumnKind::Datetime) => self.saw_datetime = true,
            Some(ColumnKind::String) => self.saw_string = true,
        }
    }

    pub(crate) fn finish(self) -> ColumnKind {
        if self.saw_string {
            return ColumnKind::String;
        }
        match (self.saw_int, self.saw_float, self.saw_datetime) {
            (false, false, false) => ColumnKind::Null,
            (true, false, false) => ColumnKind::Int,
            (_, true, false) => ColumnKind::Float,
            (false, false, true) => ColumnKind::Datetime,
            _ => ColumnKind::String,
        }
    }
}

pub(crate) fn series_kind(series: &Series) -> ColumnKind {
    if let Some(kind) = dtype_kind(series.dtype()) {
        return kind;
    }
    let mut flags = KindFlags::default();
    for idx in 0..series.len() {
        let Ok(value) = series.get(idx) else {
            continue;
        };
        flags.observe(&value);
    }
    flags.finish()
}

fn classify_any(value: &AnyValue) -> Option<ColumnKind> {
    match value {
        AnyValue::Null => None,
        AnyValue::Int8(_)
        | AnyValue::Int16(_)
        | AnyValue::Int32(_)
        | AnyValue::Int64(_)
        | AnyValue::UInt8(_)
        | AnyValue::UInt16(_)
        | AnyValue::UInt32(_)
        | AnyValue::UInt64(_) => Some(ColumnKind::Int),
        AnyValue::Float32(value) => Some(numeric_kind(f64::from(*value))),
        AnyValue::Float64(value) => Some(numeric_kind(*value)),
        AnyValue::Date(_) | AnyValue::Datetime(_, _, _) | AnyValue::Time(_) => {
            Some(ColumnKind::Datetime)
        }
        _ => anyvalue_to_search_string(value).and_then(|text| classify_text(&text)),
    }
}

fn numeric_kind(value: f64) -> ColumnKind {
    if !value.is_finite() {
        return ColumnKind::String;
    }
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        ColumnKind::Int
    } else {
        ColumnKind::Float
    }
}

fn classify_text(text: &str) -> Option<ColumnKind> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(value) = text.parse::<i128>() {
        return Some(if text == value.to_string() {
            ColumnKind::Int
        } else {
            ColumnKind::String
        });
    }
    // Integer-looking text beyond i128 is not classified as a number: the
    // numeric mode cannot compare it exactly, so it stays a string instead of
    // being rounded through f64.
    if looks_integer(text) {
        return Some(ColumnKind::String);
    }
    if text.chars().any(|ch| ch.is_ascii_digit()) {
        if let Ok(value) = text.parse::<f64>() {
            if value.is_finite() {
                return Some(ColumnKind::Float);
            }
        }
    }
    if is_datetime(text) {
        return Some(ColumnKind::Datetime);
    }
    Some(ColumnKind::String)
}

fn looks_integer(text: &str) -> bool {
    let digits = text
        .strip_prefix('+')
        .or_else(|| text.strip_prefix('-'))
        .unwrap_or(text);
    !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit())
}

pub(crate) fn parse_datetime_nanos(text: &str) -> Option<i64> {
    const FORMATS: [&str; 5] = [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ];
    for format in FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(text, format) {
            return dt.and_utc().timestamp_nanos_opt();
        }
        if let Ok(date) = chrono::NaiveDate::parse_from_str(text, format) {
            let dt = date.and_hms_opt(0, 0, 0)?;
            return dt.and_utc().timestamp_nanos_opt();
        }
    }
    None
}

fn is_datetime(text: &str) -> bool {
    parse_datetime_nanos(text).is_some()
}
