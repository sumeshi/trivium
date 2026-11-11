use crate::analysis::infer::parse_datetime_nanos;
use crate::value_utils::anyvalue_to_search_string;
use polars::prelude::{AnyValue, Series, TimeUnit};

pub(super) fn int_point(series: &Series, idx: usize) -> Option<i128> {
    let value = series.get(idx).ok()?;
    match value {
        AnyValue::Null => None,
        AnyValue::Int8(value) => Some(i128::from(value)),
        AnyValue::Int16(value) => Some(i128::from(value)),
        AnyValue::Int32(value) => Some(i128::from(value)),
        AnyValue::Int64(value) => Some(i128::from(value)),
        AnyValue::UInt8(value) => Some(i128::from(value)),
        AnyValue::UInt16(value) => Some(i128::from(value)),
        AnyValue::UInt32(value) => Some(i128::from(value)),
        AnyValue::UInt64(value) => Some(i128::from(value)),
        _ => canonical_i128(&value),
    }
}

fn canonical_i128(value: &AnyValue) -> Option<i128> {
    let text = anyvalue_to_search_string(value)?;
    let text = text.trim();
    let parsed = text.parse::<i128>().ok()?;
    (text == parsed.to_string()).then_some(parsed)
}

pub(super) fn float_point(series: &Series, idx: usize) -> Option<f64> {
    let value = series.get(idx).ok()?;
    match value {
        AnyValue::Null => None,
        AnyValue::Float32(value) => finite(f64::from(value)),
        AnyValue::Float64(value) => finite(value),
        AnyValue::Int8(value) => Some(f64::from(value)),
        AnyValue::Int16(value) => Some(f64::from(value)),
        AnyValue::Int32(value) => Some(f64::from(value)),
        AnyValue::Int64(value) => Some(value as f64),
        AnyValue::UInt8(value) => Some(f64::from(value)),
        AnyValue::UInt16(value) => Some(f64::from(value)),
        AnyValue::UInt32(value) => Some(f64::from(value)),
        AnyValue::UInt64(value) => Some(value as f64),
        _ => anyvalue_to_search_string(&value).and_then(|text| finite(text.trim().parse().ok()?)),
    }
}

pub(super) fn finite(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

pub(super) fn datetime_point(series: &Series, idx: usize) -> Option<i128> {
    let value = series.get(idx).ok()?;
    match value {
        AnyValue::Null => None,
        AnyValue::Datetime(value, unit, _) => Some(scale_nanos(value, unit)),
        AnyValue::Date(days) => Some(i128::from(days) * 86_400 * 1_000_000_000),
        _ => anyvalue_to_search_string(&value)
            .and_then(|text| parse_datetime_nanos(text.trim()))
            .map(i128::from),
    }
}

fn scale_nanos(value: i64, unit: TimeUnit) -> i128 {
    let value = i128::from(value);
    match unit {
        TimeUnit::Nanoseconds => value,
        TimeUnit::Microseconds => value * 1_000,
        TimeUnit::Milliseconds => value * 1_000_000,
    }
}

pub(super) fn format_nanos(value: i128) -> String {
    let Ok(nanos) = i64::try_from(value) else {
        return value.to_string();
    };
    let seconds = nanos.div_euclid(1_000_000_000);
    let subsecond = nanos.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(seconds, subsecond)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
        .unwrap_or_else(|| value.to_string())
}
