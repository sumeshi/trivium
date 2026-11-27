//! Native temporal values carry their unit; source strings never imply epochs.
use polars::prelude::{AnyValue, TimeUnit};

use crate::value_utils::anyvalue_to_search_string;

use super::datetime::parse_datetime_nanos;

pub(crate) fn datetime_value_nanos(value: &AnyValue<'_>) -> Option<i128> {
    match value {
        AnyValue::Null | AnyValue::Time(_) => None,
        AnyValue::Datetime(value, unit, _) => Some(
            i128::from(*value)
                * match unit {
                    TimeUnit::Nanoseconds => 1,
                    TimeUnit::Microseconds => 1_000,
                    TimeUnit::Milliseconds => 1_000_000,
                },
        ),
        AnyValue::Date(days) => Some(i128::from(*days) * 86_400 * 1_000_000_000),
        _ => anyvalue_to_search_string(value)
            .and_then(|text| parse_datetime_nanos(text.trim()))
            .map(i128::from),
    }
}
