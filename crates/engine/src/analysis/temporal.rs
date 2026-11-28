//! Native temporal values carry their unit; source strings never imply epochs.
use polars::prelude::{AnyValue, TimeUnit};

use crate::value_utils::anyvalue_to_search_string;

use super::datetime::parse_datetime_nanos;

pub(crate) fn datetime_value_nanos(value: &AnyValue<'_>) -> Option<i128> {
    match value {
        AnyValue::Null | AnyValue::Time(_) => None,
        AnyValue::Datetime(value, unit, _) | AnyValue::DatetimeOwned(value, unit, _) => Some(
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

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::TimeZone;
    use std::sync::Arc;

    #[test]
    fn owned_and_borrowed_datetimes_scale_units_without_overflow() {
        for (unit, scale) in [
            (TimeUnit::Nanoseconds, 1),
            (TimeUnit::Microseconds, 1_000),
            (TimeUnit::Milliseconds, 1_000_000),
        ] {
            for timestamp in [i64::MIN, -1, 0, 1, i64::MAX] {
                let expected = Some(i128::from(timestamp) * scale);
                assert_eq!(
                    datetime_value_nanos(&AnyValue::Datetime(
                        timestamp,
                        unit,
                        Some(&TimeZone::UTC),
                    )),
                    expected,
                );
                assert_eq!(
                    datetime_value_nanos(&AnyValue::DatetimeOwned(
                        timestamp,
                        unit,
                        Some(Arc::new(TimeZone::UTC)),
                    )),
                    expected,
                );
            }
        }
    }
}
