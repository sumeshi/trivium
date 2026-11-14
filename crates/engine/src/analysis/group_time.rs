use chrono::{Datelike, Duration, SecondsFormat, TimeZone, Timelike, Utc};
use serde::{Deserialize, Serialize};

use super::infer::parse_datetime_nanos;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ts_rs::TS)]
#[serde(rename_all = "lowercase")]
pub enum GroupGranularity {
    #[default]
    Exact,
    Year,
    Month,
    Day,
    Hour,
    Minute,
}

/// UTC, half-open boundaries understood by the existing datetime conditions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ts_rs::TS)]
pub struct GroupTimeRange {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupBucket {
    pub label: String,
    pub range: GroupTimeRange,
}

const INVALID: &str = "Datetime grouping requires a recognized datetime.";
const RANGE: &str = "Datetime grouping range is outside the supported nanosecond timestamp range.";

/// Group by calendar boundaries in UTC without rounding or rewriting evidence.
pub fn bucket(
    text: &str,
    granularity: GroupGranularity,
) -> Result<Option<GroupBucket>, &'static str> {
    if granularity == GroupGranularity::Exact {
        return Ok(None);
    }
    let nanos = parse_datetime_nanos(text.trim()).ok_or(INVALID)?;
    let value = chrono::DateTime::<Utc>::from_timestamp_nanos(nanos);
    let (year, month, day, hour, minute) = (
        value.year(),
        value.month(),
        value.day(),
        value.hour(),
        value.minute(),
    );
    let utc = |year, month, day, hour, minute| {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .ok_or(RANGE)
    };
    let start = match granularity {
        GroupGranularity::Year => utc(year, 1, 1, 0, 0)?,
        GroupGranularity::Month => utc(year, month, 1, 0, 0)?,
        GroupGranularity::Day => utc(year, month, day, 0, 0)?,
        GroupGranularity::Hour => utc(year, month, day, hour, 0)?,
        GroupGranularity::Minute => utc(year, month, day, hour, minute)?,
        GroupGranularity::Exact => unreachable!(),
    };
    let end = match granularity {
        GroupGranularity::Year => utc(year + 1, 1, 1, 0, 0)?,
        GroupGranularity::Month if month == 12 => utc(year + 1, 1, 1, 0, 0)?,
        GroupGranularity::Month => utc(year, month + 1, 1, 0, 0)?,
        GroupGranularity::Day => start.checked_add_signed(Duration::days(1)).ok_or(RANGE)?,
        GroupGranularity::Hour => start.checked_add_signed(Duration::hours(1)).ok_or(RANGE)?,
        GroupGranularity::Minute => start
            .checked_add_signed(Duration::minutes(1))
            .ok_or(RANGE)?,
        GroupGranularity::Exact => unreachable!(),
    };
    // A range outside i64 nanoseconds cannot be applied by condition::eval.
    // Reject it instead of returning a group whose drill-down cannot match.
    if start.timestamp_nanos_opt().is_none() || end.timestamp_nanos_opt().is_none() {
        return Err(RANGE);
    }
    let label = match granularity {
        GroupGranularity::Year => start.format("%Y").to_string(),
        GroupGranularity::Month => start.format("%Y-%m").to_string(),
        GroupGranularity::Day => start.format("%Y-%m-%d").to_string(),
        GroupGranularity::Hour => start.format("%Y-%m-%dT%H:00Z").to_string(),
        GroupGranularity::Minute => start.format("%Y-%m-%dT%H:%MZ").to_string(),
        GroupGranularity::Exact => unreachable!(),
    };
    Ok(Some(GroupBucket {
        label,
        range: GroupTimeRange {
            start: start.to_rfc3339_opts(SecondsFormat::Secs, true),
            end: end.to_rfc3339_opts(SecondsFormat::Secs, true),
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grouped(text: &str, granularity: GroupGranularity) -> GroupBucket {
        bucket(text, granularity).unwrap().unwrap()
    }

    #[test]
    fn exact_keeps_arbitrary_values_and_offsets_share_utc_buckets() {
        assert_eq!(bucket("not a date", GroupGranularity::Exact), Ok(None));
        for granularity in [
            GroupGranularity::Day,
            GroupGranularity::Hour,
            GroupGranularity::Minute,
        ] {
            assert_eq!(
                grouped("2026-11-14T12:15:23.123456789+09:00", granularity),
                grouped("2026-11-14T03:15:23.123456789Z", granularity),
            );
        }
        let value = grouped("2026-01-01T00:30:00+09:00", GroupGranularity::Day);
        assert_eq!(value.label, "2025-12-31");
        assert_eq!(value.range.start, "2025-12-31T00:00:00Z");
        assert_eq!(value.range.end, "2026-01-01T00:00:00Z");
    }

    #[test]
    fn calendar_boundaries_cover_leap_months_and_year_rollovers() {
        let leap = grouped("2024-02-29 23:59:59.999999999", GroupGranularity::Month);
        assert_eq!(leap.label, "2024-02");
        assert_eq!(leap.range.start, "2024-02-01T00:00:00Z");
        assert_eq!(leap.range.end, "2024-03-01T00:00:00Z");
        let day = grouped("2024-02-29", GroupGranularity::Day);
        assert_eq!(day.range.end, "2024-03-01T00:00:00Z");
        let december = grouped("2026-12-31T23:59:59Z", GroupGranularity::Month);
        assert_eq!(december.range.end, "2027-01-01T00:00:00Z");
        let year = grouped("2026-12-31T23:59:59Z", GroupGranularity::Year);
        assert_eq!(year.label, "2026");
        assert_eq!(year.range.start, "2026-01-01T00:00:00Z");
        assert_eq!(year.range.end, "2027-01-01T00:00:00Z");
    }

    #[test]
    fn fractional_instants_use_exact_half_open_minute_and_hour_ranges() {
        let text = "2026-11-14T03:59:59.999999999Z";
        for granularity in [GroupGranularity::Minute, GroupGranularity::Hour] {
            let before = grouped(text, granularity);
            let next = grouped("2026-11-14T04:00:00.000000000Z", granularity);
            let instant = parse_datetime_nanos(text).unwrap();
            let start = parse_datetime_nanos(&before.range.start).unwrap();
            let end = parse_datetime_nanos(&before.range.end).unwrap();
            assert!(start <= instant && instant < end);
            assert_eq!(end - instant, 1);
            assert_eq!(next.range.start, before.range.end);
            assert_ne!(before.label, next.label);
        }
    }

    #[test]
    fn invalid_values_and_unrepresentable_drill_down_ranges_are_rejected() {
        for text in ["", "yesterday", "2026-02-30", "2026-11-14T25:00:00Z"] {
            assert_eq!(bucket(text, GroupGranularity::Day), Err(INVALID));
        }
        // Both instants are representable; their enclosing calendar days are not.
        for nanos in [i64::MIN, i64::MAX] {
            let text = chrono::DateTime::<Utc>::from_timestamp_nanos(nanos)
                .to_rfc3339_opts(SecondsFormat::Nanos, true);
            assert_eq!(bucket(&text, GroupGranularity::Day), Err(RANGE));
        }
    }
}
