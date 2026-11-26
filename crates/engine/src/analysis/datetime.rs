//! Unambiguous source spellings share one exact, nanosecond comparison key.
use std::sync::LazyLock;

use chrono::{Datelike, FixedOffset, NaiveDate};
use regex::{Captures, Regex};

const MAX_TIMESTAMP_BYTES: usize = 160;

static YEAR_FIRST: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
    r"\A(?P<year>[0-9]{4})(?P<sep1>[-/.])(?P<month>[0-9]{1,2})(?P<sep2>[-/.])(?P<day>[0-9]{1,2})",
    r"(?:(?:[Tt]|\s+)(?P<hour>[0-9]{1,2}):(?P<minute>[0-9]{1,2})",
    r"(?::(?P<second>[0-9]{1,2})(?:[.,](?P<fraction>[0-9]{1,9}))?)?",
    r"(?:\s*(?P<zone>[Zz]|(?i:UTC|GMT)|[+-][0-9]{2}:?[0-9]{2}))?)?\z"
)).unwrap()
});

static NAMED_MONTH: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
    r"\A(?:(?P<weekday>(?i:Mon|Tue|Wed|Thu|Fri|Sat|Sun)),\s+)?",
    r"(?P<day>[0-9]{1,2})\s+(?P<month_name>(?i:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\s+",
    r"(?P<year>[0-9]{4})\s+(?P<hour>[0-9]{1,2}):(?P<minute>[0-9]{1,2}):(?P<second>[0-9]{1,2})",
    r"(?:[.,](?P<fraction>[0-9]{1,9}))?\s+(?P<zone>[Zz]|(?i:UTC|GMT)|[+-][0-9]{2}:?[0-9]{2})\z"
)).unwrap()
});

pub(crate) fn parse_datetime_nanos(text: &str) -> Option<i64> {
    // Do not spend format probing or allocation on a large arbitrary log cell.
    if text.len() > MAX_TIMESTAMP_BYTES {
        return None;
    }
    let text = text.trim();
    let bytes = text.as_bytes();
    let year_first = bytes.len() >= 8
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && matches!(bytes[4], b'-' | b'/' | b'.');
    if year_first {
        // Keep the common RFC3339 path allocation-free. Chrono accepts excess
        // fractional digits by truncating them, so check precision beforehand.
        if rfc3339_precision(text) {
            if let Ok(value) = chrono::DateTime::parse_from_rfc3339(text) {
                if value.timestamp_subsec_nanos() < 1_000_000_000 {
                    return value.timestamp_nanos_opt();
                }
                return None;
            }
        }
        let captures = YEAR_FIRST.captures(text)?;
        if captures.name("sep1")?.as_str() != captures.name("sep2")?.as_str() {
            return None;
        }
        return captured_datetime(&captures, number(&captures, "month")?);
    }
    // Most text cells are not a named-month date. Avoid even the regex scan
    // unless the prefix could be a day number or an RFC-style weekday.
    let day_digits = bytes
        .iter()
        .take(3)
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    let plausible = ((1..=2).contains(&day_digits)
        && text[day_digits..]
            .chars()
            .next()
            .is_some_and(char::is_whitespace))
        || bytes.get(3) == Some(&b',');
    if !plausible {
        return None;
    }
    let captures = NAMED_MONTH.captures(text)?;
    let name = captures.name("month_name")?.as_str().to_ascii_lowercase();
    let month = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ]
    .iter()
    .position(|candidate| *candidate == name)? as u32
        + 1;
    captured_datetime(&captures, month)
}

fn rfc3339_precision(text: &str) -> bool {
    if text.as_bytes().get(19) != Some(&b'.') {
        return true;
    }
    let digits = text.as_bytes()[20..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    (1..=9).contains(&digits)
}

fn number(captures: &Captures<'_>, name: &str) -> Option<u32> {
    captures.name(name)?.as_str().parse().ok()
}

fn captured_datetime(captures: &Captures<'_>, month: u32) -> Option<i64> {
    let date = NaiveDate::from_ymd_opt(
        number(captures, "year")? as i32,
        month,
        number(captures, "day")?,
    )?;
    if let Some(day) = captures.name("weekday") {
        let expected = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
            [date.weekday().num_days_from_monday() as usize];
        if !day.as_str().eq_ignore_ascii_case(expected) {
            return None;
        }
    }
    let hour = number(captures, "hour").unwrap_or(0);
    let minute = number(captures, "minute").unwrap_or(0);
    let second = number(captures, "second").unwrap_or(0);
    // Leap-second interpretation requires an explicit policy; never flatten
    // one into the following minute or silently discard fractional precision.
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let fraction = captures
        .name("fraction")
        .map(|value| value.as_str())
        .unwrap_or("");
    let nanos = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<u32>()
            .ok()?
            .checked_mul(10u32.pow(9 - fraction.len() as u32))?
    };
    let naive = date.and_hms_nano_opt(hour, minute, second, nanos)?;
    let zone = captures
        .name("zone")
        .map(|value| value.as_str())
        .unwrap_or("Z");
    let seconds = if zone.starts_with('+') || zone.starts_with('-') {
        let compact = zone.replace(':', "");
        let hour = compact[1..3].parse::<i32>().ok()?;
        let minute = compact[3..5].parse::<i32>().ok()?;
        if hour > 23 || minute > 59 {
            return None;
        }
        (hour * 3600 + minute * 60) * if compact.starts_with('-') { -1 } else { 1 }
    } else {
        0
    };
    let offset = FixedOffset::east_opt(seconds)?;
    naive
        .and_local_timezone(offset)
        .single()?
        .timestamp_nanos_opt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Fixture {
        input: String,
        iso: Option<String>,
    }

    #[test]
    fn shared_source_datetime_fixtures_match_exact_instants_or_reject_without_guessing() {
        let fixtures: Vec<Fixture> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/datetime_formats.json"
        ))
        .unwrap();
        for fixture in fixtures {
            let expected = fixture.iso.as_deref().map(|text| {
                chrono::DateTime::parse_from_rfc3339(text)
                    .unwrap()
                    .timestamp_nanos_opt()
                    .unwrap()
            });
            assert_eq!(
                parse_datetime_nanos(&fixture.input),
                expected,
                "{}",
                fixture.input
            );
        }
    }

    #[test]
    fn large_and_obviously_nondatetime_cells_do_not_probe_formats() {
        assert!(parse_datetime_nanos(&"2026-11-14 ".repeat(10_000)).is_none());
        for source in [
            "powershell.exe",
            "1234567890123456",
            "C:\\Windows\\Temp",
            "00123",
            "1.5",
        ] {
            assert!(parse_datetime_nanos(source).is_none(), "{source}");
        }
    }
}
