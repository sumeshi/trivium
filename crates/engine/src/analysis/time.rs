use polars::prelude::Series;
use serde_json::Value;

use crate::analysis::infer::{parse_datetime_nanos, series_kind, ColumnKind};
use crate::value_utils::anyvalue_to_json;

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct TimeSpan {
    pub start: String,
    pub end: String,
    pub count: u64,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct TimeAnalysis {
    pub column: String,
    pub gaps: Vec<TimeSpan>,
    pub sessions: Vec<TimeSpan>,
    pub bursts: Vec<TimeSpan>,
}

struct Event {
    nanos: i64,
    text: String,
}

/// Gaps are quiet intervals of at least `gap_seconds`. Sessions break on a gap of
/// `session_minutes`. Bursts are merged windows of `burst_window_seconds` with more
/// than `burst_min` events.
pub fn analyze_series(
    column: &str,
    series: &Series,
    row_indices: &[usize],
    gap_seconds: u64,
    session_minutes: u64,
    burst_window_seconds: u64,
    burst_min: u64,
) -> Result<TimeAnalysis, String> {
    if series_kind(series) != ColumnKind::Datetime {
        return Err("Column is not datetime.".into());
    }
    let mut events = Vec::new();
    for idx in row_indices {
        if let Some(event) = event_at(series, *idx) {
            events.push(event);
        }
    }
    events.sort_by(|left, right| left.nanos.cmp(&right.nanos).then(left.text.cmp(&right.text)));
    Ok(TimeAnalysis {
        column: column.to_string(),
        gaps: gaps(&events, gap_seconds),
        sessions: sessions(&events, session_minutes),
        bursts: bursts(&events, burst_window_seconds, burst_min),
    })
}

fn event_at(series: &Series, idx: usize) -> Option<Event> {
    let value = series.get(idx).ok()?;
    let json = anyvalue_to_json(&value);
    let text = match json {
        Value::Null => return None,
        Value::String(text) => text,
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    };
    let nanos = parse_datetime_nanos(text.trim())?;
    Some(Event { nanos, text })
}

fn gaps(events: &[Event], gap_seconds: u64) -> Vec<TimeSpan> {
    let minimum = i128::from(gap_seconds) * 1_000_000_000;
    events
        .windows(2)
        .filter_map(|pair| {
            let delta = i128::from(pair[1].nanos) - i128::from(pair[0].nanos);
            if delta < minimum {
                return None;
            }
            Some(TimeSpan {
                start: pair[0].text.clone(),
                end: pair[1].text.clone(),
                count: (delta / 1_000_000_000) as u64,
            })
        })
        .collect()
}

fn sessions(events: &[Event], session_minutes: u64) -> Vec<TimeSpan> {
    if events.is_empty() {
        return Vec::new();
    }
    let minimum = i128::from(session_minutes) * 60 * 1_000_000_000;
    let mut spans = Vec::new();
    let mut start = 0usize;
    for index in 1..events.len() {
        let delta = i128::from(events[index].nanos) - i128::from(events[index - 1].nanos);
        if delta >= minimum {
            spans.push(span(&events[start..index]));
            start = index;
        }
    }
    spans.push(span(&events[start..]));
    spans
}

fn bursts(events: &[Event], window_seconds: u64, burst_min: u64) -> Vec<TimeSpan> {
    if events.is_empty() {
        return Vec::new();
    }
    let window = i128::from(window_seconds) * 1_000_000_000;
    let mut ranges = Vec::new();
    let mut end = 0usize;
    for start in 0..events.len() {
        if end < start {
            end = start;
        }
        while end + 1 < events.len()
            && i128::from(events[end + 1].nanos) - i128::from(events[start].nanos) <= window
        {
            end += 1;
        }
        let count = end - start + 1;
        if count as u64 > burst_min {
            ranges.push((start, end));
        }
    }
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for range in ranges {
        if let Some(last) = merged.last_mut() {
            if range.0 <= last.1 {
                last.1 = last.1.max(range.1);
                continue;
            }
        }
        merged.push(range);
    }
    merged
        .into_iter()
        .map(|(start, end)| span(&events[start..=end]))
        .collect()
}

fn span(events: &[Event]) -> TimeSpan {
    TimeSpan {
        start: events[0].text.clone(),
        end: events[events.len() - 1].text.clone(),
        count: events.len() as u64,
    }
}
