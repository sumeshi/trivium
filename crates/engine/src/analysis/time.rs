use polars::prelude::Series;
use serde_json::Value;

use crate::analysis::infer::{parse_datetime_nanos, series_kind_with_job, ColumnKind};
use crate::value_utils::anyvalue_to_json;

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq, ts_rs::TS)]
pub struct TimeSpan {
    pub start: String,
    pub end: String,
    #[ts(type = "number")]
    pub count: u64,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq, ts_rs::TS)]
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
    analyze_series_with_job(
        column,
        series,
        row_indices,
        gap_seconds,
        session_minutes,
        burst_window_seconds,
        burst_min,
        &crate::jobs::JobContext::none(),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn analyze_series_with_job(
    column: &str,
    series: &Series,
    row_indices: &[usize],
    gap_seconds: u64,
    session_minutes: u64,
    burst_window_seconds: u64,
    burst_min: u64,
    job: &crate::jobs::JobContext,
) -> Result<TimeAnalysis, String> {
    if series_kind_with_job(series, job).map_err(|e| e.to_string())? != ColumnKind::Datetime {
        return Err("Column is not datetime.".into());
    }
    let mut events = Vec::new();
    for (position, idx) in row_indices.iter().enumerate() {
        if position % 1024 == 0 {
            job.check_cancelled().map_err(|e| e.to_string())?;
        }
        if let Some(event) = event_at(series, *idx) {
            events.push(event);
        }
    }
    events.sort_by(|left, right| {
        left.nanos
            .cmp(&right.nanos)
            .then_with(|| left.text.cmp(&right.text))
    });
    job.check_cancelled().map_err(|e| e.to_string())?;
    let result = TimeAnalysis {
        column: column.to_string(),
        gaps: gaps(&events, gap_seconds, job)?,
        sessions: sessions(&events, session_minutes, job)?,
        bursts: bursts(&events, burst_window_seconds, burst_min, job)?,
    };
    job.check_cancelled().map_err(|e| e.to_string())?;
    Ok(result)
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

fn gaps(
    events: &[Event],
    gap_seconds: u64,
    job: &crate::jobs::JobContext,
) -> Result<Vec<TimeSpan>, String> {
    let minimum = i128::from(gap_seconds) * 1_000_000_000;
    let mut spans = Vec::new();
    for (index, pair) in events.windows(2).enumerate() {
        if index % 1024 == 0 {
            job.check_cancelled().map_err(|e| e.to_string())?;
        }
        let delta = i128::from(pair[1].nanos) - i128::from(pair[0].nanos);
        if delta >= minimum {
            spans.push(TimeSpan {
                start: pair[0].text.clone(),
                end: pair[1].text.clone(),
                count: (delta / 1_000_000_000) as u64,
            });
        }
    }
    Ok(spans)
}

fn sessions(
    events: &[Event],
    session_minutes: u64,
    job: &crate::jobs::JobContext,
) -> Result<Vec<TimeSpan>, String> {
    if events.is_empty() {
        return Ok(Vec::new());
    }
    let minimum = i128::from(session_minutes) * 60 * 1_000_000_000;
    let mut spans = Vec::new();
    let mut start = 0;
    for index in 1..events.len() {
        if index % 1024 == 0 {
            job.check_cancelled().map_err(|e| e.to_string())?;
        }
        let delta = i128::from(events[index].nanos) - i128::from(events[index - 1].nanos);
        if delta >= minimum {
            spans.push(span(&events[start..index]));
            start = index;
        }
    }
    spans.push(span(&events[start..]));
    Ok(spans)
}

fn bursts(
    events: &[Event],
    window_seconds: u64,
    burst_min: u64,
    job: &crate::jobs::JobContext,
) -> Result<Vec<TimeSpan>, String> {
    let window = i128::from(window_seconds) * 1_000_000_000;
    let mut merged: Vec<(usize, usize)> = Vec::new();
    let mut end = 0;
    for start in 0..events.len() {
        if start % 1024 == 0 {
            job.check_cancelled().map_err(|e| e.to_string())?;
        }
        end = end.max(start);
        while end + 1 < events.len()
            && i128::from(events[end + 1].nanos) - i128::from(events[start].nanos) <= window
        {
            end += 1;
            if end % 1024 == 0 {
                job.check_cancelled().map_err(|e| e.to_string())?;
            }
        }
        if (end - start + 1) as u64 > burst_min {
            if let Some(last) = merged.last_mut() {
                if start <= last.1 {
                    last.1 = last.1.max(end);
                    continue;
                }
            }
            merged.push((start, end));
        }
    }
    Ok(merged
        .into_iter()
        .map(|(start, end)| span(&events[start..=end]))
        .collect())
}

fn span(events: &[Event]) -> TimeSpan {
    TimeSpan {
        start: events[0].text.clone(),
        end: events[events.len() - 1].text.clone(),
        count: events.len() as u64,
    }
}
