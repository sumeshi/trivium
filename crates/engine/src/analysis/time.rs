use polars::prelude::Series;
use serde_json::Value;

use crate::analysis::infer::{parse_datetime_nanos, series_kind_for_rows_with_job, ColumnKind};
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
    #[ts(type = "number")]
    pub missing: u64,
    #[ts(type = "number")]
    pub invalid: u64,
    pub gaps: Vec<TimeSpan>,
    pub sessions: Vec<TimeSpan>,
    pub bursts: Vec<TimeSpan>,
}

struct Event {
    nanos: i64,
    text: String,
}

const MAX_SPANS: usize = 10_000;
const MAX_SPAN_BYTES: usize = 32 * 1024 * 1024;

#[derive(Default)]
struct SpanBudget {
    count: usize,
    bytes: usize,
}

fn span_limit_error() -> String {
    "Time analysis exceeds the interval limit. Narrow the filters or increase the analysis thresholds.".into()
}

impl SpanBudget {
    fn remaining(&self) -> usize {
        MAX_SPANS - self.count
    }

    fn push(
        &mut self,
        spans: &mut Vec<TimeSpan>,
        start: &str,
        end: &str,
        count: u64,
    ) -> Result<(), String> {
        let bytes = self
            .bytes
            .saturating_add(start.len())
            .saturating_add(end.len())
            .saturating_add(std::mem::size_of::<TimeSpan>() + 64);
        if self.count == MAX_SPANS || bytes > MAX_SPAN_BYTES {
            return Err(span_limit_error());
        }
        // Account before cloning timestamps. Large padded source timestamps
        // must not bypass the retained result bound through their labels.
        self.count += 1;
        self.bytes = bytes;
        spans.push(TimeSpan {
            start: start.into(),
            end: end.into(),
            count,
        });
        Ok(())
    }
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
        false,
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
    as_datetime: bool,
    job: &crate::jobs::JobContext,
) -> Result<TimeAnalysis, String> {
    if !as_datetime
        && series_kind_for_rows_with_job(series, row_indices, job).map_err(|e| e.to_string())?
            != ColumnKind::Datetime
    {
        return Err("Column is not datetime.".into());
    }
    let mut events = Vec::new();
    let mut missing = 0;
    let mut invalid = 0;
    for (position, idx) in row_indices.iter().enumerate() {
        if position % 1024 == 0 {
            job.check_cancelled().map_err(|e| e.to_string())?;
        }
        if let Some(event) = event_at(series, *idx) {
            events.push(event);
        } else {
            let present = series
                .get(*idx)
                .ok()
                .and_then(|value| crate::value_utils::anyvalue_to_search_string(&value))
                .is_some_and(|text| !text.trim().is_empty());
            if present {
                invalid += 1;
            } else {
                missing += 1;
            }
        }
    }
    events.sort_by(|left, right| {
        left.nanos
            .cmp(&right.nanos)
            .then_with(|| left.text.cmp(&right.text))
    });
    job.check_cancelled().map_err(|e| e.to_string())?;
    let mut budget = SpanBudget::default();
    let result = TimeAnalysis {
        column: column.to_string(),
        missing,
        invalid,
        gaps: gaps(&events, gap_seconds, job, &mut budget)?,
        sessions: sessions(&events, session_minutes, job, &mut budget)?,
        bursts: bursts(&events, burst_window_seconds, burst_min, job, &mut budget)?,
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
    budget: &mut SpanBudget,
) -> Result<Vec<TimeSpan>, String> {
    let minimum = i128::from(gap_seconds) * 1_000_000_000;
    let mut spans = Vec::new();
    for (index, pair) in events.windows(2).enumerate() {
        if index % 1024 == 0 {
            job.check_cancelled().map_err(|e| e.to_string())?;
        }
        let delta = i128::from(pair[1].nanos) - i128::from(pair[0].nanos);
        if delta >= minimum {
            budget.push(
                &mut spans,
                &pair[0].text,
                &pair[1].text,
                (delta / 1_000_000_000) as u64,
            )?;
        }
    }
    Ok(spans)
}

fn sessions(
    events: &[Event],
    session_minutes: u64,
    job: &crate::jobs::JobContext,
    budget: &mut SpanBudget,
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
            push_span(&events[start..index], &mut spans, budget)?;
            start = index;
        }
    }
    push_span(&events[start..], &mut spans, budget)?;
    Ok(spans)
}

fn bursts(
    events: &[Event],
    window_seconds: u64,
    burst_min: u64,
    job: &crate::jobs::JobContext,
    budget: &mut SpanBudget,
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
            if merged.len() == budget.remaining() {
                return Err(span_limit_error());
            }
            merged.push((start, end));
        }
    }
    let mut spans = Vec::new();
    for (index, (start, end)) in merged.into_iter().enumerate() {
        if index.is_multiple_of(1024) {
            job.check_cancelled().map_err(|e| e.to_string())?;
        }
        push_span(&events[start..=end], &mut spans, budget)?;
    }
    Ok(spans)
}

fn push_span(
    events: &[Event],
    spans: &mut Vec<TimeSpan>,
    budget: &mut SpanBudget,
) -> Result<(), String> {
    budget.push(
        spans,
        &events[0].text,
        &events[events.len() - 1].text,
        events.len() as u64,
    )
}

#[cfg(test)]
mod tests;
