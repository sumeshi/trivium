use polars::prelude::Series;

use crate::analysis::infer::{parse_datetime_nanos, series_kind_with_job, ColumnKind};
mod annotated;
mod values;

pub use annotated::histogram_series_annotated;
pub(crate) use annotated::histogram_series_annotated_with_job;
use values::{datetime_point, finite, float_point, format_nanos, int_point};

pub const DEFAULT_BUCKETS: usize = 50;

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq, ts_rs::TS)]
pub struct HistogramBucket {
    pub start: String,
    pub end: String,
    #[ts(type = "number")]
    pub count: u64,
    pub flags: HistogramFlags,
    pub end_inclusive: bool,
}

#[derive(Debug, Clone, Default, serde::Serialize, PartialEq, Eq, ts_rs::TS)]
pub struct HistogramFlags {
    #[ts(type = "number")]
    pub safe: u64,
    #[ts(type = "number")]
    pub suspicious: u64,
    #[ts(type = "number")]
    pub critical: u64,
    #[ts(type = "number")]
    pub unflagged: u64,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq, ts_rs::TS)]
pub struct HistogramRangeInfo {
    pub min: String,
    pub max: String,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq, ts_rs::TS)]
pub struct Histogram {
    pub column: String,
    pub kind: String,
    pub buckets: Vec<HistogramBucket>,
    #[ts(type = "number")]
    pub missing: u64,
    #[serde(default)]
    #[ts(type = "number")]
    pub outside: u64,
    pub full_range: Option<HistogramRangeInfo>,
    pub auto_focused: bool,
}

/// Equal-width buckets over the filtered row indexes. Every present value lands in one bucket.
pub fn histogram_series(
    column: &str,
    series: &Series,
    row_indices: &[usize],
    buckets: usize,
) -> Result<Histogram, String> {
    histogram_series_ranged(column, series, row_indices, buckets, None)
}

/// Like [`histogram_series`], but confines the buckets to `[min, max]`.
///
/// `min`/`max` are parsed with the same rules used for the column values
/// (integer text, finite floats, and the shared datetime formats). Values
/// outside the window are counted in `outside` instead of a bucket.
pub fn histogram_series_ranged(
    column: &str,
    series: &Series,
    row_indices: &[usize],
    buckets: usize,
    range: Option<(&str, &str)>,
) -> Result<Histogram, String> {
    let job = crate::jobs::JobContext::none();
    histogram_series_ranged_kind(
        column,
        series,
        row_indices,
        buckets,
        range,
        series_kind_with_job(series, &job).map_err(|e| e.to_string())?,
        &job,
    )
}

pub(crate) fn histogram_series_ranged_kind(
    column: &str,
    series: &Series,
    row_indices: &[usize],
    buckets: usize,
    range: Option<(&str, &str)>,
    kind: ColumnKind,
    job: &crate::jobs::JobContext,
) -> Result<Histogram, String> {
    let buckets = buckets.max(1);
    match kind {
        ColumnKind::Int => {
            let points = points(series, row_indices, int_point, job)?;
            match range {
                Some((min, max)) => {
                    let min = parse_int_bound(min)?;
                    let max = parse_int_bound(max)?;
                    histogram_i128_range(
                        column,
                        ColumnKind::Int,
                        &points,
                        buckets,
                        min,
                        max,
                        |value| value.to_string(),
                    )
                }
                None => Ok(histogram_i128(
                    column,
                    ColumnKind::Int,
                    &points,
                    buckets,
                    |value| value.to_string(),
                )),
            }
        }
        ColumnKind::Float => {
            let values = row_indices
                .iter()
                .enumerate()
                .map(|(position, idx)| {
                    if position % 1024 == 0 {
                        job.check_cancelled().map_err(|e| e.to_string())?;
                    }
                    Ok(float_point(series, *idx))
                })
                .collect::<Result<Vec<_>, String>>()?;
            match range {
                Some((min, max)) => {
                    let min = parse_float_bound(min)?;
                    let max = parse_float_bound(max)?;
                    histogram_f64_range(column, &values, buckets, min, max)
                }
                None => Ok(histogram_f64(column, &values, buckets)),
            }
        }
        ColumnKind::Datetime => {
            let points = points(series, row_indices, datetime_point, job)?;
            match range {
                Some((min, max)) => {
                    let min = parse_datetime_bound(min)?;
                    let max = parse_datetime_bound(max)?;
                    histogram_i128_range(
                        column,
                        ColumnKind::Datetime,
                        &points,
                        buckets,
                        min,
                        max,
                        format_nanos,
                    )
                }
                None => Ok(histogram_i128(
                    column,
                    ColumnKind::Datetime,
                    &points,
                    buckets,
                    format_nanos,
                )),
            }
        }
        _ => Err("Column is not numeric or datetime.".into()),
    }
}

fn parse_int_bound(text: &str) -> Result<i128, String> {
    text.trim()
        .parse::<i128>()
        .map_err(|_| format!("Invalid range bound: {text}"))
}

fn parse_float_bound(text: &str) -> Result<f64, String> {
    let value = text
        .trim()
        .parse::<f64>()
        .map_err(|_| format!("Invalid range bound: {text}"))?;
    finite(value).ok_or_else(|| format!("Invalid range bound: {text}"))
}

fn parse_datetime_bound(text: &str) -> Result<i128, String> {
    parse_datetime_nanos(text.trim())
        .map(i128::from)
        .ok_or_else(|| format!("Invalid range bound: {text}"))
}

fn points(
    series: &Series,
    row_indices: &[usize],
    read: fn(&Series, usize) -> Option<i128>,
    job: &crate::jobs::JobContext,
) -> Result<Vec<Option<i128>>, String> {
    row_indices
        .iter()
        .enumerate()
        .map(|(position, idx)| {
            if position % 1024 == 0 {
                job.check_cancelled().map_err(|e| e.to_string())?;
            }
            Ok(read(series, *idx))
        })
        .collect()
}

fn histogram_i128(
    column: &str,
    kind: ColumnKind,
    points: &[Option<i128>],
    buckets: usize,
    label: impl Fn(i128) -> String,
) -> Histogram {
    let (counts, range, missing) = counts_i128(points, buckets);
    let buckets = match range {
        Some((min, max)) => bucket_labels(&counts, buckets, |index| {
            let start = if min == max {
                min
            } else {
                min + (max - min) * index as i128 / buckets as i128
            };
            let end = if min == max || index + 1 == buckets {
                max
            } else {
                min + (max - min) * (index + 1) as i128 / buckets as i128
            };
            (label(start), label(end))
        }),
        None => empty_buckets(buckets),
    };
    Histogram {
        column: column.to_string(),
        kind: kind.as_str().to_string(),
        buckets,
        missing,
        outside: 0,
        full_range: None,
        auto_focused: false,
    }
}

fn histogram_i128_range(
    column: &str,
    kind: ColumnKind,
    points: &[Option<i128>],
    buckets: usize,
    min: i128,
    max: i128,
    label: impl Fn(i128) -> String,
) -> Result<Histogram, String> {
    if min > max {
        return Err("Invalid range: min is greater than max.".into());
    }
    let span = max
        .checked_sub(min)
        .ok_or_else(|| "Invalid range: span is too wide.".to_string())?;
    let mut counts = vec![0u64; buckets];
    let mut missing = 0u64;
    let mut outside = 0u64;
    for point in points {
        let Some(value) = point else {
            missing += 1;
            continue;
        };
        if *value < min || *value > max {
            outside += 1;
            continue;
        }
        let index = if span == 0 {
            0
        } else {
            let offset = value - min;
            let scaled = offset
                .checked_mul(buckets as i128)
                .ok_or_else(|| "Invalid range: span is too wide.".to_string())?;
            ((scaled / span) as usize).min(buckets - 1)
        };
        counts[index] += 1;
    }
    let edge = |index: usize| -> i128 {
        if span == 0 {
            return min;
        }
        span.checked_mul(index as i128)
            .and_then(|delta| delta.checked_div(buckets as i128))
            .and_then(|delta| min.checked_add(delta))
            .unwrap_or(max)
    };
    let buckets_out = bucket_labels(&counts, buckets, |index| {
        let start = edge(index);
        let end = if index + 1 == buckets {
            max
        } else {
            edge(index + 1)
        };
        (label(start), label(end))
    });
    Ok(Histogram {
        column: column.to_string(),
        kind: kind.as_str().to_string(),
        buckets: buckets_out,
        missing,
        outside,
        full_range: None,
        auto_focused: false,
    })
}

fn histogram_f64(column: &str, points: &[Option<f64>], buckets: usize) -> Histogram {
    let mut missing = 0u64;
    let mut present = Vec::new();
    for point in points {
        match point {
            Some(value) => present.push(*value),
            None => missing += 1,
        }
    }
    let mut counts = vec![0u64; buckets];
    let buckets_out = if let (Some(min), Some(max)) = (
        present.iter().copied().reduce(f64::min),
        present.iter().copied().reduce(f64::max),
    ) {
        if min == max {
            counts[0] = present.len() as u64;
        } else {
            let span = max - min;
            for value in &present {
                let mut index = ((value - min) / span * buckets as f64) as usize;
                if index >= buckets {
                    index = buckets - 1;
                }
                counts[index] += 1;
            }
        }
        bucket_labels(&counts, buckets, |index| {
            let start = if min == max {
                min
            } else {
                min + (max - min) * index as f64 / buckets as f64
            };
            let end = if min == max || index + 1 == buckets {
                max
            } else {
                min + (max - min) * (index + 1) as f64 / buckets as f64
            };
            (start.to_string(), end.to_string())
        })
    } else {
        empty_buckets(buckets)
    };
    Histogram {
        column: column.to_string(),
        kind: ColumnKind::Float.as_str().to_string(),
        buckets: buckets_out,
        missing,
        outside: 0,
        full_range: None,
        auto_focused: false,
    }
}

fn histogram_f64_range(
    column: &str,
    points: &[Option<f64>],
    buckets: usize,
    min: f64,
    max: f64,
) -> Result<Histogram, String> {
    if min > max {
        return Err("Invalid range: min is greater than max.".into());
    }
    let span = max - min;
    if !span.is_finite() {
        return Err("Invalid range: span is too wide.".into());
    }
    let mut counts = vec![0u64; buckets];
    let mut missing = 0u64;
    let mut outside = 0u64;
    for point in points {
        let Some(value) = point else {
            missing += 1;
            continue;
        };
        if *value < min || *value > max {
            outside += 1;
            continue;
        }
        let index = if span == 0.0 {
            0
        } else {
            let mut index = ((value - min) / span * buckets as f64) as usize;
            if index >= buckets {
                index = buckets - 1;
            }
            index
        };
        counts[index] += 1;
    }
    let buckets_out = bucket_labels(&counts, buckets, |index| {
        let start = if span == 0.0 {
            min
        } else {
            min + span * index as f64 / buckets as f64
        };
        let end = if span == 0.0 || index + 1 == buckets {
            max
        } else {
            min + span * (index + 1) as f64 / buckets as f64
        };
        (start.to_string(), end.to_string())
    });
    Ok(Histogram {
        column: column.to_string(),
        kind: ColumnKind::Float.as_str().to_string(),
        buckets: buckets_out,
        missing,
        outside,
        full_range: None,
        auto_focused: false,
    })
}

fn counts_i128(points: &[Option<i128>], buckets: usize) -> (Vec<u64>, Option<(i128, i128)>, u64) {
    let mut missing = 0u64;
    let mut present = Vec::new();
    for point in points {
        match point {
            Some(value) => present.push(*value),
            None => missing += 1,
        }
    }
    let mut counts = vec![0u64; buckets];
    if present.is_empty() {
        return (counts, None, missing);
    }
    let min = *present.iter().min().expect("present");
    let max = *present.iter().max().expect("present");
    if min == max {
        counts[0] = present.len() as u64;
    } else {
        let span = max - min;
        for value in present {
            let mut index = ((value - min) * buckets as i128 / span) as usize;
            if index >= buckets {
                index = buckets - 1;
            }
            counts[index] += 1;
        }
    }
    (counts, Some((min, max)), missing)
}

fn bucket_labels(
    counts: &[u64],
    buckets: usize,
    edge: impl Fn(usize) -> (String, String),
) -> Vec<HistogramBucket> {
    (0..buckets)
        .map(|index| {
            let (start, end) = edge(index);
            let end_inclusive = index + 1 == buckets || start == end;
            HistogramBucket {
                start,
                end,
                count: counts[index],
                flags: HistogramFlags {
                    unflagged: counts[index],
                    ..Default::default()
                },
                end_inclusive,
            }
        })
        .collect()
}

fn empty_buckets(buckets: usize) -> Vec<HistogramBucket> {
    (0..buckets)
        .map(|_| HistogramBucket {
            start: String::new(),
            end: String::new(),
            count: 0,
            flags: HistogramFlags::default(),
            end_inclusive: true,
        })
        .collect()
}
