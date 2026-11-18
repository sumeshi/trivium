use polars::prelude::Series;

use crate::analysis::infer::{parse_datetime_nanos, series_kind_for_rows_with_job, ColumnKind};
mod annotated;
mod integer;
mod values;

pub use annotated::histogram_series_annotated;
pub(crate) use annotated::histogram_series_annotated_with_job;
use integer::IntegerBins;
use values::{datetime_point, finite, float_point, format_nanos, int_point};

pub const DEFAULT_BUCKETS: usize = 50;
/// Keep caller-controlled histogram output bounded, including empty buckets.
/// The GUI uses at most 200; automation may request finer intervals.
pub const MAX_HISTOGRAM_BUCKETS: usize = 4096;

pub(crate) fn validate_bucket_count(buckets: usize) -> Result<(), String> {
    if buckets > MAX_HISTOGRAM_BUCKETS {
        return Err(format!(
            "Histogram supports at most {MAX_HISTOGRAM_BUCKETS} buckets."
        ));
    }
    Ok(())
}

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
    /// Present values that could not be parsed under the requested interpretation.
    #[serde(default)]
    #[ts(type = "number")]
    pub invalid: u64,
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
    validate_bucket_count(buckets)?;
    let job = crate::jobs::JobContext::none();
    histogram_series_ranged_kind(
        column,
        series,
        row_indices,
        buckets,
        range,
        series_kind_for_rows_with_job(series, row_indices, &job).map_err(|e| e.to_string())?,
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
                None => histogram_f64(column, &values, buckets),
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
        Some((min, max)) => integer_buckets(&counts, &IntegerBins::new(min, max, buckets), label),
        None => empty_buckets(buckets),
    };
    Histogram {
        column: column.to_string(),
        kind: kind.as_str().to_string(),
        buckets,
        missing,
        invalid: 0,
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
    let bins = IntegerBins::new(min, max, buckets);
    let mut counts = vec![0u64; buckets];
    let mut missing = 0u64;
    let mut outside = 0u64;
    for point in points {
        let Some(value) = point else {
            missing += 1;
            continue;
        };
        if let Some(index) = bins.index(*value) {
            counts[index] += 1;
        } else {
            outside += 1;
        }
    }
    let buckets_out = integer_buckets(&counts, &bins, label);
    Ok(Histogram {
        column: column.to_string(),
        kind: kind.as_str().to_string(),
        buckets: buckets_out,
        missing,
        invalid: 0,
        outside,
        full_range: None,
        auto_focused: false,
    })
}

fn histogram_f64(
    column: &str,
    points: &[Option<f64>],
    buckets: usize,
) -> Result<Histogram, String> {
    if let (Some(min), Some(max)) = (
        points.iter().flatten().copied().reduce(f64::min),
        points.iter().flatten().copied().reduce(f64::max),
    ) {
        return histogram_f64_range(column, points, buckets, min, max);
    }
    Ok(Histogram {
        column: column.to_string(),
        kind: ColumnKind::Float.as_str().to_string(),
        buckets: empty_buckets(buckets),
        missing: points.len() as u64,
        invalid: 0,
        outside: 0,
        full_range: None,
        auto_focused: false,
    })
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
    let buckets_out = bucket_labels(
        &counts,
        buckets,
        |index| {
            let start = if span == 0.0 {
                min
            } else {
                min + span * (index as f64 / buckets as f64)
            };
            let end = if span == 0.0 || index + 1 == buckets {
                max
            } else {
                min + span * ((index + 1) as f64 / buckets as f64)
            };
            (start.to_string(), end.to_string())
        },
        |index| {
            if span == 0.0 {
                index == 0
            } else {
                index + 1 == buckets
            }
        },
    );
    Ok(Histogram {
        column: column.to_string(),
        kind: ColumnKind::Float.as_str().to_string(),
        buckets: buckets_out,
        missing,
        invalid: 0,
        outside,
        full_range: None,
        auto_focused: false,
    })
}

fn counts_i128(points: &[Option<i128>], buckets: usize) -> (Vec<u64>, Option<(i128, i128)>, u64) {
    let missing = points.iter().filter(|value| value.is_none()).count() as u64;
    let mut counts = vec![0u64; buckets];
    let Some(min) = points.iter().flatten().min().copied() else {
        return (counts, None, missing);
    };
    let max = points.iter().flatten().max().copied().expect("present");
    let bins = IntegerBins::new(min, max, buckets);
    for value in points.iter().flatten() {
        counts[bins.index(*value).expect("value within computed range")] += 1;
    }
    (counts, Some((min, max)), missing)
}

fn integer_buckets(
    counts: &[u64],
    bins: &IntegerBins,
    label: impl Fn(i128) -> String,
) -> Vec<HistogramBucket> {
    bucket_labels(
        counts,
        counts.len(),
        |index| (label(bins.edge(index)), label(bins.edge(index + 1))),
        |index| bins.end_inclusive(index),
    )
}

fn bucket_labels(
    counts: &[u64],
    buckets: usize,
    edge: impl Fn(usize) -> (String, String),
    end_inclusive: impl Fn(usize) -> bool,
) -> Vec<HistogramBucket> {
    (0..buckets)
        .map(|index| {
            let (start, end) = edge(index);
            HistogramBucket {
                start,
                end,
                count: counts[index],
                flags: HistogramFlags {
                    unflagged: counts[index],
                    ..Default::default()
                },
                end_inclusive: end_inclusive(index),
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
