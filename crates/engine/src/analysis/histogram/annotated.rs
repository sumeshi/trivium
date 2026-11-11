use std::collections::HashMap;

use polars::prelude::Series;

use crate::analysis::infer::{parse_datetime_nanos, series_kind, ColumnKind};
use crate::models::FlagEntry;

use super::values::{datetime_point, float_point, format_nanos, int_point};
use super::{
    histogram_series_ranged, Histogram, HistogramBucket, HistogramFlags, HistogramRangeInfo,
};

/// Shared histogram with manual flag counts. Initial timeline focus uses a
/// bounded deterministic sample; timestamps are parsed once and never rounded.
pub fn histogram_series_annotated(
    column: &str,
    series: &Series,
    rows: &[usize],
    buckets: usize,
    range: Option<(&str, &str)>,
    flags: &HashMap<usize, FlagEntry>,
    focus: bool,
) -> Result<Histogram, String> {
    if series_kind(series) == ColumnKind::Datetime {
        return datetime_histogram(column, series, rows, buckets.max(1), range, flags, focus);
    }
    let mut result = histogram_series_ranged(column, series, rows, buckets, range)?;
    // The numeric stats view retains its existing bucket calculation. Count
    // flags without retaining a second vector of numeric values.
    for bucket in &mut result.buckets {
        bucket.flags = HistogramFlags::default();
    }
    if let (Some(first), Some(last)) = (result.buckets.first(), result.buckets.last()) {
        if !first.start.is_empty() {
            let min = first.start.clone();
            let max = last.end.clone();
            let int_bounds = if result.kind == "int" {
                Some((
                    min.parse::<i128>().map_err(|error| error.to_string())?,
                    max.parse::<i128>().map_err(|error| error.to_string())?,
                ))
            } else {
                None
            };
            let float_bounds = if int_bounds.is_none() {
                Some((
                    min.parse::<f64>().map_err(|error| error.to_string())?,
                    max.parse::<f64>().map_err(|error| error.to_string())?,
                ))
            } else {
                None
            };
            for row in rows {
                let index = if let Some((low, high)) = int_bounds {
                    int_point(series, *row)
                        .and_then(|point| integer_index(point, low, high, result.buckets.len()))
                } else {
                    let (low, high) = float_bounds.expect("numeric bounds");
                    float_point(series, *row).and_then(|point| {
                        (point >= low && point <= high).then(|| {
                            if low == high {
                                0
                            } else {
                                (((point - low) / (high - low) * result.buckets.len() as f64)
                                    as usize)
                                    .min(result.buckets.len() - 1)
                            }
                        })
                    })
                };
                if let Some(index) = index {
                    add_flag(&mut result.buckets[index].flags, flags.get(row));
                }
            }
        }
    }
    Ok(result)
}

fn add_flag(counts: &mut HistogramFlags, entry: Option<&FlagEntry>) {
    let value = entry
        .map(|entry| crate::flags::normalize_flag_value(&entry.flag))
        .unwrap_or_default();
    match value.as_str() {
        "safe" => counts.safe += 1,
        "suspicious" => counts.suspicious += 1,
        "critical" => counts.critical += 1,
        _ => counts.unflagged += 1,
    }
}

fn integer_index(value: i128, min: i128, max: i128, buckets: usize) -> Option<usize> {
    if value < min || value > max {
        return None;
    }
    Some(if min == max {
        0
    } else {
        (((value - min) * buckets as i128 / (max - min)) as usize).min(buckets - 1)
    })
}

fn datetime_histogram(
    column: &str,
    series: &Series,
    rows: &[usize],
    buckets: usize,
    range: Option<(&str, &str)>,
    flags: &HashMap<usize, FlagEntry>,
    focus: bool,
) -> Result<Histogram, String> {
    let mut points = Vec::with_capacity(rows.len());
    let mut sample = Vec::with_capacity(4096.min(rows.len()));
    let mut random = 0x4d595df4d0f33173u64;
    let mut missing = 0;
    let mut full_min = i128::MAX;
    let mut full_max = i128::MIN;
    for row in rows {
        let Some(value) = datetime_point(series, *row) else {
            missing += 1;
            continue;
        };
        full_min = full_min.min(value);
        full_max = full_max.max(value);
        points.push((value, *row));
        if sample.len() < 4096 {
            sample.push(value);
        } else if focus && range.is_none() {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let slot = random as usize % points.len();
            if slot < sample.len() {
                sample[slot] = value;
            }
        }
    }
    let full_range = (!points.is_empty()).then(|| HistogramRangeInfo {
        min: format_nanos(full_min),
        max: format_nanos(full_max),
    });
    let explicit = range
        .map(|(min, max)| {
            let parse = |text: &str| {
                parse_datetime_nanos(text)
                    .map(i128::from)
                    .ok_or_else(|| format!("Invalid range bound: {text}"))
            };
            Ok::<_, String>((parse(min)?, parse(max)?))
        })
        .transpose()?;
    let suggested = (focus && range.is_none() && !points.is_empty())
        .then(|| dense_window(&mut sample, full_min, full_max))
        .flatten();
    let (min, max) = explicit.or(suggested).unwrap_or((full_min, full_max));
    if min > max && (explicit.is_some() || !points.is_empty()) {
        return Err("Invalid range: min is greater than max.".into());
    }
    let mut output = (0..buckets)
        .map(|index| HistogramBucket {
            start: if points.is_empty() && explicit.is_none() {
                String::new()
            } else {
                format_nanos(edge(min, max, index, buckets))
            },
            end: if points.is_empty() && explicit.is_none() {
                String::new()
            } else {
                format_nanos(edge(min, max, index + 1, buckets))
            },
            count: 0,
            flags: HistogramFlags::default(),
            end_inclusive: min == max || index + 1 == buckets,
        })
        .collect::<Vec<_>>();
    let mut outside = 0;
    for (point, row) in points {
        if let Some(index) = integer_index(point, min, max, buckets) {
            output[index].count += 1;
            add_flag(&mut output[index].flags, flags.get(&row));
        } else {
            outside += 1;
        }
    }
    Ok(Histogram {
        column: column.into(),
        kind: "datetime".into(),
        buckets: output,
        missing,
        outside,
        full_range,
        auto_focused: suggested.is_some(),
    })
}

// Integer nanosecond bucket boundaries round up. Every bucket is [start,end),
// except the final bucket includes the exact maximum. This agrees with index
// floor((value-min)*buckets/span), even when span is smaller than bucket count.
fn edge(min: i128, max: i128, index: usize, buckets: usize) -> i128 {
    if min == max || index == 0 {
        min
    } else {
        min + ((max - min) * index as i128 + buckets as i128 - 1) / buckets as i128
    }
}

fn dense_window(sample: &mut [i128], full_min: i128, full_max: i128) -> Option<(i128, i128)> {
    if sample.len() < 20 || full_min == full_max {
        return None;
    }
    sample.sort_unstable();
    let retained = (sample.len() * 95).div_ceil(100);
    let (start, end) = sample
        .windows(retained)
        .map(|window| (window[0], window[retained - 1]))
        .min_by_key(|(start, end)| end - start)?;
    let width = end - start;
    if width * 4 > full_max - full_min {
        return None;
    }
    let padding = (width / 20).max(1);
    let start = (start - padding).max(full_min);
    let end = (end + padding).min(full_max);
    (start > full_min || end < full_max).then_some((start, end))
}
