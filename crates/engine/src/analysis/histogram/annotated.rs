use std::collections::HashMap;

use polars::prelude::Series;

use crate::analysis::infer::{parse_datetime_nanos, series_kind_for_rows_with_job, ColumnKind};
use crate::models::FlagEntry;

use super::integer::IntegerBins;
use super::values::{datetime_point, float_point, format_nanos, int_point};
use super::{Histogram, HistogramBucket, HistogramFlags, HistogramRangeInfo};

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
    histogram_series_annotated_with_job(
        column,
        series,
        rows,
        buckets,
        range,
        flags,
        focus,
        false,
        &crate::jobs::JobContext::none(),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn histogram_series_annotated_with_job(
    column: &str,
    series: &Series,
    rows: &[usize],
    buckets: usize,
    range: Option<(&str, &str)>,
    flags: &HashMap<usize, FlagEntry>,
    focus: bool,
    as_datetime: bool,
    job: &crate::jobs::JobContext,
) -> Result<Histogram, String> {
    let kind = if as_datetime {
        ColumnKind::Datetime
    } else {
        series_kind_for_rows_with_job(series, rows, job).map_err(|e| e.to_string())?
    };
    if kind == ColumnKind::Datetime {
        return datetime_histogram(
            column,
            series,
            rows,
            buckets.max(1),
            range,
            flags,
            focus,
            job,
        );
    }
    let mut result =
        super::histogram_series_ranged_kind(column, series, rows, buckets, range, kind, job)?;
    // The numeric stats view retains its existing bucket calculation. Count
    // flags without retaining a second vector of numeric values.
    for bucket in &mut result.buckets {
        bucket.flags = HistogramFlags::default();
    }
    if let (Some(first), Some(last)) = (result.buckets.first(), result.buckets.last()) {
        if !first.start.is_empty() {
            let min = first.start.clone();
            let max = last.end.clone();
            let int_bins = if result.kind == "int" {
                Some(IntegerBins::new(
                    min.parse::<i128>().map_err(|error| error.to_string())?,
                    max.parse::<i128>().map_err(|error| error.to_string())?,
                    result.buckets.len(),
                ))
            } else {
                None
            };
            let float_bounds = if int_bins.is_none() {
                Some((
                    min.parse::<f64>().map_err(|error| error.to_string())?,
                    max.parse::<f64>().map_err(|error| error.to_string())?,
                ))
            } else {
                None
            };
            for (position, row) in rows.iter().enumerate() {
                if position % 1024 == 0 {
                    job.check_cancelled().map_err(|e| e.to_string())?;
                }
                let index = if let Some(bins) = &int_bins {
                    int_point(series, *row).and_then(|point| bins.index(point))
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

#[allow(clippy::too_many_arguments)]
fn datetime_histogram(
    column: &str,
    series: &Series,
    rows: &[usize],
    buckets: usize,
    range: Option<(&str, &str)>,
    flags: &HashMap<usize, FlagEntry>,
    focus: bool,
    job: &crate::jobs::JobContext,
) -> Result<Histogram, String> {
    let mut points = Vec::with_capacity(rows.len());
    let mut sample = Vec::with_capacity(4096.min(rows.len()));
    let mut random = 0x4d595df4d0f33173u64;
    let mut missing = 0;
    let mut invalid = 0;
    let mut full_min = i128::MAX;
    let mut full_max = i128::MIN;
    for (position, row) in rows.iter().enumerate() {
        if position % 1024 == 0 {
            job.check_cancelled().map_err(|e| e.to_string())?;
        }
        let Some(value) = datetime_point(series, *row) else {
            let present = series
                .get(*row)
                .ok()
                .and_then(|value| crate::value_utils::anyvalue_to_search_string(&value))
                .is_some_and(|text| !text.trim().is_empty());
            if present {
                invalid += 1;
            } else {
                missing += 1;
            }
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
    let bins =
        (explicit.is_some() || !points.is_empty()).then(|| IntegerBins::new(min, max, buckets));
    let mut output = (0..buckets)
        .map(|index| HistogramBucket {
            start: bins
                .as_ref()
                .map(|bins| format_nanos(bins.edge(index)))
                .unwrap_or_default(),
            end: bins
                .as_ref()
                .map(|bins| format_nanos(bins.edge(index + 1)))
                .unwrap_or_default(),
            count: 0,
            flags: HistogramFlags::default(),
            end_inclusive: bins.as_ref().is_none_or(|bins| bins.end_inclusive(index)),
        })
        .collect::<Vec<_>>();
    let mut outside = 0;
    for (position, (point, row)) in points.into_iter().enumerate() {
        if position % 1024 == 0 {
            job.check_cancelled().map_err(|e| e.to_string())?;
        }
        if let Some(index) = bins.as_ref().and_then(|bins| bins.index(point)) {
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
        invalid,
        outside,
        full_range,
        auto_focused: suggested.is_some(),
    })
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
