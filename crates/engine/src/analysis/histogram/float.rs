//! Approximate axis spacing, exact decimal boundaries and source membership.
use std::{collections::HashMap, mem::size_of};

use polars::prelude::Series;

use crate::{
    jobs::JobContext, models::FlagEntry, number::ExactNumber,
    value_utils::bounded_text::present_text,
};

use super::{annotated::add_flag, Histogram, HistogramBucket, HistogramFlags};

const MAX_BOUNDARY_BYTES: usize = 32 * 1024 * 1024;

fn budget_error() -> String {
    "Numeric histogram exceeds its 32 MiB exact boundary budget. Narrow the input values or use fewer buckets.".into()
}

struct Point {
    label: String,
    exact: ExactNumber,
    axis: f64,
}

impl Point {
    fn parse(text: &str, available: usize) -> Result<Option<Self>, String> {
        let text = text.trim();
        // Reserve before ExactNumber constructs its significant digits. Four
        // text copies cover the key, label and repeated start/end output labels.
        if estimate_bytes(text.len()) > available {
            return Err(budget_error());
        }
        let Some(axis) = text.parse::<f64>().ok().filter(|value| value.is_finite()) else {
            return Ok(None);
        };
        let Some(exact) = ExactNumber::parse(text) else {
            return Ok(None);
        };
        Ok(Some(Self {
            label: text.into(),
            exact,
            axis,
        }))
    }

    fn bytes(&self) -> usize {
        estimate_bytes(self.label.len())
    }
}

fn estimate_bytes(text: usize) -> usize {
    text.saturating_mul(4)
        .saturating_add(size_of::<Point>() + 64)
}

fn point_at(series: &Series, row: usize, available: usize) -> Result<Option<Point>, String> {
    let Ok(value) = series.get(row) else {
        return Ok(None);
    };
    let Some(text) = present_text(&value, available / 4).map_err(|_| budget_error())? else {
        return Ok(None);
    };
    Point::parse(&text, available)
}

fn extents(
    series: &Series,
    rows: &[usize],
    job: &JobContext,
) -> Result<Option<(Point, Point)>, String> {
    let mut bounds: Option<(Point, Point)> = None;
    for (position, row) in rows.iter().enumerate() {
        if position.is_multiple_of(1024) {
            job.check_cancelled().map_err(|error| error.to_string())?;
        }
        let retained = bounds
            .as_ref()
            .map_or(0, |(min, max)| min.bytes() + max.bytes());
        let Some(point) = point_at(series, *row, MAX_BOUNDARY_BYTES.saturating_sub(retained))?
        else {
            continue;
        };
        match &mut bounds {
            None => {
                let other = Point::parse(&point.label, MAX_BOUNDARY_BYTES - point.bytes())?
                    .expect("a previously parsed finite number");
                bounds = Some((point, other));
            }
            Some((min, _)) if point.exact.cmp(&min.exact).is_lt() => *min = point,
            Some((_, max)) if point.exact.cmp(&max.exact).is_gt() => *max = point,
            _ => {}
        }
    }
    Ok(bounds)
}

struct ExactBins {
    edges: Vec<Point>,
    constant: bool,
    retained: usize,
}

impl ExactBins {
    fn new(min: Point, max: Point, buckets: usize) -> Result<Self, String> {
        if min.exact.cmp(&max.exact).is_gt() {
            return Err("Invalid range: min is greater than max.".into());
        }
        // The DTO and count buffers coexist with the edge vector. Point::bytes
        // separately reserves each label/key and their final output copies.
        let fixed = (buckets + 1)
            .saturating_mul(size_of::<Point>() + size_of::<f64>())
            .saturating_add(buckets.saturating_mul(size_of::<HistogramBucket>()));
        let bounds_bytes = min.bytes() + max.bytes();
        if fixed.saturating_add(bounds_bytes) > MAX_BOUNDARY_BYTES {
            return Err(budget_error());
        }
        let axis = FloatBins::new(min.axis, max.axis, buckets)?;
        let constant = min.exact.cmp(&max.exact).is_eq();
        let mut edges = Vec::with_capacity(buckets + 1);
        let mut retained = fixed;
        for index in 0..=buckets {
            let approximate = axis.edges[index].to_string();
            let number = ExactNumber::parse(&approximate).expect("finite axis edge");
            let label = if index == buckets {
                max.label.as_str()
            } else if index == 0 || number.cmp(&min.exact).is_lt() {
                min.label.as_str()
            } else if number.cmp(&max.exact).is_gt() {
                max.label.as_str()
            } else {
                approximate.as_str()
            };
            let available = MAX_BOUNDARY_BYTES.saturating_sub(retained + bounds_bytes);
            let point = Point::parse(label, available)?.expect("finite exact boundary");
            retained += point.bytes();
            edges.push(point);
        }
        Ok(Self {
            edges,
            constant,
            retained,
        })
    }

    fn index(&self, point: &Point) -> Option<usize> {
        if point.exact.cmp(&self.edges[0].exact).is_lt()
            || point.exact.cmp(&self.edges.last()?.exact).is_gt()
        {
            return None;
        }
        if self.constant {
            return Some(0);
        }
        // Duplicate edges denote empty intervals, exactly as their labels do.
        Some(
            self.edges
                .partition_point(|edge| !edge.exact.cmp(&point.exact).is_gt())
                .saturating_sub(1)
                .min(self.edges.len() - 2),
        )
    }

    fn empty_buckets(&self) -> Vec<HistogramBucket> {
        (0..self.edges.len() - 1)
            .map(|index| HistogramBucket {
                start: self.edges[index].label.clone(),
                end: self.edges[index + 1].label.clone(),
                count: 0,
                flags: HistogramFlags::default(),
                end_inclusive: if self.constant {
                    index == 0
                } else {
                    index + 2 == self.edges.len()
                },
            })
            .collect()
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn histogram(
    column: &str,
    series: &Series,
    rows: &[usize],
    buckets: usize,
    range: Option<(&str, &str)>,
    flags: Option<&HashMap<usize, FlagEntry>>,
    job: &JobContext,
) -> Result<Histogram, String> {
    let bounds = match range {
        Some((min, max)) => {
            let min = Point::parse(min, MAX_BOUNDARY_BYTES)?
                .ok_or_else(|| format!("Invalid range bound: {min}"))?;
            let max = Point::parse(max, MAX_BOUNDARY_BYTES - min.bytes())?
                .ok_or_else(|| format!("Invalid range bound: {max}"))?;
            Some((min, max))
        }
        None => extents(series, rows, job)?,
    };
    let mut result = Histogram {
        column: column.into(),
        kind: "float".into(),
        buckets: Vec::new(),
        missing: 0,
        invalid: 0,
        outside: 0,
        full_range: None,
        auto_focused: false,
    };
    let Some((min, max)) = bounds else {
        result.buckets = super::empty_buckets(buckets);
        result.missing = rows.len() as u64;
        return Ok(result);
    };
    let bins = ExactBins::new(min, max, buckets)?;
    result.buckets = bins.empty_buckets();
    let available = MAX_BOUNDARY_BYTES.saturating_sub(bins.retained);
    // Auto range needs a first pass for two exact extrema. Count in this pass,
    // retaining no full-column vector of parsed numbers or copied source text.
    for (position, row) in rows.iter().enumerate() {
        if position.is_multiple_of(1024) {
            job.check_cancelled().map_err(|error| error.to_string())?;
        }
        let Some(point) = point_at(series, *row, available)? else {
            result.missing += 1;
            continue;
        };
        if let Some(index) = bins.index(&point) {
            let bucket = &mut result.buckets[index];
            bucket.count += 1;
            add_flag(
                &mut bucket.flags,
                flags.and_then(|entries| entries.get(row)),
            );
        } else {
            result.outside += 1;
        }
    }
    job.check_cancelled().map_err(|error| error.to_string())?;
    Ok(result)
}

/// Floating point determines axis spacing only. Decimal source values and
/// displayed boundaries never round through it when deciding membership.
struct FloatBins {
    edges: Vec<f64>,
}

impl FloatBins {
    fn new(min: f64, max: f64, buckets: usize) -> Result<Self, String> {
        let span = max - min;
        if !span.is_finite() {
            return Err("Invalid range: span is too wide.".into());
        }
        let edges: Vec<_> = (0..=buckets)
            .map(|index| {
                if index == buckets {
                    max
                } else {
                    min + span * (index as f64 / buckets as f64)
                }
            })
            .collect();
        if edges.iter().any(|edge| !edge.is_finite()) {
            return Err("Invalid range: span is too wide.".into());
        }
        Ok(Self { edges })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_source_keys_and_repeated_boundary_labels_reject_before_exceeding_the_budget() {
        let text = format!("0.{}1", "0".repeat(MAX_BOUNDARY_BYTES / 4));
        assert!(Point::parse(&text, MAX_BOUNDARY_BYTES).is_err());
        let text = format!("0.{}1", "0".repeat(4096));
        let point = || Point::parse(&text, MAX_BOUNDARY_BYTES).unwrap().unwrap();
        assert!(ExactBins::new(point(), point(), 4096).is_err());
    }
}
