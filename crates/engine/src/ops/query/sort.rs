use std::cmp::Ordering;

use polars::prelude::*;

use crate::analysis::infer::parse_datetime_nanos;
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::number::ExactNumber;
use crate::value_utils::anyvalue_to_search_string;

const SORT_RUN_ROWS: usize = 262_144;

enum SortKey {
    Int(i128),
    Number(Box<ExactNumber>),
    Datetime(i128),
    Missing,
    Text(String),
}

fn build_sort_key(series: &Series, row_index: usize) -> SortKey {
    let Ok(value) = series.get(row_index) else {
        return SortKey::Missing;
    };
    // Native temporal values already have a unit. Scaling through i128 keeps
    // nanoseconds intact without parsing a display string or guessing epochs.
    match value {
        AnyValue::Int8(value) => return SortKey::Int(i128::from(value)),
        AnyValue::Int16(value) => return SortKey::Int(i128::from(value)),
        AnyValue::Int32(value) => return SortKey::Int(i128::from(value)),
        AnyValue::Int64(value) => return SortKey::Int(i128::from(value)),
        AnyValue::UInt8(value) => return SortKey::Int(i128::from(value)),
        AnyValue::UInt16(value) => return SortKey::Int(i128::from(value)),
        AnyValue::UInt32(value) => return SortKey::Int(i128::from(value)),
        AnyValue::UInt64(value) => return SortKey::Int(i128::from(value)),
        AnyValue::Datetime(value, unit, _) => {
            let scale = match unit {
                TimeUnit::Nanoseconds => 1,
                TimeUnit::Microseconds => 1_000,
                TimeUnit::Milliseconds => 1_000_000,
            };
            return SortKey::Datetime(i128::from(value) * scale);
        }
        AnyValue::Date(days) => {
            return SortKey::Datetime(i128::from(days) * 86_400 * 1_000_000_000);
        }
        AnyValue::Time(nanos) => return SortKey::Datetime(i128::from(nanos)),
        _ => {}
    }
    let Some(text) = anyvalue_to_search_string(&value) else {
        return SortKey::Missing;
    };
    // These are comparison keys only. Never rewrite the stored/displayed cell.
    let text = text.trim().replace([',', '\u{00A0}'], "");
    if let Ok(integer) = text.parse::<i128>() {
        return SortKey::Int(integer);
    }
    if let Some(number) = ExactNumber::parse(&text) {
        return SortKey::Number(Box::new(number));
    }
    if let Some(nanos) = parse_datetime_nanos(&text) {
        return SortKey::Datetime(i128::from(nanos));
    }
    SortKey::Text(text.to_lowercase())
}

fn compare_keys(a: &SortKey, b: &SortKey) -> Ordering {
    match (a, b) {
        (SortKey::Int(a), SortKey::Int(b)) => a.cmp(b),
        (SortKey::Int(a), SortKey::Number(b)) => b.cmp_integer(*a).reverse(),
        (SortKey::Number(a), SortKey::Int(b)) => a.cmp_integer(*b),
        (SortKey::Number(a), SortKey::Number(b)) => a.cmp(b),
        (SortKey::Datetime(a), SortKey::Datetime(b)) => a.cmp(b),
        (SortKey::Text(a), SortKey::Text(b)) => a.cmp(b),
        (SortKey::Missing, SortKey::Missing) => Ordering::Equal,
        // Preserve number < null < text for mixed columns. Parsed datetimes
        // have their own typed range between numbers and nulls; invalid dates
        // stay text instead of silently becoming a timestamp.
        _ => key_rank(a).cmp(&key_rank(b)),
    }
}

fn key_rank(key: &SortKey) -> u8 {
    match key {
        SortKey::Int(_) | SortKey::Number(_) => 0,
        SortKey::Datetime(_) => 1,
        SortKey::Missing => 2,
        SortKey::Text(_) => 3,
    }
}

/// Sort only the selected original row ids, retaining source order for ties.
pub fn sort_row_indices(
    df: &DataFrame,
    mut indices: Vec<usize>,
    sort_key: Option<&str>,
    sort_direction: Option<&str>,
    job: &JobContext,
) -> Result<Vec<usize>, AppError> {
    job.check_cancelled()?;
    let Some(sort_key) = sort_key else {
        return Ok(indices);
    };
    let Ok(series) = df.column(sort_key) else {
        return Ok(indices);
    };
    // Query targets already arrive in source order. Normalize only an unusual
    // caller-provided subset, then stable value comparisons retain that order
    // without making every equal value a distinct key.
    if !indices.is_sorted() {
        let source_order = cancellable_order(
            indices.len(),
            |a, b| indices[a].cmp(&indices[b]),
            || job.check_cancelled(),
        )?;
        indices = source_order
            .into_iter()
            .map(|position| indices[position])
            .collect();
    }
    let mut keys = Vec::with_capacity(indices.len());
    for (position, &row) in indices.iter().enumerate() {
        if position.is_multiple_of(1024) {
            job.check_cancelled()?;
        }
        keys.push(build_sort_key(series, row));
    }
    let descending = sort_direction == Some("desc");
    let compare = |a: usize, b: usize| {
        let order = compare_keys(&keys[a], &keys[b]);
        if descending {
            order.reverse()
        } else {
            order
        }
    };
    let order = cancellable_order(indices.len(), compare, || job.check_cancelled())?;
    Ok(order
        .into_iter()
        .map(|position| indices[position])
        .collect())
}

/// The standard sort cannot return a cancellation error from its comparator.
/// Bound that work to small runs, then merge with cooperative checkpoints.
fn cancellable_order(
    len: usize,
    compare: impl Fn(usize, usize) -> Ordering,
    mut checkpoint: impl FnMut() -> Result<(), AppError>,
) -> Result<Vec<usize>, AppError> {
    checkpoint()?;
    let mut order: Vec<usize> = (0..len).collect();
    // Common source-ordered columns should keep the standard sort's O(n)
    // best case rather than doing a sequence of unnecessary merge passes.
    let mut sorted = true;
    let mut reversed = true;
    for index in 1..len {
        if index.is_multiple_of(1024) {
            checkpoint()?;
        }
        match compare(index - 1, index) {
            Ordering::Less => reversed = false,
            Ordering::Greater => sorted = false,
            Ordering::Equal => {}
        }
        if !sorted && !reversed {
            break;
        }
    }
    if sorted {
        checkpoint()?;
        return Ok(order);
    }
    if reversed {
        order.reverse();
        // Reverse the value order without reversing rows that share a key.
        let mut start = 0;
        for end in 1..=len {
            if end.is_multiple_of(1024) {
                checkpoint()?;
            }
            if end == len || compare(order[start], order[end]) != Ordering::Equal {
                order[start..end].reverse();
                start = end;
            }
        }
        checkpoint()?;
        return Ok(order);
    }
    for run in order.chunks_mut(SORT_RUN_ROWS) {
        checkpoint()?;
        run.sort_by(|a, b| compare(*a, *b));
    }
    let mut width = SORT_RUN_ROWS;
    if width >= len {
        checkpoint()?;
        return Ok(order);
    }
    let mut merged = vec![0; len];
    while width < len {
        for start in (0..len).step_by(width.saturating_mul(2)) {
            checkpoint()?;
            let middle = start.saturating_add(width).min(len);
            let end = middle.saturating_add(width).min(len);
            let (mut left, mut right) = (start, middle);
            for (offset, output) in merged[start..end].iter_mut().enumerate() {
                if (start + offset).is_multiple_of(1024) {
                    checkpoint()?;
                }
                if left < middle && (right == end || !compare(order[left], order[right]).is_gt()) {
                    *output = order[left];
                    left += 1;
                } else {
                    *output = order[right];
                    right += 1;
                }
            }
        }
        std::mem::swap(&mut order, &mut merged);
        width = width.saturating_mul(2);
    }
    checkpoint()?;
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted(values: &[Option<&str>], direction: &str) -> Vec<usize> {
        let frame = DataFrame::new(vec![Series::new("value", values)]).unwrap();
        sort_row_indices(
            &frame,
            (0..values.len()).collect(),
            Some("value"),
            Some(direction),
            &JobContext::none(),
        )
        .unwrap()
    }

    #[test]
    fn datetime_offsets_formats_and_nanoseconds_compare_as_instants() {
        let values = [
            Some("2026-11-14T01:00:00Z"),
            Some("2026-11-14T09:00:00+09:00"),
            Some("2026/11/14 00:00:00.000000001"),
            Some("2026-11-14T00:00:00Z"),
            Some("2026-11-13T19:00:00-05:00"),
        ];
        assert_eq!(sorted(&values, "asc"), [1, 3, 4, 2, 0]);
        assert_eq!(sorted(&values, "desc"), [0, 2, 1, 3, 4]);
    }

    #[test]
    fn decimals_and_huge_integers_never_round_through_float() {
        let values = [
            Some("10.5"),
            Some("2.5"),
            Some("9007199254740993.000000000000000001"),
            Some("9007199254740993.000000000000000000"),
            Some("170141183460469231731687303715884105729"),
            Some("170141183460469231731687303715884105728"),
            Some("2.50e0"),
            Some("-10.5"),
            Some("-2.5"),
            Some("1e1000000"),
            Some("9e999999"),
        ];
        assert_eq!(sorted(&values, "asc"), [7, 8, 1, 6, 0, 3, 2, 5, 4, 10, 9]);
        assert_eq!(sorted(&values, "desc"), [9, 10, 4, 5, 2, 3, 0, 1, 6, 8, 7]);
    }

    #[test]
    fn mixed_values_keep_text_missing_and_stable_ties_deterministic() {
        let values = [
            Some("zebra"),
            None,
            Some("10"),
            Some("2"),
            Some("ALPHA"),
            Some("alpha"),
            Some("2026-02-30"),
            Some("2026-01-01"),
        ];
        assert_eq!(sorted(&values, "asc"), [3, 2, 7, 1, 6, 4, 5, 0]);
        assert_eq!(sorted(&values, "desc"), [0, 4, 5, 6, 1, 7, 2, 3]);
    }

    #[test]
    fn native_datetime_units_and_selected_indices_keep_source_ties() {
        let values = Series::new("value", &[3_i64, 1, 2, 1])
            .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
            .unwrap();
        let frame = DataFrame::new(vec![values]).unwrap();
        let selected = vec![3, 0, 1];
        assert_eq!(
            sort_row_indices(
                &frame,
                selected.clone(),
                Some("value"),
                Some("asc"),
                &JobContext::none(),
            )
            .unwrap(),
            [1, 3, 0]
        );
        assert_eq!(
            sort_row_indices(
                &frame,
                selected.clone(),
                Some("value"),
                Some("desc"),
                &JobContext::none(),
            )
            .unwrap(),
            [0, 1, 3]
        );
        for key in [None, Some("unknown")] {
            assert_eq!(
                sort_row_indices(&frame, selected.clone(), key, None, &JobContext::none()).unwrap(),
                selected
            );
        }
    }

    #[test]
    fn merge_passes_match_standard_sort_and_observe_cancellation() {
        let len = SORT_RUN_ROWS * 2 + 3;
        let compare = |a: usize, b: usize| ((a * 7919) % 193).cmp(&((b * 7919) % 193));
        let mut expected: Vec<_> = (0..len).collect();
        expected.sort_by(|a, b| compare(*a, *b));
        assert_eq!(
            cancellable_order(len, compare, || Ok(())).unwrap(),
            expected
        );

        let mut checks = 0;
        let error = cancellable_order(len, compare, || {
            checks += 1;
            // Stop in a merge pass, after all bounded runs have been sorted.
            if checks == 12 {
                Err(crate::jobs::cancelled_error())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "Cancelled.");
        assert_eq!(checks, 12);
    }

    #[test]
    fn reverse_monotonic_values_keep_equal_rows_in_source_order() {
        let len = 20_003;
        let compare = |a: usize, b: usize| (b / 7).cmp(&(a / 7));
        let mut expected: Vec<_> = (0..len).collect();
        expected.sort_by(|a, b| compare(*a, *b));
        assert_eq!(
            cancellable_order(len, compare, || Ok(())).unwrap(),
            expected
        );
    }
}
