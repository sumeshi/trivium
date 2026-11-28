use std::{borrow::Cow, cmp::Ordering};

use polars::prelude::*;

use crate::analysis::datetime::parse_datetime_nanos;
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::number::ExactNumber;
use crate::value_utils::anyvalue_text;

const SORT_RUN_ROWS: usize = 262_144;

enum SortKey<'a> {
    Int(i128),
    Number(Box<ExactNumber>),
    Datetime(i128),
    Missing,
    Text(Cow<'a, str>),
}

fn build_sort_key(series: &Series, row_index: usize) -> SortKey<'_> {
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
        AnyValue::Datetime(value, unit, _) | AnyValue::DatetimeOwned(value, unit, _) => {
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
        AnyValue::String(text) => {
            let text = text.trim();
            if let Some(key) = parsed_text_key(text) {
                return key;
            }
            // ASCII lowercase is byte-for-byte unchanged. Other strings keep
            // Rust's whole-string lowercase, including contextual Unicode.
            return SortKey::Text(
                if text
                    .bytes()
                    .all(|byte| byte.is_ascii() && !byte.is_ascii_uppercase())
                {
                    Cow::Borrowed(text)
                } else {
                    Cow::Owned(text.to_lowercase())
                },
            );
        }
        _ => {}
    }
    let Some(text) = anyvalue_text(&value) else {
        return SortKey::Missing;
    };
    let text = text.trim();
    parsed_text_key(text).unwrap_or_else(|| SortKey::Text(Cow::Owned(text.to_lowercase())))
}

// These are comparison keys only. Never rewrite the stored/displayed cell.
fn parsed_text_key(text: &str) -> Option<SortKey<'static>> {
    // Share ordered conditions' numeric grammar; punctuation inside source
    // text must not silently become a different number during sorting.
    if let Ok(integer) = text.parse::<i128>() {
        return Some(SortKey::Int(integer));
    }
    if let Some(number) = ExactNumber::parse(text) {
        return Some(SortKey::Number(Box::new(number)));
    }
    if let Some(nanos) = parse_datetime_nanos(text) {
        return Some(SortKey::Datetime(i128::from(nanos)));
    }
    None
}

fn compare_keys(a: &SortKey<'_>, b: &SortKey<'_>) -> Ordering {
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

fn key_rank(key: &SortKey<'_>) -> u8 {
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
    // Projection already resolves internal IDs case-insensitively. Resolve
    // the same column here. Standalone DataFrames can contain both `Value`
    // and `value`, so prefer an exact match before ignoring case.
    let Some(series) = df.column(sort_key).ok().or_else(|| {
        df.columns()
            .iter()
            .find(|series| series.name().eq_ignore_ascii_case(sort_key))
    }) else {
        return Ok(indices);
    };
    let series = series.as_materialized_series();
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
        let frame =
            DataFrame::new_infer_height(vec![Series::new("value".into(), values).into()]).unwrap();
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
    fn text_sort_keys_borrow_lowercase_ascii_and_keep_other_text_owned() {
        let text = "lowercase-command-".repeat(4096);
        let padded = format!("\t{text} \n");
        let series = Series::new(
            "value".into(),
            [padded.as_str(), "UPPER", "ΟΣ", "ǅelta", "İSTANBUL"],
        );
        let keys: Vec<_> = (0..series.len())
            .map(|row| build_sort_key(&series, row))
            .collect();
        let SortKey::Text(Cow::Borrowed(borrowed)) = &keys[0] else {
            panic!("ASCII key was copied");
        };
        assert_eq!(*borrowed, text);
        assert_eq!(
            borrowed.as_ptr(),
            series.str().unwrap().get(0).unwrap().trim().as_ptr()
        );
        for (key, expected) in keys[1..]
            .iter()
            .zip(["upper", "ος", "ǆelta", "i\u{307}stanbul"])
        {
            assert!(matches!(key, SortKey::Text(Cow::Owned(value)) if value == expected));
        }
        let owned_bytes: usize = keys
            .iter()
            .map(|key| match key {
                SortKey::Text(Cow::Owned(value)) => value.capacity(),
                _ => 0,
            })
            .sum();
        assert!(
            owned_bytes > 0 && owned_bytes < text.len(),
            "long ASCII key is not retained twice"
        );
        let typed = Series::new("value".into(), [true]);
        assert!(
            matches!(build_sort_key(&typed, 0), SortKey::Text(Cow::Owned(value)) if value == "true")
        );
    }

    #[test]
    fn borrowed_keys_keep_owned_lowercase_order_and_stable_unicode_ties() {
        let values = [
            Some("lower"),
            Some(" UPPER "),
            Some("upper"),
            Some("ΟΣ"),
            Some("ος"),
            Some("ǅelta"),
            Some("ǆelta"),
            Some("İSTANBUL"),
            Some("i\u{307}stanbul"),
            Some(""),
            None,
        ];
        for direction in ["asc", "desc"] {
            let mut expected: Vec<_> = (0..values.len()).collect();
            expected.sort_by(|a, b| {
                let order = values[*a]
                    .map(|value| value.trim().to_lowercase())
                    .cmp(&values[*b].map(|value| value.trim().to_lowercase()));
                if direction == "desc" {
                    order.reverse()
                } else {
                    order
                }
            });
            assert_eq!(sorted(&values, direction), expected);
        }
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
        let values = Series::new("value".into(), &[3_i64, 1, 2, 1])
            .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
            .unwrap();
        let frame = DataFrame::new_infer_height(vec![values.into()]).unwrap();
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
    fn case_insensitive_sort_lookup_prefers_an_exact_source_name() {
        let frame = DataFrame::new_infer_height(vec![
            Series::new("Value".into(), [2_i64, 1, 1]).into(),
            Series::new("value".into(), [1_i64, 2, 3]).into(),
        ])
        .unwrap();
        for (key, expected) in [
            ("value", vec![0, 1, 2]),
            ("Value", vec![1, 2, 0]),
            ("VALUE", vec![1, 2, 0]),
        ] {
            assert_eq!(
                sort_row_indices(
                    &frame,
                    vec![0, 1, 2],
                    Some(key),
                    Some("asc"),
                    &JobContext::none(),
                )
                .unwrap(),
                expected,
                "{key}"
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
