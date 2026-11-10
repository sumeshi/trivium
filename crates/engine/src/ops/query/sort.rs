use polars::prelude::*;

use crate::value_utils::anyvalue_to_search_string;

/// Precomputed sort key for one row (P1). Comparison semantics are identical
/// to the previous per-comparison conversion: integers compare exactly (F26),
/// mixed int/float and number-vs-text comparisons go through `f64` with text
/// ranked as +infinity, and nulls sort after numbers but before text.
enum SortKey {
    Int(i64),
    Float(f64),
    Text(String),
    Missing,
}

fn build_sort_key(series: &Series, row_index: usize) -> SortKey {
    let normalized = series
        .get(row_index)
        .ok()
        .and_then(|v| anyvalue_to_search_string(&v))
        .map(|s| {
            s.trim()
                .replace(',', "")
                .replace('\u{00A0}', "")
                .to_lowercase()
        });
    let Some(text) = normalized else {
        return SortKey::Missing;
    };
    if let Ok(value) = text.parse::<i64>() {
        return SortKey::Int(value);
    }
    if let Ok(value) = text.parse::<f64>() {
        return SortKey::Float(value);
    }
    SortKey::Text(text)
}

fn numeric_rank(key: &SortKey) -> f64 {
    match key {
        SortKey::Int(value) => *value as f64,
        SortKey::Float(value) => *value,
        // Number-vs-text pairs previously ranked text as +infinity.
        SortKey::Text(_) | SortKey::Missing => f64::INFINITY,
    }
}

fn compare_keys(a: &SortKey, b: &SortKey) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (SortKey::Missing, SortKey::Missing) => Equal,
        // Text path: null sorts before text.
        (SortKey::Missing, SortKey::Text(_)) => Less,
        (SortKey::Text(_), SortKey::Missing) => Greater,
        // Numeric path: null sorts after every number (previous +infinity rule).
        (SortKey::Missing, _) => Greater,
        (_, SortKey::Missing) => Less,
        (SortKey::Int(av), SortKey::Int(bv)) => av.cmp(bv),
        (SortKey::Text(av), SortKey::Text(bv)) => av.cmp(bv),
        _ => numeric_rank(a)
            .partial_cmp(&numeric_rank(b))
            .unwrap_or(Equal),
    }
}

/// Row order before filtering. Unchanged when `sort_key` is absent or unknown.
pub fn ordered_row_indices(
    df: &DataFrame,
    sort_key: Option<&str>,
    sort_direction: Option<&str>,
) -> Vec<usize> {
    let mut ordered_indices: Vec<usize> = (0..df.height()).collect();
    let Some(sort_key) = sort_key else {
        return ordered_indices;
    };
    let Ok(series) = df.column(sort_key) else {
        return ordered_indices;
    };

    // P1: convert each row once into a sort key; comparisons no longer
    // stringify and re-parse the cell value.
    let keys: Vec<SortKey> = (0..df.height())
        .map(|row_index| build_sort_key(series, row_index))
        .collect();

    ordered_indices.sort_by(|a, b| {
        let mut ord = compare_keys(&keys[*a], &keys[*b]);
        if sort_direction == Some("desc") {
            ord = ord.reverse();
        }
        ord
    });
    ordered_indices
}
