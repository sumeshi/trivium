use polars::prelude::*;

use crate::value_utils::anyvalue_to_search_string;

/// Precomputed sort key for one row (P1). Integers compare exactly through
/// i128. Values outside i128 and decimal spellings are text keys, so a long
/// integer is never rounded into another value's key. Text still ranks after
/// numbers, and nulls sort after numbers but before text.
enum SortKey {
    Int(i128),
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
                .replace([',', '\u{00A0}'], "")
                .to_lowercase()
        });
    let Some(text) = normalized else {
        return SortKey::Missing;
    };
    if let Ok(value) = text.parse::<i128>() {
        return SortKey::Int(value);
    }
    SortKey::Text(text)
}

fn compare_keys(a: &SortKey, b: &SortKey) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (SortKey::Missing, SortKey::Missing) => Equal,
        // Text path: null sorts before text.
        (SortKey::Missing, SortKey::Text(_)) => Less,
        (SortKey::Text(_), SortKey::Missing) => Greater,
        // Numeric path: null sorts after every number.
        (SortKey::Missing, _) => Greater,
        (_, SortKey::Missing) => Less,
        (SortKey::Int(av), SortKey::Int(bv)) => av.cmp(bv),
        (SortKey::Text(av), SortKey::Text(bv)) => av.cmp(bv),
        // Number-vs-text pairs keep text ranked as +infinity.
        (SortKey::Int(_), SortKey::Text(_)) => Less,
        (SortKey::Text(_), SortKey::Int(_)) => Greater,
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
