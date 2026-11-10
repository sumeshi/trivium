use polars::prelude::*;

use crate::value_utils::anyvalue_to_search_string;

/// Numeric sort keys. Integers compare exactly (F26); only mixed int/float
/// comparisons go through `f64`, matching the previous behavior.
enum NumericKey {
    Int(i64),
    Float(f64),
}

fn parse_numeric(text: &str) -> Option<NumericKey> {
    if let Ok(value) = text.parse::<i64>() {
        return Some(NumericKey::Int(value));
    }
    text.parse::<f64>().ok().map(NumericKey::Float)
}

fn numeric_as_f64(key: &NumericKey) -> f64 {
    match key {
        NumericKey::Int(value) => *value as f64,
        NumericKey::Float(value) => *value,
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
    ordered_indices.sort_by(|a, b| {
        let a_s = series
            .get(*a)
            .ok()
            .and_then(|v| anyvalue_to_search_string(&v))
            .map(|s| s.trim().replace(',', "").replace('\u{00A0}', ""));
        let b_s = series
            .get(*b)
            .ok()
            .and_then(|v| anyvalue_to_search_string(&v))
            .map(|s| s.trim().replace(',', "").replace('\u{00A0}', ""));

        let a_num = a_s.as_ref().and_then(|s| parse_numeric(s));
        let b_num = b_s.as_ref().and_then(|s| parse_numeric(s));

        if a_num.is_some() || b_num.is_some() {
            let mut ord = match (&a_num, &b_num) {
                (Some(NumericKey::Int(av)), Some(NumericKey::Int(bv))) => av.cmp(bv),
                (Some(av), Some(bv)) => numeric_as_f64(av)
                    .partial_cmp(&numeric_as_f64(bv))
                    .unwrap_or(std::cmp::Ordering::Equal),
                // Missing values sort as +infinity (after every number, ascending).
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            };
            if sort_direction == Some("desc") {
                ord = ord.reverse();
            }
            return ord;
        }

        let mut ord = match (&a_s, &b_s) {
            (Some(a_str), Some(b_str)) => a_str.to_lowercase().cmp(&b_str.to_lowercase()),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        };
        if sort_direction == Some("desc") {
            ord = ord.reverse();
        }
        ord
    });
    ordered_indices
}
