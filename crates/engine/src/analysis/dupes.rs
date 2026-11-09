use std::collections::HashMap;

use polars::prelude::{AnyValue, Series};

use crate::value_utils::anyvalue_to_search_string;

/// Row indexes whose key appears more than once (`duplicates`) or exactly once.
/// A key that appears three times contributes three indexes.
pub fn matching_row_indices(columns: &[&Series], duplicates: bool) -> Vec<usize> {
    let height = columns.first().map(|series| series.len()).unwrap_or(0);
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for idx in 0..height {
        groups.entry(row_key(columns, idx)).or_default().push(idx);
    }
    let mut matched = Vec::new();
    for rows in groups.into_values() {
        let keep = if duplicates {
            rows.len() > 1
        } else {
            rows.len() == 1
        };
        if keep {
            matched.extend(rows);
        }
    }
    matched.sort_unstable();
    matched
}

fn row_key(columns: &[&Series], idx: usize) -> String {
    let mut key = String::new();
    for series in columns {
        let text = cell_text(series, idx);
        key.push_str(&text.len().to_string());
        key.push(':');
        key.push_str(&text);
        key.push('\u{1e}');
    }
    key
}

fn cell_text(series: &Series, idx: usize) -> String {
    let Ok(value) = series.get(idx) else {
        return String::new();
    };
    if matches!(value, AnyValue::Null) {
        return String::new();
    }
    anyvalue_to_search_string(&value).unwrap_or_default()
}
