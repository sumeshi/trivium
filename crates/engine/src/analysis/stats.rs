use std::collections::HashMap;

use polars::prelude::{AnyValue, Series};
use serde::Serialize;

use crate::analysis::infer::{dtype_kind, KindFlags};
use crate::value_utils::anyvalue_to_search_string;

const TOP_VALUES: usize = 20;
/// The rare list is bounded too: the UI must never receive every unique value.
const RARE_VALUES: usize = 20;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ValueCount {
    pub value: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ColumnStats {
    pub column: String,
    pub kind: String,
    pub missing: u64,
    pub unique: u64,
    pub top: Vec<ValueCount>,
    pub rare: Vec<ValueCount>,
}

/// One pass over `series`: missing count, distinct values, and the column kind.
pub fn summarize_series(column: &str, series: &Series, rare_max: u64) -> ColumnStats {
    let hinted = dtype_kind(series.dtype());
    let mut flags = KindFlags::default();
    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut missing = 0u64;

    for idx in 0..series.len() {
        let Ok(value) = series.get(idx) else {
            missing += 1;
            continue;
        };
        let Some(text) = present_text(&value) else {
            missing += 1;
            continue;
        };
        if hinted.is_none() {
            flags.observe(&value);
        }
        *counts.entry(text).or_default() += 1;
    }

    let kind = match hinted {
        Some(kind) => kind,
        None => flags.finish(),
    };
    let ranked = rank(counts);
    let unique = ranked.len() as u64;
    let top = ranked.iter().take(TOP_VALUES).cloned().collect();
    let rare = ranked
        .into_iter()
        .filter(|item| item.count <= rare_max)
        .take(RARE_VALUES)
        .collect();

    ColumnStats {
        column: column.to_string(),
        kind: kind.as_str().to_string(),
        missing,
        unique,
        top,
        rare,
    }
}

fn present_text(value: &AnyValue) -> Option<String> {
    if matches!(value, AnyValue::Null) {
        return None;
    }
    anyvalue_to_search_string(value).filter(|text| !text.is_empty())
}

fn rank(counts: HashMap<String, u64>) -> Vec<ValueCount> {
    let mut ranked: Vec<ValueCount> = counts
        .into_iter()
        .map(|(value, count)| ValueCount { value, count })
        .collect();
    ranked.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.value.cmp(&right.value))
    });
    ranked
}
