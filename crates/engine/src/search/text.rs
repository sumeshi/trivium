use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use polars::prelude::{AnyValue, Series};

use crate::value_utils::anyvalue_to_search_string;

/// Test instrumentation: how many times a full searchable-text vector was
/// built in this process.
static SEARCHABLE_TEXT_BUILDS: AtomicU64 = AtomicU64::new(0);

pub fn searchable_text_build_count() -> u64 {
    SEARCHABLE_TEXT_BUILDS.load(Ordering::SeqCst)
}

/// Resets the test instrumentation counter.
pub fn reset_searchable_text_build_count() {
    SEARCHABLE_TEXT_BUILDS.store(0, Ordering::SeqCst);
}

/// Appends one cell's normalized text to a row's joined searchable text.
///
/// Values are lowercased and separated by one space; null and empty values are
/// skipped. This is shared with [`build_searchable_text`] so the batch
/// evaluation keeps the established search meaning.
pub fn push_searchable_value(target: &mut String, value: &AnyValue) {
    let Some(text) = anyvalue_to_search_string(value) else {
        return;
    };
    let lower = text.to_lowercase();
    if lower.is_empty() {
        return;
    }
    if !target.is_empty() {
        target.push(' ');
    }
    target.push_str(&lower);
}

pub fn build_searchable_text(
    row_count: usize,
    search_cols: &[String],
    column_series: &HashMap<&str, &Series>,
) -> Vec<String> {
    SEARCHABLE_TEXT_BUILDS.fetch_add(1, Ordering::SeqCst);
    let mut searchable_text: Vec<String> = vec![String::new(); row_count];
    for col in search_cols {
        if let Some(series) = column_series.get(col.as_str()) {
            // The index appends into the shared searchable_text row.
            #[allow(clippy::needless_range_loop)]
            for row_idx in 0..row_count {
                if let Ok(value) = series.get(row_idx) {
                    push_searchable_value(&mut searchable_text[row_idx], &value);
                }
            }
        }
    }
    searchable_text
}

pub fn ensure_searchable_text<'a>(
    storage: &'a mut Option<Vec<String>>,
    built_flag: &mut bool,
    row_count: usize,
    search_cols: &[String],
    column_series: &HashMap<&str, &Series>,
) -> &'a Vec<String> {
    if storage.is_none() {
        let built = build_searchable_text(row_count, search_cols, column_series);
        *storage = Some(built);
        *built_flag = true;
    }
    storage.as_ref().unwrap()
}
