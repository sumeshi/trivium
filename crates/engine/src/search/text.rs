use std::collections::HashMap;

use polars::prelude::Series;

use crate::value_utils::anyvalue_to_search_string;

pub fn build_searchable_text(
    row_count: usize,
    search_cols: &[String],
    column_series: &HashMap<&str, &Series>,
) -> Vec<String> {
    let mut searchable_text: Vec<String> = vec![String::new(); row_count];
    for col in search_cols {
        if let Some(series) = column_series.get(col.as_str()) {
            for row_idx in 0..row_count {
                if let Ok(value) = series.get(row_idx) {
                    if let Some(text) = anyvalue_to_search_string(&value) {
                        let lower = text.to_lowercase();
                        if lower.is_empty() {
                            continue;
                        }
                        let entry = &mut searchable_text[row_idx];
                        if entry.is_empty() {
                            entry.push_str(&lower);
                        } else {
                            entry.push(' ');
                            entry.push_str(&lower);
                        }
                    }
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
