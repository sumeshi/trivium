use std::collections::HashMap;
use std::path::Path;

use polars::prelude::*;
use serde_json::Value;

use crate::condition::{and_nodes, eval, search_to_node, Node};
use crate::project_io::is_internal_column;
use crate::search::{ensure_searchable_text, tokenize_search_query, SearchToken};
use crate::storage::{load_searchable_cache, save_searchable_cache};
use crate::value_utils::anyvalue_to_search_string;

pub struct SearchState {
    pub mask: Option<Vec<bool>>,
    pub searchable_text: Option<Vec<String>>,
    pub built: bool,
    pub search_cols: Vec<String>,
}

pub fn prepare_search(
    df: &DataFrame,
    project_dir: &Path,
    requested_columns: Option<&[String]>,
    column_names: &[String],
    search: Option<&str>,
    condition: Option<&Node>,
) -> SearchState {
    let search_cols: Vec<String> = requested_columns
        .map(|cols| cols.to_vec())
        .unwrap_or_else(|| column_names.to_vec());
    let row_count = df.height();
    let cached_search = match load_searchable_cache(project_dir) {
        Ok(cache) => cache,
        Err(err) => {
            eprintln!(
                "[cache] failed to load searchable cache for {:?}: {:?}",
                project_dir, err
            );
            None
        }
    };
    let mut searchable_text: Option<Vec<String>> = cached_search
        .filter(|cached| {
            cached.columns == search_cols
                && cached.text.len() == row_count
                && cached.text.iter().any(|s| !s.is_empty())
        })
        .map(|cached| cached.text);
    let mut searchable_text_built = false;

    let search_raw = search.map(str::trim).filter(|value| !value.is_empty());
    let has_terms = search_raw.is_some_and(|query| {
        tokenize_search_query(query).iter().any(|token| match token {
            SearchToken::Term { text, .. } | SearchToken::QuotedTerm { text, .. } => !text.is_empty(),
            _ => false,
        })
    });
    if has_terms {
        let column_series: HashMap<&str, &Series> =
            df.get_columns().iter().map(|series| (series.name(), series)).collect();
        let _ = ensure_searchable_text(
            &mut searchable_text,
            &mut searchable_text_built,
            row_count,
            &search_cols,
            &column_series,
        );
    }

    let search_node = if has_terms {
        search_raw.and_then(search_to_node)
    } else {
        None
    };
    let combined = match (search_node, condition.cloned()) {
        (Some(search_node), Some(condition)) => Some(and_nodes(search_node, condition)),
        (Some(search_node), None) => Some(search_node),
        (None, Some(condition)) => Some(condition),
        (None, None) => None,
    };
    let search_mask = combined.map(|node| {
        let joined = if has_terms {
            searchable_text.clone().unwrap_or_default()
        } else {
            vec![String::new(); row_count]
        };
        eval_frame(df, &node, &joined)
    });

    SearchState {
        mask: search_mask,
        searchable_text,
        built: searchable_text_built,
        search_cols,
    }
}

pub fn persist_search_cache(project_dir: &Path, state: &SearchState) {
    if state.built {
        if let Some(ref built) = state.searchable_text {
            if let Err(err) = save_searchable_cache(project_dir, &state.search_cols, built) {
                eprintln!(
                    "[cache] failed to persist searchable cache for {:?}: {:?}",
                    project_dir, err
                );
            }
        }
    }
}

fn eval_frame(df: &DataFrame, node: &Node, joined: &[String]) -> Vec<bool> {
    let mut mask = vec![false; df.height()];
    for row_idx in 0..df.height() {
        let mut row: HashMap<String, Value> = HashMap::new();
        for series in df.get_columns() {
            if is_internal_column(series.name()) {
                continue;
            }
            if let Ok(value) = series.get(row_idx) {
                if let Some(cell) = cell_value(&value) {
                    row.insert(series.name().to_string(), cell);
                }
            }
        }
        if let Some(text) = joined.get(row_idx) {
            row.insert(String::new(), Value::String(text.clone()));
        }
        mask[row_idx] = eval(node, &row);
    }
    mask
}

fn cell_value(value: &AnyValue) -> Option<Value> {
    match value {
        AnyValue::Null => None,
        AnyValue::Int8(v) => Some(Value::from(i64::from(*v))),
        AnyValue::Int16(v) => Some(Value::from(i64::from(*v))),
        AnyValue::Int32(v) => Some(Value::from(i64::from(*v))),
        AnyValue::Int64(v) => Some(Value::from(*v)),
        AnyValue::UInt8(v) => Some(Value::from(i64::from(*v))),
        AnyValue::UInt16(v) => Some(Value::from(i64::from(*v))),
        AnyValue::UInt32(v) => Some(Value::from(i64::from(*v))),
        AnyValue::UInt64(v) => i64::try_from(*v)
            .ok()
            .map(Value::from)
            .or_else(|| Some(Value::String(v.to_string()))),
        _ => anyvalue_to_search_string(value).map(Value::String),
    }
}
