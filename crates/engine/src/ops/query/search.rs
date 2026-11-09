use std::collections::HashMap;
use std::path::Path;

use polars::prelude::*;

use crate::search::{
    build_search_mask_boolean, ensure_searchable_text, to_rpn, tokenize_search_query, SearchToken,
};
use crate::storage::{load_searchable_cache, save_searchable_cache};

use super::super::util::ensure_column_text_cache;

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
) -> SearchState {
    let column_series: HashMap<&str, &Series> =
        df.get_columns().iter().map(|s| (s.name(), s)).collect();
    let column_series_lower: HashMap<String, &Series> = df
        .get_columns()
        .iter()
        .map(|s| (s.name().to_lowercase(), s))
        .collect();

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
    let mut per_column_text: HashMap<String, Vec<String>> = HashMap::new();

    let mut search_mask: Option<Vec<bool>> = None;
    if let Some(search_str_raw) = search.map(str::trim).filter(|s| !s.is_empty()) {
        let tokens = tokenize_search_query(search_str_raw);
        let mut terms: Vec<(Option<String>, String)> = Vec::new();
        let mut needed_cols: Vec<String> = Vec::new();
        for token in &tokens {
            if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = token {
                let key = (col.clone(), text.clone());
                if !text.is_empty() && !terms.contains(&key) {
                    terms.push(key);
                }
                if let Some(column_name) = col {
                    if !needed_cols.contains(column_name) {
                        needed_cols.push(column_name.clone());
                    }
                }
            }
        }
        for column in needed_cols {
            ensure_column_text_cache(&column, &column_series_lower, &mut per_column_text, df.height());
        }
        if !terms.is_empty() {
            let rpn = to_rpn(&tokens);
            let search_text = ensure_searchable_text(
                &mut searchable_text,
                &mut searchable_text_built,
                row_count,
                &search_cols,
                &column_series,
            );
            let mask = build_search_mask_boolean(&rpn, &terms, search_text, Some(&per_column_text));
            search_mask = Some(mask);
        }
    }

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
