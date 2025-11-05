use std::collections::HashMap;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use tauri::State;
use uuid::Uuid;

use crate::{
    error::AppError,
    flags::{normalize_flag_value, severity_rank},
    ioc::load_ioc_entries,
    models::ProjectRow,
    project_io::read_project_dataframe,
    search::{
        build_search_mask_boolean, ensure_searchable_text, to_rpn, tokenize_search_query,
        SearchToken,
    },
    state::AppState,
    storage::load_flags,
    value_utils::anyvalue_to_search_string,
};

use super::{
    utils::{build_row_search_text, collect_row_record, ensure_column_text_cache},
    DEFAULT_PAGE_SIZE,
};

fn matches_flag_filter(current_flag: &str, filter: &str) -> bool {
    match filter {
        "all" => true,
        "none" => current_flag.is_empty(),
        "priority" => current_flag == "suspicious" || current_flag == "critical",
        "safe" | "suspicious" | "critical" => current_flag == filter,
        _ => true,
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryRowsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(rename = "sortKey", default)]
    pub sort_key: Option<String>,
    #[serde(rename = "sortDirection", default)]
    pub sort_direction: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QueryRowsResponse {
    pub rows: Vec<ProjectRow>,
    pub total_flagged: usize,
    pub total_rows: usize,
    pub total_filtered_rows: usize,
    pub offset: usize,
}

/// Streams project rows with filtering, sorting, IOC application, and pagination.
#[tauri::command]
pub fn query_project_rows(
    state: State<AppState>,
    payload: QueryRowsPayload,
) -> Result<QueryRowsResponse, String> {
    let meta = state
        .projects
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = state.projects.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()).into());
    }

    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| name != &"__rowid")
        .map(|name| name.to_string())
        .collect();

    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let offset = payload.offset.unwrap_or(0);
    let limit = payload.limit.unwrap_or(DEFAULT_PAGE_SIZE).max(1);

    let total_rows_before_flag_filter = df.height();

    let mut rows: Vec<ProjectRow> = Vec::with_capacity(limit);
    let mut total_flagged_after_ioc: usize = 0;

    let column_names: Vec<String> = columns.clone();
    let column_series: HashMap<&str, &Series> =
        df.get_columns().iter().map(|s| (s.name(), s)).collect();
    let column_series_lower: HashMap<String, &Series> = df
        .get_columns()
        .iter()
        .map(|s| (s.name().to_lowercase(), s))
        .collect();

    let search_cols: Vec<String> = payload
        .columns
        .as_ref()
        .cloned()
        .unwrap_or_else(|| column_names.clone());
    let row_count = df.height();
    let cached_search = {
        let guard = state.searchable_cache.lock();
        guard.get(&meta.id).cloned()
    };
    let mut searchable_text: Option<Vec<String>> = cached_search.and_then(|cached| {
        if cached.len() == row_count && cached.iter().any(|s| !s.is_empty()) {
            Some(cached)
        } else {
            None
        }
    });
    let mut searchable_text_built = false;
    let mut per_column_text: HashMap<String, Vec<String>> = HashMap::new();

    let mut search_mask: Option<Vec<bool>> = None;
    if let Some(search_str_raw) = payload.search.as_ref().map(|s| s.trim().to_string()) {
        if !search_str_raw.is_empty() {
            let tokens = tokenize_search_query(&search_str_raw);
            let mut terms: Vec<(Option<String>, String)> = Vec::new();
            let mut needed_cols: Vec<String> = Vec::new();
            for t in &tokens {
                match t {
                    SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } => {
                        let key = (col.clone(), text.clone());
                        if !text.is_empty() && !terms.contains(&key) {
                            terms.push(key);
                        }
                        if let Some(c) = col {
                            if !needed_cols.contains(c) {
                                needed_cols.push(c.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            for c in needed_cols {
                ensure_column_text_cache(
                    &c,
                    &column_series_lower,
                    &mut per_column_text,
                    df.height(),
                );
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
                let mask =
                    build_search_mask_boolean(&rpn, &terms, search_text, Some(&per_column_text));
                search_mask = Some(mask);
            }
        }
    }

    let mut user_flag_vec: Vec<String> = vec![String::new(); df.height()];
    for (idx, entry) in flags.iter() {
        if *idx < df.height() {
            user_flag_vec[*idx] = normalize_flag_value(&entry.flag);
        }
    }

    let mut ioc_flag_vec: Vec<String> =
        if let Some(cached) = state.ioc_flag_cache.lock().get(&meta.id).cloned() {
            if cached.len() == df.height() {
                cached
            } else {
                vec![String::new(); df.height()]
            }
        } else {
            vec![String::new(); df.height()]
        };
    let mut sorted_iocs = iocs.clone();
    sorted_iocs.sort_by_key(|e| std::cmp::Reverse(severity_rank(&normalize_flag_value(&e.flag))));
    let need_rebuild_ioc = ioc_flag_vec.iter().all(|s| s.is_empty());
    if need_rebuild_ioc {
        for ioc_entry in &sorted_iocs {
            let query = ioc_entry.query.trim();
            if query.is_empty() {
                continue;
            }
            let tokens = tokenize_search_query(query);
            let mut terms: Vec<(Option<String>, String)> = Vec::new();
            let mut needed_cols: Vec<String> = Vec::new();
            for t in &tokens {
                if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t {
                    let key = (col.clone(), text.clone());
                    if !text.is_empty() && !terms.contains(&key) {
                        terms.push(key);
                    }
                    if let Some(c) = col {
                        if !needed_cols.contains(c) {
                            needed_cols.push(c.clone());
                        }
                    }
                }
            }
            for c in needed_cols {
                ensure_column_text_cache(
                    &c,
                    &column_series_lower,
                    &mut per_column_text,
                    df.height(),
                );
            }
            if terms.is_empty() {
                continue;
            }
            let rpn = to_rpn(&tokens);
            let search_text = ensure_searchable_text(
                &mut searchable_text,
                &mut searchable_text_built,
                row_count,
                &search_cols,
                &column_series,
            );
            let mask = build_search_mask_boolean(&rpn, &terms, search_text, Some(&per_column_text));
            for i in 0..df.height() {
                if !ioc_flag_vec[i].is_empty() || !user_flag_vec[i].is_empty() {
                    continue;
                }
                if mask.get(i).copied().unwrap_or(false) {
                    ioc_flag_vec[i] = normalize_flag_value(&ioc_entry.flag);
                }
            }
        }
        state
            .ioc_flag_cache
            .lock()
            .insert(meta.id, ioc_flag_vec.clone());
    }

    let mut ordered_indices: Vec<usize> = (0..df.height()).collect();
    if let Some(sort_key) = &payload.sort_key {
        if let Ok(series) = df.column(sort_key) {
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

                let a_num = a_s.as_ref().and_then(|s| s.parse::<f64>().ok());
                let b_num = b_s.as_ref().and_then(|s| s.parse::<f64>().ok());

                if a_num.is_some() || b_num.is_some() {
                    let av = a_num.unwrap_or(f64::INFINITY);
                    let bv = b_num.unwrap_or(f64::INFINITY);
                    let mut ord = av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal);
                    if payload.sort_direction.as_deref() == Some("desc") {
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
                if payload.sort_direction.as_deref() == Some("desc") {
                    ord = ord.reverse();
                }
                ord
            });
        }
    }

    let mut final_flag_vec: Vec<String> = Vec::with_capacity(df.height());
    final_flag_vec.extend((0..df.height()).map(|i| {
        if !user_flag_vec[i].is_empty() {
            user_flag_vec[i].clone()
        } else {
            ioc_flag_vec[i].clone()
        }
    }));

    let mut filtered_indices: Vec<usize> = Vec::with_capacity(df.height());
    for &idx in &ordered_indices {
        let ff = &final_flag_vec[idx];
        let flag_ok = if let Some(filter) = &payload.flag_filter {
            matches_flag_filter(ff, filter)
        } else {
            true
        };
        if !flag_ok {
            continue;
        }
        if let Some(mask) = &search_mask {
            if !mask[idx] {
                continue;
            }
        }
        filtered_indices.push(idx);
    }
    for &idx in &filtered_indices {
        if !final_flag_vec[idx].trim().is_empty() {
            total_flagged_after_ioc += 1;
        }
    }
    let total_filtered_rows = filtered_indices.len();

    for &row_idx in filtered_indices.iter().skip(offset).take(limit) {
        let record = collect_row_record(&df, &column_names, row_idx);
        let user_memo = flags
            .get(&row_idx)
            .and_then(|e| e.memo.clone())
            .unwrap_or_default();
        let mut final_memo = user_memo;
        if !iocs.is_empty() && final_flag_vec[row_idx] == ioc_flag_vec[row_idx] {
            let mut memo_tags: Vec<String> = Vec::new();
            for ioc_entry in &iocs {
                let query = ioc_entry.query.trim();
                if query.is_empty() {
                    continue;
                }
                let tokens = tokenize_search_query(query);
                let mut terms: Vec<(Option<String>, String)> = Vec::new();
                let mut needed_cols: Vec<String> = Vec::new();
                for t in &tokens {
                    if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } =
                        t
                    {
                        let key = (col.clone(), text.clone());
                        if !text.is_empty() && !terms.contains(&key) {
                            terms.push(key);
                        }
                        if let Some(c) = col {
                            if !needed_cols.contains(c) {
                                needed_cols.push(c.clone());
                            }
                        }
                    }
                }
                if terms.is_empty() {
                    continue;
                }
                let rpn = to_rpn(&tokens);
                for c in needed_cols {
                    ensure_column_text_cache(
                        &c,
                        &column_series_lower,
                        &mut per_column_text,
                        df.height(),
                    );
                }
                let (row_search_text, single_per_col) =
                    build_row_search_text(&column_names, &column_series, row_idx);
                let single_row = vec![row_search_text];
                let single_mask =
                    build_search_mask_boolean(&rpn, &terms, &single_row, Some(&single_per_col));
                if single_mask.get(0).copied().unwrap_or(false) {
                    let tag = ioc_entry.tag.trim();
                    if !tag.is_empty() {
                        let token = format!("[{}]", tag);
                        if !memo_tags.contains(&token) {
                            memo_tags.push(token);
                        }
                    }
                }
            }
            for tag in memo_tags {
                if !final_memo.contains(&tag) {
                    if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                        final_memo.push(' ');
                    }
                    final_memo.push_str(&tag);
                }
            }
        }
        rows.push(ProjectRow {
            row_index: row_idx,
            data: record,
            flag: final_flag_vec[row_idx].clone(),
            memo: if final_memo.is_empty() {
                None
            } else {
                Some(final_memo)
            },
        });
    }

    if searchable_text_built {
        if let Some(ref built) = searchable_text {
            state.searchable_cache.lock().insert(meta.id, built.clone());
        }
    }

    Ok(QueryRowsResponse {
        rows,
        total_flagged: total_flagged_after_ioc,
        total_rows: total_rows_before_flag_filter,
        total_filtered_rows,
        offset,
    })
}
