use std::collections::HashMap;

use anyhow::Error as AnyhowError;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    error::AppError,
    flags::normalize_flag_value,
    ioc::{cached_ioc_outcomes, load_ioc_entries},
    models::{FlagEntry, ProjectRow},
    project_io::read_project_dataframe,
    search::{
        build_search_mask_boolean, ensure_searchable_text, to_rpn, tokenize_search_query,
        SearchToken,
    },
    storage::{load_flags, load_searchable_cache, save_searchable_cache},
    store::ProjectsStore,
    value_utils::anyvalue_to_search_string,
};

use super::util::{collect_row_record_from_series, ensure_column_text_cache, DEFAULT_PAGE_SIZE};

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
pub fn query_project_rows(
    store: &ProjectsStore,
    payload: QueryRowsPayload,
) -> Result<QueryRowsResponse, AppError> {
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
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
    let cached_search = match load_searchable_cache(&project_dir) {
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
    if let Some(search_str_raw) = payload
        .search
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        let tokens = tokenize_search_query(&search_str_raw);
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
            ensure_column_text_cache(
                &column,
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

    let mut user_flag_vec: Vec<String> = vec![String::new(); df.height()];
    for (idx, entry) in flags.iter() {
        if *idx < df.height() {
            user_flag_vec[*idx] = normalize_flag_value(&entry.flag);
        }
    }

    let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);

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
            ioc_outcomes[i].flag.clone()
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

    let selected_indices: Vec<usize> = filtered_indices
        .iter()
        .skip(offset)
        .take(limit)
        .copied()
        .collect();
    let take_idx =
        UInt32Chunked::from_iter_values("take_idx", selected_indices.iter().map(|&idx| idx as u32));
    let taken_df = df
        .take(&take_idx)
        .map_err(|err| AppError::from(AnyhowError::from(err)))?;
    let taken_series_map: HashMap<&str, &Series> = taken_df
        .get_columns()
        .iter()
        .map(|series| (series.name(), series))
        .collect();
    let page_flags: HashMap<usize, FlagEntry> = selected_indices
        .iter()
        .filter_map(|idx| flags.get(idx).cloned().map(|entry| (*idx, entry)))
        .collect();

    for (position, &row_idx) in selected_indices.iter().enumerate() {
        let record = collect_row_record_from_series(&taken_series_map, &column_names, position);
        let user_memo = page_flags
            .get(&row_idx)
            .and_then(|e| e.memo.clone())
            .unwrap_or_default();
        let mut final_memo = user_memo;
        // Pre-B2 behaviour preserved: tags surface only when the IOC flag is
        // the active one for this row (no user flag). The tags themselves come
        // from the cached evaluation instead of a per-row re-evaluation.
        if !iocs.is_empty() && final_flag_vec[row_idx] == ioc_outcomes[row_idx].flag {
            for tag in ioc_outcomes[row_idx].tags.clone() {
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
            if let Err(err) = save_searchable_cache(&project_dir, &search_cols, built) {
                eprintln!(
                    "[cache] failed to persist searchable cache for {:?}: {:?}",
                    project_dir, err
                );
            }
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
