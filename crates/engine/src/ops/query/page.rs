use std::collections::HashMap;

use anyhow::Error as AnyhowError;
use polars::prelude::*;

use crate::error::AppError;
use crate::ioc::IocRowOutcome;
use crate::models::{FlagEntry, ProjectRow};

use super::ioc::append_ioc_tags;
use super::super::util::collect_row_record_from_series;

pub fn matches_flag_filter(current_flag: &str, filter: &str) -> bool {
    match filter {
        "all" => true,
        "none" => current_flag.is_empty(),
        "priority" => current_flag == "suspicious" || current_flag == "critical",
        "safe" | "suspicious" | "critical" => current_flag == filter,
        _ => true,
    }
}

pub fn filtered_indices(
    ordered_indices: &[usize],
    final_flag_vec: &[String],
    bookmarked: &[bool],
    bookmarked_only: Option<bool>,
    flag_filter: Option<&str>,
    search_mask: Option<&[bool]>,
) -> (Vec<usize>, usize) {
    let mut filtered_indices: Vec<usize> = Vec::with_capacity(ordered_indices.len());
    for &idx in ordered_indices {
        let ff = &final_flag_vec[idx];
        let flag_ok = if let Some(filter) = flag_filter {
            matches_flag_filter(ff, filter)
        } else {
            true
        };
        if !flag_ok {
            continue;
        }
        if bookmarked_only == Some(true) && !bookmarked.get(idx).copied().unwrap_or(false) {
            continue;
        }
        if let Some(mask) = search_mask {
            if !mask[idx] {
                continue;
            }
        }
        filtered_indices.push(idx);
    }
    let total_flagged = filtered_indices
        .iter()
        .filter(|idx| !final_flag_vec[**idx].trim().is_empty())
        .count();
    (filtered_indices, total_flagged)
}

pub fn materialize_page(
    df: &DataFrame,
    selected_indices: &[usize],
    column_names: &[String],
    flags: &HashMap<usize, FlagEntry>,
    final_flag_vec: &[String],
    ioc_outcomes: &[IocRowOutcome],
    apply_ioc_tags: bool,
) -> Result<Vec<ProjectRow>, AppError> {
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

    let mut rows = Vec::with_capacity(selected_indices.len());
    for (position, &row_idx) in selected_indices.iter().enumerate() {
        let record = collect_row_record_from_series(&taken_series_map, column_names, position);
        let user_memo = page_flags
            .get(&row_idx)
            .and_then(|e| e.memo.clone())
            .unwrap_or_default();
        let mut final_memo = user_memo;
        if apply_ioc_tags {
            append_ioc_tags(&mut final_memo, &ioc_outcomes[row_idx].tags);
        }
        let tags = page_flags
            .get(&row_idx)
            .map(|e| e.tags.clone())
            .unwrap_or_default();
        let bookmarked = page_flags
            .get(&row_idx)
            .is_some_and(|e| e.bookmarked);
        rows.push(ProjectRow {
            row_index: row_idx,
            data: record,
            flag: final_flag_vec[row_idx].clone(),
            memo: if final_memo.is_empty() {
                None
            } else {
                Some(final_memo)
            },
            tags,
            bookmarked,
        });
    }
    Ok(rows)
}
