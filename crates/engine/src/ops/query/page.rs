use std::collections::HashMap;

use anyhow::Error as AnyhowError;
use polars::prelude::*;

use crate::error::AppError;
use crate::flags::normalize_flag_value;
use crate::ioc::IocRowOutcome;
use crate::models::{ColumnMeta, FlagEntry, ProjectRow};
use crate::value_utils::{build_row_cells, CellColumns};

use super::ioc::append_ioc_tags;

/// Parse once per query population, rather than splitting a combination for
/// every row. Unknown legacy values retain the old match-all fallback.
struct FlagFilterMask(u8);

impl FlagFilterMask {
    const ALL: Self = Self(u8::MAX);

    fn parse(filter: Option<&str>) -> Self {
        let filter = filter.unwrap_or("all").trim();
        match filter {
            "" | "all" => return Self::ALL,
            "priority" => return Self(2 | 4),
            _ => {}
        }
        let mut mask = 0;
        for token in filter.split('|') {
            mask |= match token.trim() {
                "safe" => 1,
                "suspicious" => 2,
                "critical" => 4,
                "none" => 8,
                _ => return Self::ALL,
            };
        }
        Self(mask)
    }

    fn matches(&self, flag: &str) -> bool {
        self.0 == Self::ALL.0
            || self.0
                & match flag {
                    "safe" => 1,
                    "suspicious" => 2,
                    "critical" => 4,
                    "" => 8,
                    _ => 0,
                }
                != 0
    }
}

/// No selection and selecting every normalized flag population are both
/// unrestricted, while the UI can retain their distinct checkbox state.
pub(super) fn is_unrestricted_flag_filter(filter: Option<&str>) -> bool {
    matches!(FlagFilterMask::parse(filter).0, 15 | u8::MAX)
}

pub fn filtered_indices(
    ordered_indices: &[usize],
    final_flag_vec: &[String],
    bookmarked: &[bool],
    bookmarked_only: Option<bool>,
    flag_filter: Option<&str>,
    search_mask: Option<&[bool]>,
) -> (Vec<usize>, usize) {
    let flag_filter = FlagFilterMask::parse(flag_filter);
    let mut filtered_indices: Vec<usize> = Vec::with_capacity(ordered_indices.len());
    for &idx in ordered_indices {
        let ff = &final_flag_vec[idx];
        if !flag_filter.matches(ff) {
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
    let annotated_filtered = filtered_indices
        .iter()
        .filter(|idx| !final_flag_vec[**idx].trim().is_empty())
        .count();
    (filtered_indices, annotated_filtered)
}

/// Final flag for one row: a non-empty user flag wins, otherwise the IOC flag.
///
/// Computing this from the page's own annotations keeps a cache hit from
/// rebuilding a project-wide flag vector.
fn final_flag_for(
    row_idx: usize,
    page_flags: &HashMap<usize, FlagEntry>,
    ioc_outcomes: &HashMap<usize, IocRowOutcome>,
) -> String {
    let user_flag = page_flags
        .get(&row_idx)
        .map(|entry| normalize_flag_value(&entry.flag))
        .unwrap_or_default();
    if !user_flag.is_empty() {
        return user_flag;
    }
    ioc_outcomes
        .get(&row_idx)
        .map(|outcome| outcome.flag.clone())
        .unwrap_or_default()
}

/// Materializes a page from an already page-sized frame in display order.
///
/// `row_ids[position]` is the original row id for `df`'s row `position`.
/// `ioc_outcomes` only needs the page's rows, so a page read never converts
/// the project-wide IOC outcome vector. The frame may contain extra columns
/// (for color evaluation); only `columns` become row data and cells.
pub fn materialize_page_rows(
    df: &DataFrame,
    row_ids: &[usize],
    columns: &[ColumnMeta],
    flags: &HashMap<usize, FlagEntry>,
    ioc_outcomes: &HashMap<usize, IocRowOutcome>,
    apply_ioc_tags: bool,
) -> Result<Vec<ProjectRow>, AppError> {
    if df.height() != row_ids.len() {
        return Err(AppError::Message(
            "Page row count does not match the selected row ids.".into(),
        ));
    }
    let cell_columns = CellColumns::new(df, columns);
    let page_flags: HashMap<usize, FlagEntry> = row_ids
        .iter()
        .filter_map(|idx| flags.get(idx).cloned().map(|entry| (*idx, entry)))
        .collect();

    let mut rows = Vec::with_capacity(row_ids.len());
    for (position, &row_idx) in row_ids.iter().enumerate() {
        let cells = build_row_cells(df, position, columns, &cell_columns);
        let user_memo = page_flags
            .get(&row_idx)
            .and_then(|e| e.memo.clone())
            .unwrap_or_default();
        let mut final_memo = user_memo;
        if apply_ioc_tags {
            if let Some(outcome) = ioc_outcomes.get(&row_idx) {
                append_ioc_tags(&mut final_memo, &outcome.tags);
            }
        }
        let tags = page_flags
            .get(&row_idx)
            .map(|e| e.tags.clone())
            .unwrap_or_default();
        let bookmarked = page_flags.get(&row_idx).is_some_and(|e| e.bookmarked);
        rows.push(ProjectRow {
            row_index: row_idx,
            cells,
            flag: final_flag_for(row_idx, &page_flags, ioc_outcomes),
            memo: if final_memo.is_empty() {
                None
            } else {
                Some(final_memo)
            },
            tags,
            bookmarked,
            color: None,
        });
    }
    Ok(rows)
}

/// Materializes arbitrary rows from a full frame.
///
/// Report and surrounding-row views already hold a full frame; this keeps
/// their behavior while the query page path passes an already page-sized
/// frame to [`materialize_page_rows`].
pub fn materialize_page(
    df: &DataFrame,
    selected_indices: &[usize],
    columns: &[ColumnMeta],
    flags: &HashMap<usize, FlagEntry>,
    ioc_outcomes: &[IocRowOutcome],
    apply_ioc_tags: bool,
) -> Result<Vec<ProjectRow>, AppError> {
    let take_idx =
        UInt32Chunked::from_iter_values("take_idx", selected_indices.iter().map(|&idx| idx as u32));
    let taken_df = df
        .take(&take_idx)
        .map_err(|err| AppError::from(AnyhowError::from(err)))?;
    let page_ioc_outcomes: HashMap<usize, IocRowOutcome> = selected_indices
        .iter()
        .filter_map(|idx| {
            ioc_outcomes
                .get(*idx)
                .cloned()
                .map(|outcome| (*idx, outcome))
        })
        .collect();
    materialize_page_rows(
        &taken_df,
        selected_indices,
        columns,
        flags,
        &page_ioc_outcomes,
        apply_ioc_tags,
    )
}
