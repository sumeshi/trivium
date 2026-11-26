use std::collections::HashMap;

use polars::prelude::*;

use crate::error::AppError;
use crate::flags::normalize_flag_value;
use crate::models::{ColumnMeta, FlagEntry, ProjectRow};
use crate::value_utils::{build_row_cells, CellColumns};

/// Parse once per query population, rather than splitting a combination for
/// every row. Unknown options must never expand a filtered operation to all rows.
pub(super) struct FlagFilterMask(u8);

impl FlagFilterMask {
    const ALL: Self = Self(u8::MAX);

    pub(super) fn parse(filter: Option<&str>) -> Result<Self, AppError> {
        let filter = filter.unwrap_or("all");
        if filter == "all" {
            return Ok(Self::ALL);
        }
        let mut mask = 0;
        let mut previous = 0;
        for token in filter.split('|') {
            let population = match token {
                "safe" => 1,
                "suspicious" => 2,
                "critical" => 4,
                "none" => 8,
                invalid => {
                    return Err(AppError::Message(format!(
                        "Invalid flag filter value: {invalid:?}."
                    )))
                }
            };
            if population <= previous {
                return Err(AppError::Message(format!(
                    "Invalid flag filter value: {filter:?}. Select each flag once in canonical order."
                )));
            }
            mask |= population;
            previous = population;
        }
        Ok(Self(mask))
    }

    pub(super) fn matches_population(&self, population: u8) -> bool {
        self.0 == Self::ALL.0 || self.0 & population != 0
    }
}

/// Match canonical persisted flags without allocating a String.
pub(super) fn flag_population(flag: &str) -> u8 {
    match flag {
        "safe" => 1,
        "suspicious" => 2,
        "critical" => 4,
        _ => 8,
    }
}

/// No selection and selecting every normalized flag population are both
/// unrestricted, while the UI can retain their distinct checkbox state.
pub(super) fn is_unrestricted_flag_filter(filter: Option<&str>) -> bool {
    FlagFilterMask::parse(filter).is_ok_and(|filter| matches!(filter.0, 15 | u8::MAX))
}

/// Materializes a page from an already page-sized frame in display order.
///
/// `row_ids[position]` is the original row id for `df`'s row `position`.
/// The frame may contain extra columns (for color evaluation); only `columns`
/// become row data and cells. Annotations come from the captured saved snapshot.
pub fn materialize_page_rows(
    df: &DataFrame,
    row_ids: &[usize],
    columns: &[ColumnMeta],
    flags: &HashMap<usize, FlagEntry>,
) -> Result<Vec<ProjectRow>, AppError> {
    if df.height() != row_ids.len() {
        return Err(AppError::Message(
            "Page row count does not match the selected row ids.".into(),
        ));
    }
    let cell_columns = CellColumns::new(columns);
    let mut rows = Vec::with_capacity(row_ids.len());
    for (position, &row_idx) in row_ids.iter().enumerate() {
        let cells = build_row_cells(df, position, columns, &cell_columns);
        let entry = flags.get(&row_idx);
        rows.push(ProjectRow {
            row_index: row_idx,
            cells,
            flag: entry
                .map(|entry| normalize_flag_value(&entry.flag))
                .unwrap_or_default(),
            memo: entry
                .and_then(|entry| entry.memo.clone())
                .filter(|memo| !memo.is_empty()),
            tags: entry.map(|entry| entry.tags.clone()).unwrap_or_default(),
            bookmarked: entry.is_some_and(|entry| entry.bookmarked),
            color: None,
        });
    }
    Ok(rows)
}
