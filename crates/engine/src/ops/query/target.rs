use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::FlagEntry;

use super::page::{flag_population, is_unrestricted_flag_filter, FlagFilterMask};
use super::search::prepare_search;
use super::sort::ordered_row_indices;

/// Number of `resolve_target` calls since process start.
///
/// Test instrumentation only: repeated page requests for one result must not
/// increment this.
static RESOLVE_COUNT: AtomicU64 = AtomicU64::new(0);

/// Test hook: reads the `resolve_target` call counter.
#[doc(hidden)]
pub fn resolve_count() -> u64 {
    RESOLVE_COUNT.load(Ordering::Relaxed)
}

/// Test hook: resets the `resolve_target` call counter.
#[doc(hidden)]
pub fn reset_resolve_count() {
    RESOLVE_COUNT.store(0, Ordering::Relaxed);
}

/// Shared query contract for investigation operations.
///
/// `search_columns` is the search scope, not the visible or output columns.
/// `None` or an empty list means every data column.
#[derive(Debug, Clone, Default, Deserialize, Serialize, ts_rs::TS)]
#[ts(optional_fields = nullable)]
pub struct QuerySpec {
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default, rename = "searchColumns")]
    pub search_columns: Option<Vec<String>>,
    #[serde(default)]
    pub condition: Option<crate::condition::Node>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default, rename = "bookmarkedOnly")]
    pub bookmarked_only: Option<bool>,
    #[serde(default, rename = "sortKey")]
    pub sort_key: Option<String>,
    #[serde(default, rename = "sortDirection")]
    pub sort_direction: Option<String>,
}

/// Rows selected by a `QuerySpec`, in display order, plus annotation counts
/// that share the query path's definitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTarget {
    /// Original row ids in display order.
    pub filtered: Vec<usize>,
    /// Rows with a non-empty user flag inside `filtered`.
    pub annotated_filtered: usize,
    /// Rows with a non-empty user flag in the whole project.
    pub annotated_total: usize,
}

/// Resolves a `QuerySpec` to original row ids in display order.
///
/// This is the single implementation used by the page query, export, and bulk
/// targets. Search is always evaluated on the full frame; callers restrict
/// their output columns only after this returns.
pub fn resolve_target(
    df: &DataFrame,
    spec: &QuerySpec,
    all_columns: &[String],
    flags: &HashMap<usize, FlagEntry>,
    job: &JobContext,
) -> Result<ResolvedTarget, AppError> {
    RESOLVE_COUNT.fetch_add(1, Ordering::Relaxed);
    // An empty search scope is the same as "every data column".
    let search_columns = spec
        .search_columns
        .as_deref()
        .filter(|requested| !requested.is_empty());
    let search_state = prepare_search(
        df,
        search_columns,
        all_columns,
        spec.search.as_deref(),
        spec.condition.as_ref(),
        job,
    )?;
    let restrict_flags = !is_unrestricted_flag_filter(spec.flag_filter.as_deref());
    let flag_filter = FlagFilterMask::parse(spec.flag_filter.as_deref());
    let matches = |index: usize| {
        if search_state.mask.as_ref().is_some_and(|mask| !mask[index]) {
            return false;
        }
        if restrict_flags || spec.bookmarked_only == Some(true) {
            let entry = flags.get(&index);
            if restrict_flags
                && !flag_filter
                    .matches_population(entry.map_or(8, |entry| flag_population(&entry.flag)))
            {
                return false;
            }
            if spec.bookmarked_only == Some(true) && !entry.is_some_and(|entry| entry.bookmarked) {
                return false;
            }
        }
        true
    };
    // Source order is implicit; a search does not need a second N-row ordering
    // vector or N owned flag strings/bookmark bools.
    let mut filtered = if spec
        .sort_key
        .as_deref()
        .is_some_and(|key| df.column(key).is_ok())
    {
        ordered_row_indices(df, spec.sort_key.as_deref(), spec.sort_direction.as_deref())
    } else {
        (0..df.height()).collect()
    };
    let mut cancelled = None;
    let mut visited = 0usize;
    filtered.retain(|index| {
        if visited.is_multiple_of(1024) && cancelled.is_none() {
            cancelled = job.check_cancelled().err();
        }
        visited += 1;
        cancelled.is_none() && matches(*index)
    });
    if let Some(error) = cancelled {
        return Err(error);
    }
    let annotated_total = flags
        .iter()
        .filter(|(index, entry)| **index < df.height() && flag_population(&entry.flag) != 8)
        .count();
    let annotated_filtered = flags
        .iter()
        .filter(|(index, entry)| {
            **index < df.height() && flag_population(&entry.flag) != 8 && matches(**index)
        })
        .count();

    Ok(ResolvedTarget {
        filtered,
        annotated_filtered,
        annotated_total,
    })
}

/// Stable JSON for the preview token. Field order is fixed by the struct
/// definition, so the same spec always produces the same string.
pub fn spec_token_json(spec: &QuerySpec) -> String {
    serde_json::to_string(spec).unwrap_or_default()
}
