use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::FlagEntry;

use super::page::{flag_population, FlagFilterMask};
use super::search::PreparedQuery;
use super::sort::sort_row_indices;

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

impl QuerySpec {
    /// Validate options before a metadata/cache fast path can omit resolution.
    pub fn validate_filter(&self) -> Result<(), AppError> {
        FlagFilterMask::parse(self.flag_filter.as_deref())?;
        crate::search::validate_search_query(self.search.as_deref().unwrap_or_default())
            .map_err(AppError::Message)
    }
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
/// The in-memory/reference path shares membership evaluation with persisted
/// batch searches. Output-column restrictions never change the search scope.
pub fn resolve_target(
    df: &DataFrame,
    spec: &QuerySpec,
    all_columns: &[String],
    flags: &HashMap<usize, FlagEntry>,
    job: &JobContext,
) -> Result<ResolvedTarget, AppError> {
    spec.validate_filter()?;
    // An empty search scope is the same as "every data column".
    let search_columns = spec
        .search_columns
        .as_deref()
        .filter(|requested| !requested.is_empty());
    let prepared = PreparedQuery::prepare(
        all_columns,
        search_columns,
        spec.search.as_deref(),
        spec.condition.as_ref(),
    )?;
    resolve_target_prepared(df, spec, flags, &prepared, job)
}

pub(super) fn resolve_target_prepared(
    df: &DataFrame,
    spec: &QuerySpec,
    flags: &HashMap<usize, FlagEntry>,
    prepared: &PreparedQuery,
    job: &JobContext,
) -> Result<ResolvedTarget, AppError> {
    record_resolution();
    let mut filtered = Vec::new();
    let annotated_filtered =
        append_matching_rows(df, 0, spec, prepared, flags, &mut filtered, job)?;
    let filtered = sort_row_indices(
        df,
        filtered,
        spec.sort_key.as_deref(),
        spec.sort_direction.as_deref(),
        job,
    )?;
    let annotated_total = flags
        .iter()
        .filter(|(index, entry)| **index < df.height() && flag_population(&entry.flag) != 8)
        .count();

    Ok(ResolvedTarget {
        filtered,
        annotated_filtered,
        annotated_total,
    })
}

pub(super) fn record_resolution() {
    RESOLVE_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Shared membership evaluation for an in-memory frame or one decoded batch.
/// Annotation IDs always refer to source rows, never positions within a batch.
pub(super) fn append_matching_rows(
    frame: &DataFrame,
    row_offset: usize,
    spec: &QuerySpec,
    prepared: &PreparedQuery,
    flags: &HashMap<usize, FlagEntry>,
    output: &mut Vec<usize>,
    job: &JobContext,
) -> Result<usize, AppError> {
    let mask = prepared.evaluate(frame, flags, row_offset, job)?;
    let flag_filter = FlagFilterMask::parse(spec.flag_filter.as_deref())?;
    let mut annotated = 0;
    for index in 0..frame.height() {
        if index.is_multiple_of(1024) {
            job.check_cancelled()?;
        }
        if mask.as_ref().is_some_and(|mask| !mask[index]) {
            continue;
        }
        let row_id = row_offset + index;
        let entry = flags.get(&row_id);
        let population = entry.map_or(8, |entry| flag_population(&entry.flag));
        if !flag_filter.matches_population(population)
            || (spec.bookmarked_only == Some(true) && !entry.is_some_and(|entry| entry.bookmarked))
        {
            continue;
        }
        output.push(row_id);
        annotated += usize::from(population != 8);
    }
    Ok(annotated)
}

/// Stable JSON for the preview token. Field order is fixed by the struct
/// definition, so the same spec always produces the same string.
pub fn spec_token_json(spec: &QuerySpec) -> String {
    serde_json::to_string(spec).unwrap_or_default()
}
