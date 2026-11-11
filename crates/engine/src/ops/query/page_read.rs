//! Page-read helpers for T20a.
//!
//! The query entry point in `mod.rs` uses these to decide which columns the
//! resolver needs, which page rows to read, and how to obtain page annotations
//! without materializing the whole frame.

use std::collections::HashMap;
use std::path::Path;

use polars::prelude::{DataFrame, Series};

use crate::{
    condition::Node,
    error::AppError,
    flags::normalize_flag_value,
    ioc::{
        cached_ioc_outcomes, cached_ioc_outcomes_without_frame, cached_ioc_page_outcomes,
        IocRowOutcome,
    },
    models::{ColorRule, FlagEntry, IocEntry, ProjectRow},
    project_io::{is_internal_column, read_project_columns, INTERNAL_ROW_ID},
    search::{tokenize_search_query, SearchToken},
    storage::{load_color_rules, load_flag},
};

use super::super::util::{collect_row_record_from_series, matched_rule_color};
use super::target::QuerySpec;

pub(super) fn is_unconditional_page(spec: &QuerySpec) -> bool {
    let search_empty = spec
        .search
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty);
    let flag_filter_empty = spec
        .flag_filter
        .as_deref()
        .map(str::trim)
        .map(|filter| filter.is_empty() || filter == "all")
        .unwrap_or(true);
    let sort_empty = spec
        .sort_key
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty);
    search_empty
        && spec.condition.is_none()
        && flag_filter_empty
        && spec.bookmarked_only != Some(true)
        && sort_empty
}

/// Columns the resolver actually reads: search scope, condition columns, the
/// sort key, and `__rowid` as a row-count anchor.
pub(super) fn resolution_projection(
    spec: &QuerySpec,
    all_columns: &[String],
    has_row_id: bool,
) -> Vec<String> {
    let mut requested: Vec<String> = Vec::new();
    if let Some(search) = spec
        .search
        .as_deref()
        .map(str::trim)
        .filter(|search| !search.is_empty())
    {
        let tokens = tokenize_search_query(search);
        let has_unscoped_term = tokens.iter().any(|token| match token {
            SearchToken::Term { col: None, text } | SearchToken::QuotedTerm { col: None, text } => {
                !text.is_empty()
            }
            _ => false,
        });
        if has_unscoped_term {
            match spec
                .search_columns
                .as_deref()
                .filter(|requested| !requested.is_empty())
            {
                Some(scope) => requested.extend(scope.iter().cloned()),
                None => requested.extend(all_columns.iter().cloned()),
            }
        }
        for token in &tokens {
            match token {
                SearchToken::Term {
                    col: Some(column), ..
                }
                | SearchToken::QuotedTerm {
                    col: Some(column), ..
                } => {
                    requested.push(column.clone());
                }
                _ => {}
            }
        }
    }
    if let Some(condition) = &spec.condition {
        collect_node_columns(condition, &mut |column| requested.push(column.to_string()));
    }
    if let Some(sort_key) = spec
        .sort_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
    {
        requested.push(sort_key.to_string());
    }

    let mut resolved: Vec<String> = Vec::new();
    for name in requested {
        if let Some(actual) = all_columns
            .iter()
            .find(|known| known.eq_ignore_ascii_case(&name))
        {
            if !resolved.iter().any(|known| known == actual) {
                resolved.push(actual.clone());
            }
        }
    }
    if has_row_id && !resolved.iter().any(|column| column == INTERNAL_ROW_ID) {
        resolved.push(INTERNAL_ROW_ID.to_string());
    }
    resolved
}

pub(super) fn collect_node_columns(node: &Node, visit: &mut impl FnMut(&str)) {
    match node {
        Node::Condition(condition) => visit(condition.column.as_str()),
        Node::Group(group) => {
            for child in &group.children {
                collect_node_columns(child, visit);
            }
        }
    }
}

pub(super) fn load_page_color_rules(project_dir: &Path) -> Vec<ColorRule> {
    // Colors are decorative: a missing or malformed colors.json must never
    // fail a query, so fall back to "no rules" instead of propagating.
    match load_color_rules(project_dir) {
        Ok(rules) => rules.unwrap_or_default(),
        Err(err) => {
            eprintln!(
                "[colors] failed to load color rules for {:?}: {:?}",
                project_dir, err
            );
            Vec::new()
        }
    }
}

pub(super) fn apply_page_colors(
    rows: &mut [ProjectRow],
    df: &DataFrame,
    rules: &[ColorRule],
) -> Result<(), AppError> {
    if rules.is_empty() {
        return Ok(());
    }
    let column_names: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| !is_internal_column(name))
        .map(|name| name.to_string())
        .collect();
    let series_map: HashMap<&str, &Series> = df
        .get_columns()
        .iter()
        .map(|series| (series.name(), series))
        .collect();
    for (position, row) in rows.iter_mut().enumerate() {
        let record = collect_row_record_from_series(&series_map, &column_names, position);
        row.color = matched_rule_color(&record, rules)?;
    }
    Ok(())
}

pub(super) fn load_full_ioc_outcomes(
    project_dir: &Path,
    parquet_path: &Path,
    iocs: &[IocEntry],
    total_rows: usize,
    all_columns: &[String],
) -> Result<Vec<IocRowOutcome>, AppError> {
    if let Some(outcomes) = cached_ioc_outcomes_without_frame(project_dir, iocs, total_rows) {
        return Ok(outcomes);
    }
    // The persisted cache is missing or stale. Evaluate against all data
    // columns, because an unscoped IOC query can read any of them. A project
    // with no data columns still needs a row count for the outcome vector.
    let mut fallback_columns = all_columns.to_vec();
    if fallback_columns.is_empty() {
        fallback_columns.push(INTERNAL_ROW_ID.to_string());
    }
    let df = read_project_columns(parquet_path, &fallback_columns).map_err(AppError::from)?;
    Ok(cached_ioc_outcomes(project_dir, &df, iocs))
}

pub(super) fn load_page_ioc_outcomes(
    project_dir: &Path,
    parquet_path: &Path,
    iocs: &[IocEntry],
    total_rows: usize,
    all_columns: &[String],
    row_ids: &[usize],
) -> Result<HashMap<usize, IocRowOutcome>, AppError> {
    if let Some(outcomes) = cached_ioc_page_outcomes(project_dir, iocs, total_rows, row_ids) {
        return Ok(outcomes);
    }
    let outcomes =
        load_full_ioc_outcomes(project_dir, parquet_path, iocs, total_rows, all_columns)?;
    Ok(outcome_map(&outcomes, row_ids))
}

pub(super) fn outcome_map(
    outcomes: &[IocRowOutcome],
    row_ids: &[usize],
) -> HashMap<usize, IocRowOutcome> {
    row_ids
        .iter()
        .filter_map(|row_id| {
            outcomes
                .get(*row_id)
                .cloned()
                .map(|outcome| (*row_id, outcome))
        })
        .collect()
}

pub(super) fn page_flag_map(
    flags: &HashMap<usize, FlagEntry>,
    row_ids: &[usize],
) -> HashMap<usize, FlagEntry> {
    row_ids
        .iter()
        .filter_map(|row_id| flags.get(row_id).cloned().map(|entry| (*row_id, entry)))
        .collect()
}

pub(super) fn count_annotated_rows(
    total_rows: usize,
    flags: &HashMap<usize, FlagEntry>,
    ioc_outcomes: &[IocRowOutcome],
) -> usize {
    (0..total_rows)
        .filter(|row_id| {
            let user_flag = flags
                .get(row_id)
                .map(|entry| normalize_flag_value(&entry.flag))
                .unwrap_or_default();
            if !user_flag.is_empty() {
                return true;
            }
            ioc_outcomes
                .get(*row_id)
                .is_some_and(|outcome| !outcome.flag.is_empty())
        })
        .count()
}

pub(super) fn load_page_flags(
    flags_path: &Path,
    row_indices: &[usize],
) -> Result<HashMap<usize, FlagEntry>, AppError> {
    let mut flags = HashMap::with_capacity(row_indices.len());
    for &row_index in row_indices {
        if let Some(entry) = load_flag(flags_path, row_index).map_err(AppError::from)? {
            flags.insert(row_index, entry);
        }
    }
    Ok(flags)
}

pub(super) fn ioc_signature(iocs: &[IocEntry]) -> String {
    serde_json::to_string(iocs).unwrap_or_default()
}
