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
    models::{ColorRule, ProjectRow},
    project_io::{is_internal_column, INTERNAL_ROW_ID},
    search::{tokenize_search_query, SearchToken},
    storage::load_color_rules,
};

use super::super::util::{collect_row_record_from_series, matched_rule_color};
use super::page::is_unrestricted_flag_filter;
use super::target::QuerySpec;

pub(super) fn is_unconditional_page(spec: &QuerySpec) -> bool {
    is_source_order_unsearched(spec)
        && is_unrestricted_flag_filter(spec.flag_filter.as_deref())
        && spec.bookmarked_only != Some(true)
}

pub(super) fn is_source_order_unsearched(spec: &QuerySpec) -> bool {
    let search_empty = spec
        .search
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty);
    let sort_empty = spec
        .sort_key
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty);
    search_empty && spec.condition.is_none() && sort_empty
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
            .find(|known| *known == &name)
            .or_else(|| {
                all_columns
                    .iter()
                    .find(|known| known.eq_ignore_ascii_case(&name))
            })
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
    node.visit_columns(visit);
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
