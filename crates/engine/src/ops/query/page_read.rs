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
    project_io::is_internal_column,
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
