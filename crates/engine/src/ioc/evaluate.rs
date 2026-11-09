//! Single IOC evaluation path.
//!
//! Every consumer of IOC results (query rows, export, applied-record counts,
//! and the page-level tag path via `row_contains_query`) must agree on what a
//! given IOC set means for a given frame. This module owns the frame-level
//! evaluation; the per-row helper in the parent module covers materialized
//! pages.
//!
//! IOC matching is a data-level predicate: all data columns participate,
//! regardless of which columns the UI happens to show.

use std::collections::HashMap;
use std::path::Path;

use polars::prelude::{DataFrame, Series};
use serde::{Deserialize, Serialize};

use crate::flags::{normalize_flag_value, severity_rank};
use crate::models::IocEntry;
use crate::ops::util::ensure_column_text_cache;
use crate::search::{
    build_search_mask_boolean, build_searchable_text, to_rpn, tokenize_search_query, SearchToken,
};
use crate::storage::{load_ioc_flag_cache, save_ioc_flag_cache};

/// Per-row IOC evaluation result: the highest-severity matched flag plus every
/// matched tag (as `[tag]` tokens, deduplicated, in evaluation order).
/// `flag` is empty when nothing matched.
///
/// This is the single source of truth for IOC semantics: the query path, the
/// export path, the applied-record count, and the initial page of a loaded
/// project all consume `evaluate_iocs` results. Outcomes are independent of
/// user flags — callers decide precedence.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IocRowOutcome {
    pub flag: String,
    pub tags: Vec<String>,
}

/// Stable signature of an IOC set, used to key the persisted cache.
fn ioc_signature(iocs: &[IocEntry]) -> String {
    let mut normalized: Vec<(String, String, String)> = iocs
        .iter()
        .map(|e| {
            (
                normalize_flag_value(&e.flag),
                e.tag.trim().to_string(),
                e.query.trim().to_string(),
            )
        })
        .collect();
    normalized.sort();
    serde_json::to_string(&normalized).unwrap_or_default()
}

/// Evaluates all IOC entries against the whole frame in a single pass.
///
/// Column-scoped IOC terms only materialize the text of the columns they
/// reference; unscoped terms additionally build the all-column row text.
pub fn evaluate_iocs(df: &DataFrame, iocs: &[IocEntry]) -> Vec<IocRowOutcome> {
    let row_count = df.height();
    let mut outcomes = vec![IocRowOutcome::default(); row_count];
    if iocs.is_empty() || row_count == 0 {
        return outcomes;
    }

    let column_series: HashMap<&str, &Series> =
        df.get_columns().iter().map(|s| (s.name(), s)).collect();
    let column_series_lower: HashMap<String, &Series> = df
        .get_columns()
        .iter()
        .map(|s| (s.name().to_lowercase(), s))
        .collect();
    let all_columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| *name != "__rowid")
        .map(|name| name.to_string())
        .collect();

    // Highest severity first; the sort is stable so equal severities keep
    // their definition order (first match wins the flag).
    let mut sorted: Vec<&IocEntry> = iocs.iter().collect();
    sorted.sort_by_key(|e| std::cmp::Reverse(severity_rank(&normalize_flag_value(&e.flag))));

    let mut per_column_text: HashMap<String, Vec<String>> = HashMap::new();
    // Built lazily: only needed when some IOC term has no column scope.
    let mut all_column_text: Option<Vec<String>> = None;

    for entry in sorted {
        let query = entry.query.trim();
        if query.is_empty() {
            continue;
        }
        let tokens = tokenize_search_query(query);
        let mut terms: Vec<(Option<String>, String)> = Vec::new();
        let mut needed_cols: Vec<String> = Vec::new();
        let mut needs_all_column_text = false;
        for token in &tokens {
            if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = token
            {
                if text.is_empty() {
                    continue;
                }
                let key = (col.clone(), text.clone());
                if !terms.contains(&key) {
                    terms.push(key);
                }
                match col {
                    Some(c) => {
                        if !needed_cols.contains(c) {
                            needed_cols.push(c.clone());
                        }
                    }
                    None => needs_all_column_text = true,
                }
            }
        }
        if terms.is_empty() {
            continue;
        }
        let rpn = to_rpn(&tokens);
        for c in needed_cols {
            ensure_column_text_cache(&c, &column_series_lower, &mut per_column_text, row_count);
        }
        if needs_all_column_text && all_column_text.is_none() {
            all_column_text = Some(build_searchable_text(row_count, &all_columns, &column_series));
        }
        // The mask length follows the text slice. Every term is either
        // column-scoped (served from per_column_text) or covered by
        // all_column_text, so any row_count-sized slice works as the provider.
        let length_provider: &[String] = all_column_text
            .as_deref()
            .or_else(|| per_column_text.values().next().map(|v| v.as_slice()))
            .unwrap_or_default();
        let mask = build_search_mask_boolean(&rpn, &terms, length_provider, Some(&per_column_text));

        let severity = normalize_flag_value(&entry.flag);
        let tag = entry.tag.trim().to_string();
        for (i, matched) in mask.into_iter().enumerate() {
            if !matched {
                continue;
            }
            if severity_rank(&severity) > severity_rank(&outcomes[i].flag) {
                outcomes[i].flag = severity.clone();
            }
            if !tag.is_empty() {
                let token = format!("[{}]", tag);
                if !outcomes[i].tags.contains(&token) {
                    outcomes[i].tags.push(token);
                }
            }
        }
    }
    outcomes
}

/// Returns cached IOC outcomes for the frame, rebuilding the cache when the
/// row count or the IOC set changed.
///
/// The cache stores raw outcomes (no user-flag awareness), so user flag
/// updates never invalidate it; only IOC definition changes do — and even
/// those are caught by the signature check, which also covers a cache that
/// legitimately matched nothing.
pub fn cached_ioc_outcomes(
    project_dir: &Path,
    df: &DataFrame,
    iocs: &[IocEntry],
) -> Vec<IocRowOutcome> {
    let row_count = df.height();
    if iocs.is_empty() || row_count == 0 {
        return vec![IocRowOutcome::default(); row_count];
    }
    let signature = ioc_signature(iocs);

    let cached = match load_ioc_flag_cache(project_dir) {
        Ok(cache) => cache,
        Err(err) => {
            eprintln!(
                "[cache] failed to load IOC cache for {:?}: {:?}",
                project_dir, err
            );
            None
        }
    };
    let valid = cached
        .as_ref()
        .map(|entry| entry.signature == signature && entry.rows.len() == row_count)
        .unwrap_or(false);
    if valid {
        return cached.expect("checked above").rows;
    }

    let outcomes = evaluate_iocs(df, iocs);
    if let Err(err) = save_ioc_flag_cache(project_dir, &signature, &outcomes) {
        eprintln!(
            "[cache] failed to persist IOC cache for {:?}: {:?}",
            project_dir, err
        );
    }
    outcomes
}
