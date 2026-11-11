//! Single IOC evaluation path.
//!
//! Every consumer of IOC results (query rows, export, applied-record counts,
//! and the initial page of a loaded project) must agree on what a given IOC set
//! means for a given frame. This module owns the frame-level evaluation; the
//! per-row helper in the parent module covers materialized pages.
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
use crate::project_io::is_internal_column;
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

/// Internal tag with the severity of the entry that produced it. Severity is
/// kept alongside the token so an appended IOC set can be merged into cached
/// outcomes in the same order a full evaluation would produce.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct InternalTag {
    /// Normalized flag (`normalize_flag_value` result).
    pub severity: String,
    /// Rendered `[tag]` token.
    pub token: String,
}

/// Internal per-row outcome stored in the IOC cache.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct InternalOutcome {
    /// Normalized flag.
    pub flag: String,
    pub tags: Vec<InternalTag>,
}

/// Definition-order normalized entries: (flag, tag, query).
fn normalized_in_order(iocs: &[IocEntry]) -> Vec<(String, String, String)> {
    iocs.iter()
        .map(|entry| {
            (
                normalize_flag_value(&entry.flag),
                entry.tag.trim().to_string(),
                entry.query.trim().to_string(),
            )
        })
        .collect()
}

/// Stable signature of an IOC set, used to key the persisted cache.
fn ioc_signature(iocs: &[IocEntry]) -> String {
    let mut normalized = normalized_in_order(iocs);
    normalized.sort();
    serde_json::to_string(&normalized).unwrap_or_default()
}

/// Evaluates all IOC entries against the whole frame in a single pass.
///
/// Column-scoped IOC terms only materialize the text of the columns they
/// reference; unscoped terms additionally build the all-column row text.
pub fn evaluate_iocs(df: &DataFrame, iocs: &[IocEntry]) -> Vec<IocRowOutcome> {
    evaluate_entries_internal(df, iocs)
        .iter()
        .map(to_public_outcome)
        .collect()
}

/// Internal evaluation shared by the full path and the incremental path.
fn evaluate_entries_internal(df: &DataFrame, iocs: &[IocEntry]) -> Vec<InternalOutcome> {
    let row_count = df.height();
    let mut outcomes = vec![InternalOutcome::default(); row_count];
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
        .filter(|name| !is_internal_column(name))
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
                if !outcomes[i].tags.iter().any(|t| t.token == token) {
                    outcomes[i].tags.push(InternalTag {
                        severity: severity.clone(),
                        token,
                    });
                }
            }
        }
    }
    outcomes
}

fn to_public_outcome(outcome: &InternalOutcome) -> IocRowOutcome {
    IocRowOutcome {
        flag: outcome.flag.clone(),
        tags: outcome.tags.iter().map(|tag| tag.token.clone()).collect(),
    }
}

/// Merges outcomes computed from a suffix of the IOC set into cached outcomes
/// computed from the prefix.
///
/// This is exact — flag and tag order included — when the new IOC set is the
/// cached set plus entries appended at the end: a full evaluation sorts
/// stably by severity, so within one severity the cached (older, earlier in
/// definition order) entries come first.
fn merge_outcomes(
    mut old: Vec<InternalOutcome>,
    added: Vec<InternalOutcome>,
) -> Vec<InternalOutcome> {
    for (target, new) in old.iter_mut().zip(added) {
        // Strictly greater only: on a tie the cached entry wins, matching the
        // full evaluation's definition-order-first rule.
        if severity_rank(&new.flag) > severity_rank(&target.flag) {
            target.flag = new.flag;
        }
        for tag in new.tags {
            let rank = severity_rank(&tag.severity);
            if let Some(existing) = target.tags.iter().position(|t| t.token == tag.token) {
                // The cached token appeared earlier in the full evaluation
                // unless the appended entry has the higher severity.
                if severity_rank(&target.tags[existing].severity) >= rank {
                    continue;
                }
                target.tags.remove(existing);
            }
            let position = target
                .tags
                .iter()
                .position(|t| severity_rank(&t.severity) < rank)
                .unwrap_or(target.tags.len());
            target.tags.insert(position, tag);
        }
    }
    old
}

/// Returns the full project-wide outcomes from the persisted IOC cache.
///
/// This is the page path's alternative to [`cached_ioc_outcomes`] when the
/// caller does not need a `DataFrame` (and must not read one). `None` means
/// the cache is absent or does not match the current IOC set and row count;
/// the caller keeps the existing evaluation path in that case.
pub fn cached_ioc_outcomes_without_frame(
    project_dir: &Path,
    iocs: &[IocEntry],
    row_count: usize,
) -> Option<Vec<IocRowOutcome>> {
    if iocs.is_empty() {
        return Some(vec![IocRowOutcome::default(); row_count]);
    }
    let cache = load_ioc_flag_cache(project_dir).ok().flatten()?;
    if cache.signature != ioc_signature(iocs) || cache.rows.len() != row_count {
        return None;
    }
    Some(cache.rows.iter().map(to_public_outcome).collect())
}

/// Returns cached IOC outcomes for the requested page rows only.
///
/// This avoids converting the whole project's cached outcomes for every page.
/// `None` has the same meaning as in [`cached_ioc_outcomes_without_frame`].
pub fn cached_ioc_page_outcomes(
    project_dir: &Path,
    iocs: &[IocEntry],
    row_count: usize,
    row_indices: &[usize],
) -> Option<HashMap<usize, IocRowOutcome>> {
    if iocs.is_empty() {
        return Some(HashMap::new());
    }
    let cache = load_ioc_flag_cache(project_dir).ok().flatten()?;
    if cache.signature != ioc_signature(iocs) || cache.rows.len() != row_count {
        return None;
    }
    let mut outcomes = HashMap::with_capacity(row_indices.len());
    for &row_index in row_indices {
        if let Some(outcome) = cache.rows.get(row_index) {
            outcomes.insert(row_index, to_public_outcome(outcome));
        }
    }
    Some(outcomes)
}

/// Returns cached IOC outcomes for the frame, rebuilding the cache when the
/// row count or the IOC set changed.
///
/// When the new IOC set is the cached set plus appended entries, only the
/// appended entries are evaluated and merged into the cached outcomes.
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
    let ordered = normalized_in_order(iocs);
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

    if let Some(cache) = cached.as_ref() {
        if cache.signature == signature && cache.rows.len() == row_count {
            return cache.rows.iter().map(to_public_outcome).collect();
        }
        let is_appended = cache.rows.len() == row_count
            && ordered.len() > cache.entries.len()
            && ordered[..cache.entries.len()] == cache.entries[..];
        if is_appended {
            let added = evaluate_entries_internal(df, &iocs[cache.entries.len()..]);
            let merged = merge_outcomes(cache.rows.clone(), added);
            if let Err(err) = save_ioc_flag_cache(project_dir, &signature, &ordered, &merged) {
                eprintln!(
                    "[cache] failed to persist IOC cache for {:?}: {:?}",
                    project_dir, err
                );
            }
            return merged.iter().map(to_public_outcome).collect();
        }
    }

    let outcomes = evaluate_entries_internal(df, iocs);
    if let Err(err) = save_ioc_flag_cache(project_dir, &signature, &ordered, &outcomes) {
        eprintln!(
            "[cache] failed to persist IOC cache for {:?}: {:?}",
            project_dir, err
        );
    }
    outcomes.iter().map(to_public_outcome).collect()
}
