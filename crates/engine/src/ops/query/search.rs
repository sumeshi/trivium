use std::borrow::Cow;
use std::collections::HashMap;

use polars::prelude::*;

use crate::condition::{search_tokens_to_node, Node, PreparedNode};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::FlagEntry;
use crate::search::{is_tag_scope, prepare_tag_tokens, tokenize_search_query, SearchToken};
use crate::value_utils::{anyvalue_to_search_string, record_stringified_column_count};

/// Fixed evaluation batch. Cancellation is checked at each boundary.
const EVAL_BATCH_ROWS: usize = 1024;

pub struct SearchState {
    pub mask: Option<Vec<bool>>,
}

/// A query prepared once at the start of a search.
///
/// Preparation tokenizes the search string, builds the condition tree, compiles
/// regexes, and resolves the columns the tree actually reads. Evaluation then
/// touches only those columns, plus the joined search scope when an unscoped
/// term exists.
pub struct PreparedQuery {
    node: Option<PreparedNode>,
    required_columns: Vec<String>,
    join_columns: Vec<String>,
    needs_joined_text: bool,
    reads_tags: bool,
}

impl PreparedQuery {
    fn prepare(
        column_names: &[String],
        requested_columns: Option<&[String]>,
        search: Option<&str>,
        condition: Option<&Node>,
    ) -> Result<Self, AppError> {
        // An empty search scope is the same as "every data column".
        let requested_scope = requested_columns.filter(|columns| !columns.is_empty());
        let search_raw = search.map(str::trim).filter(|value| !value.is_empty());
        let tokens = search_raw.map(tokenize_search_query).unwrap_or_default();
        let has_unscoped_terms = tokens.iter().any(|token| match token {
            SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } => {
                col.as_deref().is_none_or(str::is_empty) && !text.is_empty()
            }
            _ => false,
        });
        let search_columns = if has_unscoped_terms {
            resolve_columns(column_names, requested_scope.unwrap_or(column_names))?
        } else {
            Vec::new()
        };
        let reads_tags = tokens.iter().any(|token| match token {
            SearchToken::Term {
                col: Some(column), ..
            }
            | SearchToken::QuotedTerm {
                col: Some(column), ..
            } => is_tag_scope(column),
            _ => false,
        });
        let tokens = prepare_tag_tokens(&tokens).map_err(AppError::Message)?;
        let search_node = search_tokens_to_node(&tokens)
            .as_ref()
            .map(PreparedNode::prepare_search)
            .transpose()
            .map_err(AppError::from)?;
        let condition = condition
            .map(PreparedNode::prepare)
            .transpose()
            .map_err(AppError::from)?;
        let node = match (search_node, condition) {
            (Some(search_node), Some(condition)) => Some(search_node.and(condition)),
            (Some(search_node), None) => Some(search_node),
            (None, Some(condition)) => Some(condition),
            (None, None) => None,
        };

        let mut required_columns = Vec::new();
        let mut needs_joined_text = false;
        if let Some(node) = &node {
            for column in node.columns() {
                if column.is_empty() {
                    needs_joined_text = true;
                    continue;
                }
                let actual = resolve_column(column_names, column)?;
                if !required_columns.iter().any(|known| known == actual) {
                    required_columns.push(actual.to_string());
                }
            }
        }

        Ok(Self {
            node,
            required_columns,
            join_columns: if has_unscoped_terms {
                search_columns
            } else {
                Vec::new()
            },
            needs_joined_text,
            reads_tags,
        })
    }

    fn evaluate(
        &self,
        df: &DataFrame,
        flags: &HashMap<usize, FlagEntry>,
        job: &JobContext,
    ) -> Result<Option<Vec<bool>>, AppError> {
        let Some(node) = &self.node else {
            return Ok(None);
        };
        let required_series = self.series(df, &self.required_columns)?;
        // Exact equality on source strings has identical Polars semantics
        // once null is mapped to false before applying Ne/negation. More
        // complex predicates retain the borrowed evaluator below.
        if let Some((column, values, inverted)) = node.exact_text_comparison() {
            let (name, series) = required_series
                .iter()
                .find(|(name, _)| *name == column)
                .or_else(|| {
                    required_series
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(column))
                })
                .expect("validated exact comparison column");
            if let Ok(strings) = series.str() {
                job.check_cancelled()?;
                let mut mask = vec![false; df.height()];
                for value in values {
                    job.check_cancelled()?;
                    let equal = strings.equal(value.as_str());
                    for (index, matches) in equal.into_iter().enumerate() {
                        mask[index] |= matches.unwrap_or(false);
                    }
                }
                if inverted {
                    for matched in &mut mask {
                        *matched = !*matched;
                    }
                }
                record_stringified_column_count(name, (series.len() - series.null_count()) as u64);
                job.check_cancelled()?;
                return Ok(Some(mask));
            }
        }
        let join_series = self.series(df, &self.join_columns)?;
        let bound = node.bind_columns(&|column| {
            self.required_columns
                .iter()
                .position(|name| name == column)
                .or_else(|| {
                    self.required_columns
                        .iter()
                        .position(|name| name.eq_ignore_ascii_case(column))
                })
                .expect("validated query column")
        });
        let row_count = df.height();
        let mut mask = vec![false; row_count];
        let mut cells = Vec::with_capacity(required_series.len());
        let mut joined = String::new();
        let mut required_counts = vec![0; required_series.len()];
        let mut joined_counts = vec![0; join_series.len()];

        let outcome = (|| {
            let mut batch_start = 0;
            while batch_start < row_count {
                job.check_cancelled()?;
                let batch_end = (batch_start + EVAL_BATCH_ROWS).min(row_count);
                // This index addresses several parallel column series and the mask.
                #[allow(clippy::needless_range_loop)]
                for row_idx in batch_start..batch_end {
                    cells.clear();
                    for (slot, (_, series)) in required_series.iter().enumerate() {
                        let text = series.get(row_idx).ok().and_then(borrow_text);
                        required_counts[slot] += u64::from(text.is_some());
                        cells.push(text);
                    }
                    if self.needs_joined_text {
                        joined.clear();
                        for (slot, (_, series)) in join_series.iter().enumerate() {
                            if let Some(text) = series.get(row_idx).ok().and_then(borrow_text) {
                                joined_counts[slot] += 1;
                                append_searchable(&mut joined, &text);
                            }
                        }
                    }
                    let tags = if self.reads_tags {
                        flags
                            .get(&row_idx)
                            .map_or(&[][..], |entry| entry.tags.as_slice())
                    } else {
                        &[]
                    };
                    mask[row_idx] = bound.eval_with_tags(
                        &cells,
                        self.needs_joined_text.then_some(joined.as_str()),
                        tags,
                    )?;
                }
                batch_start = batch_end;
            }
            Ok(Some(mask))
        })();
        for ((name, _), count) in required_series.iter().zip(required_counts) {
            record_stringified_column_count(name, count);
        }
        for ((name, _), count) in join_series.iter().zip(joined_counts) {
            record_stringified_column_count(name, count);
        }
        outcome
    }

    fn series<'a>(
        &'a self,
        df: &'a DataFrame,
        columns: &'a [String],
    ) -> Result<Vec<(&'a str, &'a Series)>, AppError> {
        columns
            .iter()
            .map(|name| {
                df.column(name.as_str())
                    .map(|series| (name.as_str(), series))
                    .map_err(|_| AppError::Message(format!("Unknown column: {}", name)))
            })
            .collect()
    }
}

fn borrow_text(value: AnyValue<'_>) -> Option<Cow<'_, str>> {
    match value {
        AnyValue::Null => None,
        AnyValue::String(text) => Some(Cow::Borrowed(text)),
        other => anyvalue_to_search_string(&other).map(Cow::Owned),
    }
}

fn append_searchable(joined: &mut String, text: &str) {
    if text.is_empty() {
        return;
    }
    if !joined.is_empty() {
        joined.push(' ');
    }
    if text.is_ascii() {
        joined.extend(
            text.bytes()
                .map(|byte| char::from(byte.to_ascii_lowercase())),
        );
    } else {
        // Keep Rust's contextual Unicode lowercase rules (not per-char folding).
        joined.push_str(&text.to_lowercase());
    }
}

pub fn prepare_search(
    df: &DataFrame,
    requested_columns: Option<&[String]>,
    column_names: &[String],
    search: Option<&str>,
    condition: Option<&Node>,
    flags: &HashMap<usize, FlagEntry>,
    job: &JobContext,
) -> Result<SearchState, AppError> {
    let prepared = PreparedQuery::prepare(column_names, requested_columns, search, condition)?;
    let mask = prepared.evaluate(df, flags, job)?;
    Ok(SearchState { mask })
}

fn resolve_columns(column_names: &[String], requested: &[String]) -> Result<Vec<String>, AppError> {
    let mut resolved = Vec::new();
    for column in requested {
        let actual = resolve_column(column_names, column)?;
        if !resolved.iter().any(|known| known == actual) {
            resolved.push(actual.to_string());
        }
    }
    Ok(resolved)
}

fn resolve_column<'a>(column_names: &'a [String], column: &str) -> Result<&'a str, AppError> {
    if let Some(name) = column_names.iter().find(|name| name.as_str() == column) {
        return Ok(name.as_str());
    }
    column_names
        .iter()
        .find(|name| name.eq_ignore_ascii_case(column))
        .map(String::as_str)
        .ok_or_else(|| AppError::Message(format!("Unknown column: {}", column)))
}
