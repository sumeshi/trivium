use std::collections::HashMap;

use polars::prelude::*;
use serde_json::Value;

use crate::condition::{and_nodes, search_tokens_to_node, Node, PreparedNode};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::search::{push_searchable_value, tokenize_search_query, SearchToken};
use crate::value_utils::{anyvalue_to_search_string, record_stringified_column};

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
        let search_columns = resolve_columns(
            column_names,
            requested_scope.unwrap_or(column_names),
        )?;

        let search_raw = search.map(str::trim).filter(|value| !value.is_empty());
        let tokens = search_raw.map(tokenize_search_query).unwrap_or_default();
        let has_unscoped_terms = tokens.iter().any(|token| match token {
            SearchToken::Term { col: None, text }
            | SearchToken::QuotedTerm { col: None, text } => !text.is_empty(),
            _ => false,
        });
        let search_node = search_tokens_to_node(&tokens);
        let combined = match (search_node, condition.cloned()) {
            (Some(search_node), Some(condition)) => Some(and_nodes(search_node, condition)),
            (Some(search_node), None) => Some(search_node),
            (None, Some(condition)) => Some(condition),
            (None, None) => None,
        };
        let node = combined
            .as_ref()
            .map(PreparedNode::prepare)
            .transpose()
            .map_err(AppError::from)?;

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
        })
    }

    fn evaluate(&self, df: &DataFrame, job: &JobContext) -> Result<Option<Vec<bool>>, AppError> {
        let Some(node) = &self.node else {
            return Ok(None);
        };
        let required_series = self.series(df, &self.required_columns)?;
        let join_series = self.series(df, &self.join_columns)?;
        let row_count = df.height();
        let mut mask = vec![false; row_count];

        let mut batch_start = 0;
        while batch_start < row_count {
            job.check_cancelled()?;
            let batch_end = (batch_start + EVAL_BATCH_ROWS).min(row_count);
            for row_idx in batch_start..batch_end {
                let mut row: HashMap<String, Value> =
                    HashMap::with_capacity(self.required_columns.len() + 1);
                for (name, series) in &required_series {
                    if let Ok(value) = series.get(row_idx) {
                        if let Some(text) = anyvalue_to_search_string(&value) {
                            record_stringified_column(name);
                            row.insert((*name).to_string(), Value::String(text));
                        }
                    }
                }
                if self.needs_joined_text {
                    let mut joined = String::new();
                    for (name, series) in &join_series {
                        if let Ok(value) = series.get(row_idx) {
                            record_stringified_column(name);
                            push_searchable_value(&mut joined, &value);
                        }
                    }
                    row.insert(String::new(), Value::String(joined));
                }
                mask[row_idx] = node.eval(&row)?;
            }
            batch_start = batch_end;
        }

        Ok(Some(mask))
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

pub fn prepare_search(
    df: &DataFrame,
    requested_columns: Option<&[String]>,
    column_names: &[String],
    search: Option<&str>,
    condition: Option<&Node>,
    job: &JobContext,
) -> Result<SearchState, AppError> {
    let prepared = PreparedQuery::prepare(column_names, requested_columns, search, condition)?;
    let mask = prepared.evaluate(df, job)?;
    Ok(SearchState { mask })
}

fn resolve_columns(
    column_names: &[String],
    requested: &[String],
) -> Result<Vec<String>, AppError> {
    let mut resolved = Vec::new();
    for column in requested {
        let actual = resolve_column(column_names, column)?;
        if !resolved.iter().any(|known| known == actual) {
            resolved.push(actual.to_string());
        }
    }
    Ok(resolved)
}

fn resolve_column<'a>(
    column_names: &'a [String],
    column: &str,
) -> Result<&'a str, AppError> {
    if let Some(name) = column_names.iter().find(|name| name.as_str() == column) {
        return Ok(name.as_str());
    }
    column_names
        .iter()
        .find(|name| name.eq_ignore_ascii_case(column))
        .map(String::as_str)
        .ok_or_else(|| AppError::Message(format!("Unknown column: {}", column)))
}
