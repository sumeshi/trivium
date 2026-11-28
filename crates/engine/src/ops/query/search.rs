use std::borrow::Cow;
use std::collections::HashMap;

mod text;

use polars::prelude::*;

use crate::condition::{search_tokens_to_node, Node, PreparedNode};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::models::FlagEntry;
use crate::search::{
    prepare_tag_tokens, tokenize_search_query, validate_search_input_size, validate_search_tokens,
};
use crate::value_utils::{anyvalue_to_search_string, record_stringified_column_count};

/// Fixed evaluation batch. Cancellation is checked at each boundary.
const EVAL_BATCH_ROWS: usize = 1024;

/// A query prepared once at the start of a search.
///
/// Preparation tokenizes the search string, builds the condition tree, compiles
/// regexes, and resolves the columns the tree actually reads. Evaluation then
/// touches only those columns, plus the selected search scope when an unscoped
/// predicate exists. Contains predicates read individual cells; other
/// unscoped operations retain their joined-text semantics.
pub struct PreparedQuery {
    node: Option<PreparedNode>,
    required_columns: Vec<String>,
    join_columns: Vec<String>,
    needs_joined_text: bool,
    reads_tags: bool,
}

impl PreparedQuery {
    pub(super) fn prepare(
        column_names: &[String],
        requested_columns: Option<&[String]>,
        search: Option<&str>,
        condition: Option<&Node>,
    ) -> Result<Self, AppError> {
        validate_search_input_size(search.unwrap_or_default()).map_err(AppError::Message)?;
        let search_raw = search.map(str::trim).filter(|value| !value.is_empty());
        let tokens = search_raw.map(tokenize_search_query).unwrap_or_default();
        if search_raw.is_some() {
            validate_search_tokens(&tokens).map_err(AppError::Message)?;
        }
        let tokens = prepare_tag_tokens(&tokens).map_err(AppError::Message)?;
        let search_node = search_tokens_to_node(&tokens)
            .map_err(AppError::Message)?
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
        Self::from_compiled_node(column_names, requested_columns, node)
    }

    fn from_compiled_node(
        column_names: &[String],
        requested_columns: Option<&[String]>,
        node: Option<PreparedNode>,
    ) -> Result<Self, AppError> {
        let mut query = Self::from_compiled_nodes(column_names, requested_columns, node.iter())?;
        query.node = node;
        Ok(query)
    }

    fn from_compiled_nodes<'a>(
        column_names: &[String],
        requested_columns: Option<&[String]>,
        nodes: impl Iterator<Item = &'a PreparedNode>,
    ) -> Result<Self, AppError> {
        // An empty search scope is the same as "every data column".
        let requested_scope = requested_columns.filter(|columns| !columns.is_empty());
        let mut required_columns = Vec::new();
        let mut needs_unscoped_cells = false;
        let mut needs_joined_text = false;
        let mut reads_tags = false;
        for node in nodes {
            needs_joined_text |= node.needs_joined_text();
            reads_tags |= node.reads_tags();
            for column in node.columns() {
                if column.is_empty() {
                    needs_unscoped_cells = true;
                    continue;
                }
                let actual = resolve_column(column_names, column)?;
                if !required_columns.iter().any(|known| known == actual) {
                    required_columns.push(actual.to_string());
                }
            }
        }

        Ok(Self {
            node: None,
            required_columns,
            // Search-bar and structured predicates share the selected scope.
            join_columns: if needs_unscoped_cells {
                resolve_columns(column_names, requested_scope.unwrap_or(column_names))?
            } else {
                Vec::new()
            },
            needs_joined_text,
            reads_tags,
        })
    }

    /// Removes conjunction prefixes already satisfied by a cached result.
    /// Preparation of the complete query must validate every predicate first.
    pub(super) fn refine(
        &mut self,
        column_names: &[String],
        requested_columns: Option<&[String]>,
        search_prefix: usize,
        condition_prefix: usize,
        search_terms: usize,
        condition_terms: usize,
    ) -> Result<bool, AppError> {
        let Some(total_terms) = search_terms.checked_add(condition_terms) else {
            return Ok(false);
        };
        let actual_terms = self
            .node
            .as_ref()
            .map_or(0, |node| node.conjunction().len());
        if actual_terms != total_terms
            || search_prefix > search_terms
            || condition_prefix > condition_terms
        {
            return Ok(false);
        }
        let Some(node) = self.node.as_ref() else {
            return Ok(true);
        };
        let retain = |index: usize| {
            if index < search_terms {
                index >= search_prefix
            } else {
                index - search_terms >= condition_prefix
            }
        };
        // Resolve residual bindings before consuming the tree, leaving the
        // complete query intact if caller-supplied column metadata is invalid.
        let mut residual = Self::from_compiled_nodes(
            column_names,
            requested_columns,
            node.conjunction()
                .iter()
                .enumerate()
                .filter_map(|(index, child)| retain(index).then_some(child)),
        )?;
        let children = self
            .node
            .take()
            .expect("validated conjunction")
            .into_conjunction()
            .into_iter()
            .enumerate()
            .filter_map(|(index, child)| retain(index).then_some(child))
            .collect();
        residual.node = Some(PreparedNode::from_conjunction(children));
        *self = residual;
        Ok(true)
    }

    /// Required source columns for both projection and evaluation. Unscoped
    /// conditions use exactly the same selected scope as search-bar terms.
    pub(super) fn projection(&self) -> Vec<String> {
        let mut columns = self.required_columns.clone();
        for column in &self.join_columns {
            if !columns.contains(column) {
                columns.push(column.clone());
            }
        }
        columns
    }

    pub(super) fn has_predicate(&self) -> bool {
        self.node.is_some()
    }

    pub(super) fn evaluate(
        &self,
        df: &DataFrame,
        flags: &HashMap<usize, FlagEntry>,
        row_offset: usize,
        eligible: Option<&[bool]>,
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
                    for (index, matches) in equal.iter().enumerate() {
                        mask[index] |= matches.unwrap_or(false);
                    }
                }
                if inverted {
                    for matched in &mut mask {
                        *matched = !*matched;
                    }
                }
                if let Some(eligible) = eligible {
                    for (matched, eligible) in mask.iter_mut().zip(eligible) {
                        *matched &= *eligible;
                    }
                }
                record_stringified_column_count(name, (series.len() - series.null_count()) as u64);
                job.check_cancelled()?;
                return Ok(Some(mask));
            }
        }
        let join_series = self.series(df, &self.join_columns)?;
        if let Some(mask) = text::evaluate(
            node,
            &required_series,
            &join_series,
            df.height(),
            eligible,
            job,
        )? {
            return Ok(Some(mask));
        }
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
        let mut unscoped_cells = Vec::with_capacity(join_series.len());
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
                    if eligible.is_some_and(|eligible| !eligible[row_idx]) {
                        continue;
                    }
                    cells.clear();
                    for (slot, (_, series)) in required_series.iter().enumerate() {
                        let text = series.get(row_idx).ok().and_then(borrow_text);
                        required_counts[slot] += u64::from(text.is_some());
                        cells.push(text);
                    }
                    unscoped_cells.clear();
                    joined.clear();
                    for (slot, (_, series)) in join_series.iter().enumerate() {
                        let text = series.get(row_idx).ok().and_then(borrow_text);
                        joined_counts[slot] += u64::from(text.is_some());
                        if self.needs_joined_text {
                            if let Some(text) = &text {
                                append_searchable(&mut joined, text);
                            }
                        }
                        unscoped_cells.push(text);
                    }
                    let tags = if self.reads_tags {
                        flags
                            .get(&(row_offset + row_idx))
                            .map_or(&[][..], |entry| entry.tags.as_slice())
                    } else {
                        &[]
                    };
                    mask[row_idx] = bound.eval_with_tags_and_check(
                        &cells,
                        &unscoped_cells,
                        self.needs_joined_text.then_some(joined.as_str()),
                        tags,
                        &mut || job.check_cancelled(),
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
                    .map(|column| (name.as_str(), column.as_materialized_series()))
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
    // Exact equality and regex predicates use the original selected text.
    // Case-insensitive text operators fold it in PreparedCondition::eval_text.
    joined.push_str(text);
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

#[cfg(test)]
mod refinement_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn refinement_removes_separate_prefixes_and_keeps_remaining_order() {
        let columns =
            ["old_search", "new_search", "old_condition", "new_condition"].map(str::to_string);
        let condition: Node = serde_json::from_value(json!({
            "kind": "and",
            "children": [
                {"column": "old_condition", "op": "regex", "values": ["^old$"]},
                {"column": "new_condition", "op": "regex", "values": ["^pass$"]}
            ]
        }))
        .unwrap();
        let mut query = PreparedQuery::prepare(
            &columns,
            None,
            Some("old_search:old new_search:yes"),
            Some(&condition),
        )
        .unwrap();
        assert!(query.refine(&columns, None, 1, 1, 2, 2).unwrap());
        assert_eq!(query.projection(), ["new_search", "new_condition"]);
        let frame = DataFrame::new_infer_height(vec![
            Series::new("new_search".into(), ["yes", "yes", "no"]).into(),
            Series::new("new_condition".into(), ["pass", "fail", "pass"]).into(),
        ])
        .unwrap();
        assert_eq!(
            query
                .evaluate(&frame, &HashMap::new(), 0, None, &JobContext::none())
                .unwrap(),
            Some(vec![true, false, false])
        );
    }

    #[test]
    fn refinement_recomputes_unscoped_text_and_tag_requirements() {
        let columns = ["value".to_string(), "other".to_string()];
        let scope = ["value".to_string()];
        let condition: Node = serde_json::from_value(json!({
            "column": "", "op": "regex", "values": ["^exact value$"]
        }))
        .unwrap();
        let mut query = PreparedQuery::prepare(
            &columns,
            Some(&scope),
            Some("@tag:review"),
            Some(&condition),
        )
        .unwrap();
        assert!(query.reads_tags && query.needs_joined_text);
        assert!(query.refine(&columns, Some(&scope), 1, 0, 1, 1).unwrap());
        assert!(!query.reads_tags && query.needs_joined_text);
        assert_eq!(query.projection(), ["value"]);
        let frame = DataFrame::new_infer_height(vec![Series::new(
            "value".into(),
            ["exact value", "other"],
        )
        .into()])
        .unwrap();
        assert_eq!(
            query
                .evaluate(&frame, &HashMap::new(), 0, None, &JobContext::none())
                .unwrap(),
            Some(vec![true, false])
        );
        assert!(query.refine(&columns, Some(&scope), 0, 1, 0, 1).unwrap());
        assert!(query.projection().is_empty());
        assert!(!query.reads_tags && !query.needs_joined_text);
    }

    #[test]
    fn invalid_refinement_leaves_the_prepared_query_unchanged() {
        let columns = ["value".to_string()];
        let mut query =
            PreparedQuery::prepare(&columns, None, Some("value:keep value:yes"), None).unwrap();
        let original = format!("{:?}", query.node);
        for (search_prefix, condition_prefix, search_terms, condition_terms) in [
            (1, 0, 1, 0),
            (3, 0, 2, 0),
            (0, 1, 2, 0),
            (0, 0, usize::MAX, 1),
        ] {
            assert!(!query
                .refine(
                    &columns,
                    None,
                    search_prefix,
                    condition_prefix,
                    search_terms,
                    condition_terms,
                )
                .unwrap());
            assert_eq!(format!("{:?}", query.node), original);
            assert_eq!(query.projection(), ["value"]);
        }
    }
}
