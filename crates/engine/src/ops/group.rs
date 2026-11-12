use polars::prelude::Series;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::analysis::group::{GroupNode, GroupTreeBuilder, MAX_GROUP_LEVELS};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::store::ProjectsStore;
use crate::value_utils::{anyvalue_to_json, anyvalue_to_search_string};

use super::analysis_data::{AnalysisSource, SelectedRows};
use super::query::QuerySpec;

#[derive(Debug, Deserialize)]
pub struct GroupTreePayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    #[serde(default)]
    pub search: Option<String>,
    /// Deprecated visible-column field kept for old callers.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "searchColumns")]
    pub search_columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default, rename = "bookmarkedOnly")]
    pub bookmarked_only: Option<bool>,
    #[serde(default)]
    pub condition: Option<crate::condition::Node>,
    #[serde(rename = "groupColumns")]
    pub group_columns: Vec<String>,
}

#[derive(Debug, Serialize, ts_rs::TS)]
pub struct GroupTreeNode {
    pub value: serde_json::Value,
    pub label: String,
    pub count: usize,
    pub children: Vec<GroupTreeNode>,
}

#[derive(Debug, Serialize, ts_rs::TS)]
pub struct GroupTreeResponse {
    pub columns: Vec<String>,
    pub nodes: Vec<GroupTreeNode>,
    pub total: usize,
}

/// Builds a hierarchical grouping over rows that match the current filter.
pub fn group_tree(
    store: &ProjectsStore,
    payload: GroupTreePayload,
) -> Result<GroupTreeResponse, AppError> {
    group_tree_with_job(store, payload, &JobContext::none())
}

pub fn group_tree_with_job(
    store: &ProjectsStore,
    payload: GroupTreePayload,
    job: &JobContext,
) -> Result<GroupTreeResponse, AppError> {
    let requested: Vec<String> = payload
        .group_columns
        .iter()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect();
    if requested.is_empty() {
        return Err(AppError::Message("Group columns must not be empty.".into()));
    }
    if requested.len() > MAX_GROUP_LEVELS {
        return Err(AppError::Message(
            "Grouping supports at most 8 column levels.".into(),
        ));
    }

    let spec = QuerySpec {
        search: payload.search.clone(),
        search_columns: payload
            .search_columns
            .clone()
            .or_else(|| payload.columns.clone()),
        condition: payload.condition.clone(),
        flag_filter: payload.flag_filter.clone(),
        bookmarked_only: payload.bookmarked_only,
        sort_key: None,
        sort_direction: None,
    };
    let source = AnalysisSource::open(store, payload.project_id, &spec, job)?;
    let resolved = source.columns(&requested, "group columns")?;
    let unique: std::collections::HashSet<_> = resolved.iter().collect();
    if unique.len() != resolved.len() {
        return Err(AppError::Message(
            "Choose each grouping column only once.".into(),
        ));
    }
    let total = source.target.ids.len();
    let selected = SelectedRows::new(source.target.total_rows, &source.target.ids);
    let mut reader = source.selected_reader(&resolved)?;
    let mut tree = GroupTreeBuilder::default();
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch.map_err(AppError::from)?;
        let group_series: Vec<&Series> = resolved
            .iter()
            .map(|name| batch.frame.column(name).expect("projected column"))
            .collect();
        for row_idx in 0..batch.frame.height() {
            if row_idx % 1024 == 0 {
                job.check_cancelled()?;
            }
            if !selected.contains(batch.offset + row_idx) {
                continue;
            }
            let mut labels = Vec::with_capacity(group_series.len());
            let mut values = Vec::with_capacity(group_series.len());
            for series in &group_series {
                let value = series.get(row_idx).ok();
                labels.push(
                    value
                        .as_ref()
                        .and_then(anyvalue_to_search_string)
                        .unwrap_or_default(),
                );
                values.push(value.as_ref().map(anyvalue_to_json).unwrap_or(Value::Null));
            }
            tree.add_row(&labels, &values)
                .map_err(|message| AppError::Message(message.into()))?;
        }
    }
    job.check_cancelled()?;
    let nodes = tree.finish().into_iter().map(to_response).collect();
    Ok(GroupTreeResponse {
        columns: resolved,
        nodes,
        total,
    })
}

fn to_response(node: GroupNode) -> GroupTreeNode {
    GroupTreeNode {
        value: node.value,
        label: node.label,
        count: node.count,
        children: node.children.into_iter().map(to_response).collect(),
    }
}
