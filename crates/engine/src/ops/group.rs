use polars::prelude::Series;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::analysis::group::{add_row, finalize, GroupNode, GroupNodePartial, GroupTreeRow};
use crate::error::AppError;
use crate::jobs::JobContext;
use crate::project_io::read_project_dataframe;
use crate::storage::load_flags;
use crate::store::ProjectsStore;
use crate::value_utils::{anyvalue_to_json, anyvalue_to_search_string};

use super::query::{data_columns, resolve_target, QuerySpec};
use super::util::resolve_columns;

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

#[derive(Debug, Serialize)]
pub struct GroupTreeNode {
    pub value: serde_json::Value,
    pub label: String,
    pub count: usize,
    pub children: Vec<GroupTreeNode>,
}

#[derive(Debug, Serialize)]
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
    let requested: Vec<String> = payload
        .group_columns
        .iter()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect();
    if requested.is_empty() {
        return Err(AppError::Message("Group columns must not be empty.".into()));
    }

    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let columns = data_columns(&df, meta.storage_version);
    let resolved = resolve_columns(&df, &requested, "group columns")?;
    let group_series: Vec<Option<&Series>> =
        resolved.iter().map(|name| df.column(name).ok()).collect();

    let flags = load_flags(&store.flags_path(&meta.id)).map_err(AppError::from)?;
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
    let filtered = resolve_target(&df, &spec, &columns, &flags, &JobContext::none())?.filtered;
    let total = filtered.len();

    let mut root = GroupNodePartial::default();
    for row_idx in filtered {
        let mut labels = Vec::with_capacity(group_series.len());
        let mut values = Vec::with_capacity(group_series.len());
        for series in &group_series {
            let value = series.and_then(|series| series.get(row_idx).ok());
            labels.push(
                value
                    .as_ref()
                    .and_then(anyvalue_to_search_string)
                    .map(|text| text.trim().to_string())
                    .unwrap_or_default(),
            );
            values.push(value.as_ref().map(anyvalue_to_json).unwrap_or(Value::Null));
        }
        add_row(&mut root, &GroupTreeRow { labels, values });
    }

    let nodes = finalize(&root).into_iter().map(to_response).collect();
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
