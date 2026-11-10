use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Where a data column came from and how it is displayed.
///
/// Legacy projects (storage version 0) have no column metadata; their ids and
/// labels are synthesized from the original frame column names. New projects
/// use an internal id (`c000001`) so the display label and the source path can
/// differ from the column the query paths address.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnMeta {
    /// Internal column id. `c000001` for the new storage format, the original
    /// column name for legacy projects.
    pub id: String,
    /// Display label. Not guaranteed to be unique.
    pub label: String,
    /// Original CSV header or JSON Pointer.
    pub source: String,
    /// Column order in the source.
    pub order: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectMeta {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub total_records: usize,
    #[serde(default)]
    pub flagged_records: usize,
    #[serde(default)]
    pub ioc_applied_records: usize,
    #[serde(default)]
    pub source_sha256: Option<String>,
    pub hidden_columns: Vec<String>,
    /// 0/1 = legacy typed Parquet, 2 = internal column ids and `__original`.
    #[serde(default)]
    pub storage_version: u32,
    #[serde(default)]
    pub columns: Vec<ColumnMeta>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectSummary {
    pub meta: ProjectMeta,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectRow {
    pub row_index: usize,
    pub data: HashMap<String, Value>,
    pub flag: String,
    pub memo: Option<String>,
    pub tags: Vec<String>,
    pub bookmarked: bool,
    /// `None` when no color rule matches. Query and flag updates evaluate this
    /// in the engine so the frontend never carries a second predicate.
    pub color: Option<String>,
}

/// A condition-to-color rule. `condition` is the shared F1 node, so row
/// coloring uses the same predicate as search and structured filters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColorRule {
    pub name: String,
    pub condition: crate::condition::Node,
    /// Normalized "#rrggbb" or "#rgb".
    pub color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IocEntry {
    pub flag: String,
    pub tag: String,
    pub query: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoadProjectResponse {
    pub project: ProjectSummary,
    /// Data column ids in source order. For legacy projects these are the
    /// original column names.
    pub columns: Vec<String>,
    /// Display and source metadata for `columns`.
    pub columns_meta: Vec<ColumnMeta>,
    pub hidden_columns: Vec<String>,
    pub column_max_chars: HashMap<String, usize>,
    pub column_kinds: HashMap<String, String>,
    pub iocs: Vec<IocEntry>,
    pub color_rules: Vec<ColorRule>,
    pub initial_rows: Vec<ProjectRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlagEntry {
    pub flag: String,
    pub memo: Option<String>,
    /// User-defined tags. serde's default keeps entries written before F19 readable.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Independent bookmark flag. Old entries deserialize as `false`.
    #[serde(default)]
    pub bookmarked: bool,
}
