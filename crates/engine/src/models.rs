use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

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
    pub columns: Vec<String>,
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
