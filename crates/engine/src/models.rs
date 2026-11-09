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
    pub iocs: Vec<IocEntry>,
    pub initial_rows: Vec<ProjectRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlagEntry {
    pub flag: String,
    pub memo: Option<String>,
}
