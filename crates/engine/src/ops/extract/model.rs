use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{formats::FormatDefinition, ops::query::QuerySpec};

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, ts_rs::TS)]
#[serde(rename_all = "lowercase")]
pub enum ExtractScope {
    All,
    #[default]
    Filtered,
}

#[derive(Debug, Clone, Deserialize, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ExtractPayload {
    pub project_id: Uuid,
    pub columns: Vec<String>,
    pub format: FormatDefinition,
    #[serde(default)]
    pub scope: ExtractScope,
    #[serde(flatten)]
    pub spec: QuerySpec,
}

#[derive(Debug, Clone, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ExtractResult {
    pub result_id: Uuid,
    pub project_id: Uuid,
    pub format: FormatDefinition,
    pub columns: Vec<String>,
    pub scope: ExtractScope,
    pub source_rows: usize,
    #[ts(type = "number")]
    pub total_occurrences: u64,
    pub matched_rows: usize,
    pub unique_values: usize,
    pub has_classification: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, ts_rs::TS)]
pub struct ExtractValue {
    pub value: String,
    #[ts(type = "number")]
    pub count: u64,
    #[ts(type = "number")]
    pub rows: u64,
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ExtractPagePayload {
    pub result_id: Uuid,
    #[serde(default)]
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, ts_rs::TS)]
pub struct ExtractPage {
    pub rows: Vec<ExtractValue>,
    pub total: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ExtractSourcesPayload {
    pub result_id: Uuid,
    pub value: String,
    #[serde(default)]
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ExtractSource {
    pub row_index: usize,
    pub column: String,
    /// UTF-8 byte offsets into the original decoded cell, not file bytes.
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ExtractSources {
    pub rows: Vec<ExtractSource>,
    pub offset: usize,
    pub has_more: bool,
}

#[derive(Debug, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct ExportExtractPayload {
    pub result_id: Uuid,
    pub destination: String,
    #[serde(default)]
    pub allow_overwrite: bool,
}
