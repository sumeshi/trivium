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
