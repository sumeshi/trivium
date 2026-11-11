use std::collections::HashMap;

use chrono::{DateTime, Utc};
use polars::prelude::DataFrame;
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

/// Result of reading one source file. The frame uses internal column ids, the
/// metadata records the source header or JSON Pointer, and imported flags are
/// separate from the data columns.
pub struct ImportedData {
    pub frame: DataFrame,
    pub columns: Vec<ColumnMeta>,
    pub flags: HashMap<usize, FlagEntry>,
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
    /// Original file name as selected by the user. Legacy projects and
    /// hand-built fixtures have none.
    #[serde(default)]
    pub source_file_name: Option<String>,
    /// `csv`, `json`, `jsonl`, or `ndjson` when known.
    #[serde(default)]
    pub source_format: Option<String>,
    /// Size of the imported bytes. The recorded hash covers the same bytes.
    #[serde(default)]
    pub source_size: Option<u64>,
    /// Absolute path of the evidence file as it was when the project was
    /// created. Export refuses to overwrite this file. Legacy projects and
    /// hand-built fixtures have none.
    #[serde(default)]
    pub source_path: Option<String>,
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

/// Current on-disk version of the per-project view state.
pub const VIEW_STATE_VERSION: u32 = 1;

/// Rebuildable view settings for one project.
///
/// This file is independent from annotations: a missing, broken, or newer
/// view state must never prevent the evidence or annotations from loading.
/// `condition` keeps the frontend editor tree as-is; the engine does not
/// interpret it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ViewState {
    pub version: u32,
    #[serde(default)]
    pub search: String,
    #[serde(default, rename = "searchColumns")]
    pub search_columns: Vec<String>,
    #[serde(default)]
    pub condition: Option<Value>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: String,
    #[serde(default, rename = "bookmarkedOnly")]
    pub bookmarked_only: bool,
    #[serde(default, rename = "sortKey")]
    pub sort_key: Option<String>,
    #[serde(default = "default_sort_direction", rename = "sortDirection")]
    pub sort_direction: String,
    #[serde(default, rename = "hiddenColumns")]
    pub hidden_columns: Vec<String>,
    #[serde(default, rename = "columnWidths")]
    pub column_widths: HashMap<String, f64>,
    #[serde(default, rename = "logicalRow")]
    pub logical_row: usize,
}

fn default_sort_direction() -> String {
    "asc".to_string()
}

impl Default for ViewState {
    fn default() -> Self {
        Self {
            version: VIEW_STATE_VERSION,
            search: String::new(),
            search_columns: Vec::new(),
            condition: None,
            flag_filter: "all".to_string(),
            bookmarked_only: false,
            sort_key: None,
            sort_direction: default_sort_direction(),
            hidden_columns: Vec::new(),
            column_widths: HashMap::new(),
            logical_row: 0,
        }
    }
}

/// One cell as the frontend should read it.
///
/// For list responses `text` is a preview truncated to a fixed number of
/// characters; `truncated` and `length` describe the original value. The
/// source Parquet data is never truncated. `get_cell` returns the full text in
/// the same shape with `truncated = false`.
///
/// `kind` is the column-level type. Integers are always passed as decimal
/// strings, so a long integer never travels through a JavaScript `Number`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Cell {
    /// Preview text for list responses; full text from `get_cell`. `None` for
    /// null/missing.
    pub text: Option<String>,
    /// "string" | "int" | "float" | "bool" | "null" | "missing"
    pub kind: String,
    /// True when `text` is a prefix of the original value.
    #[serde(default)]
    pub truncated: bool,
    /// Original character count. `None` for null/missing.
    #[serde(default)]
    pub length: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectRow {
    pub row_index: usize,
    /// Exact per-column cell DTOs. This is the only row-value map; display,
    /// copy and filter values all come from here.
    #[serde(default)]
    pub cells: HashMap<String, Cell>,
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

impl FlagEntry {
    /// A row is flagged only by a non-empty flag. Memo/tag/bookmark-only rows
    /// are annotated but not flagged.
    pub fn is_flagged(&self) -> bool {
        !self.flag.trim().is_empty()
    }

    /// True when any independent annotation field is present.
    pub fn is_annotated(&self) -> bool {
        self.is_flagged()
            || self
                .memo
                .as_deref()
                .is_some_and(|memo| !memo.trim().is_empty())
            || !self.tags.is_empty()
            || self.bookmarked
    }
}

/// Result of a single-row annotation update.
///
/// The updated row data is not included: the frontend already has it and
/// applies only the annotation fields to its cache.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateFlagResponse {
    /// `None` when the patch removed the last annotation on the row.
    pub entry: Option<FlagEntry>,
    /// Process-local monotonic revision for the project.
    pub revision: u64,
    /// Whole-project count after applying this update.
    pub flagged_records: usize,
}
