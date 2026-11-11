//! Shared helpers for engine integration tests after the column-id change.
//!
//! Tests keep referring to source names and resolve them through the project
//! metadata, so the expectations stay about data rather than id formatting.

#![allow(dead_code)]

use std::collections::HashMap;

use uuid::Uuid;

use trivium_engine::models::ProjectRow;
use trivium_engine::ops::projects::{self, ProjectRequest};
use trivium_engine::store::ProjectsStore;

/// Maps original source name (CSV header / JSON Pointer) to internal column id.
pub fn column_ids(store: &ProjectsStore, project_id: Uuid) -> HashMap<String, String> {
    let loaded = projects::load_project(
        store,
        ProjectRequest {
            project_id,
        },
    )
    .expect("failed to load project columns");
    loaded
        .columns_meta
        .into_iter()
        .map(|column| (column.source, column.id))
        .collect()
}

/// Source name to id; panics when the fixture does not contain the column.
pub fn id(ids: &HashMap<String, String>, source: &str) -> String {
    ids.get(source)
        .unwrap_or_else(|| panic!("source column not found: {source}"))
        .clone()
}

/// Builds a column-scoped search expression with an internal id.
pub fn search(ids: &HashMap<String, String>, column: &str, expression: &str) -> String {
    format!("{}:{}", id(ids, column), expression)
}

/// Exact text of one cell, or `None` for null/missing.
pub fn cell_text<'a>(row: &'a ProjectRow, column: &str) -> Option<&'a str> {
    row.cells
        .get(column)
        .and_then(|cell| cell.text.as_deref())
}
