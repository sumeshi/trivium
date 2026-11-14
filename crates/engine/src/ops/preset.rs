use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::condition::Node;
use crate::error::AppError;
use crate::export::atomic::{
    ensure_destination_is_not_managed, ensure_destination_is_not_source, write_atomically,
};
use crate::jobs::JobContext;
use crate::models::{ColumnMeta, ProjectMeta};
use crate::project_io::{read_project_schema_columns, ProjectColumns};
use crate::store::ProjectsStore;

#[derive(Debug, Serialize, Deserialize)]
pub struct PresetFile {
    pub version: u32,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub condition: Option<Node>,
    #[serde(default, rename = "hiddenColumns")]
    pub hidden_columns: Vec<String>,
    #[serde(default, rename = "sortKey")]
    pub sort_key: Option<String>,
    #[serde(default, rename = "sortDirection")]
    pub sort_direction: Option<String>,
    #[serde(
        default,
        rename = "columnOrder",
        skip_serializing_if = "Option::is_none"
    )]
    pub column_order: Option<Vec<String>>,
    #[serde(
        default,
        rename = "columnWidths",
        skip_serializing_if = "Option::is_none"
    )]
    pub column_widths: Option<HashMap<String, f64>>,
    /// Source identities make v2 presets portable across reordered columns.
    #[serde(default)]
    pub columns: Vec<ColumnMeta>,
}

#[derive(Debug, Deserialize)]
pub struct SavePresetPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub name: Option<String>,
    #[serde(default)]
    pub condition: Option<Node>,
    #[serde(default, rename = "hiddenColumns")]
    pub hidden_columns: Vec<String>,
    #[serde(default, rename = "sortKey")]
    pub sort_key: Option<String>,
    #[serde(default, rename = "sortDirection")]
    pub sort_direction: Option<String>,
    #[serde(default, rename = "columnOrder")]
    pub column_order: Option<Vec<String>>,
    #[serde(default, rename = "columnWidths")]
    pub column_widths: Option<HashMap<String, f64>>,
    pub destination: String,
}

#[derive(Debug, Deserialize)]
pub struct LoadPresetPayload {
    #[serde(default, rename = "projectId")]
    pub project_id: Option<Uuid>,
    /// Path to the preset file to read.
    pub source: String,
}

#[derive(Debug, Serialize)]
pub struct PresetData {
    pub name: Option<String>,
    pub condition: Option<Node>,
    #[serde(rename = "hiddenColumns")]
    pub hidden_columns: Vec<String>,
    #[serde(rename = "sortKey")]
    pub sort_key: Option<String>,
    #[serde(rename = "sortDirection")]
    pub sort_direction: Option<String>,
    #[serde(rename = "columnOrder", skip_serializing_if = "Option::is_none")]
    pub column_order: Option<Vec<String>>,
    #[serde(rename = "columnWidths", skip_serializing_if = "Option::is_none")]
    pub column_widths: Option<HashMap<String, f64>>,
    /// References in rules, columns, layout or sorting that could not be
    /// resolved. Empty when no project id was supplied.
    #[serde(default)]
    pub unknown_columns: Vec<String>,
}

/// Collects every column referenced by a condition tree, in traversal order.
pub fn extract_columns(node: &Node) -> Vec<String> {
    node.columns().into_iter().map(str::to_string).collect()
}

/// Writes the current view as a portable preset file.
pub fn save_preset(store: &ProjectsStore, payload: SavePresetPayload) -> Result<(), AppError> {
    let meta = store
        .try_find(&payload.project_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;

    let preset = PresetFile {
        version: 2,
        name: payload.name,
        condition: payload.condition,
        hidden_columns: payload.hidden_columns,
        sort_key: payload.sort_key,
        sort_direction: payload.sort_direction,
        column_order: payload.column_order,
        column_widths: payload.column_widths,
        columns: project_columns(store, &meta)?,
    };
    validate_layout(&preset)?;
    let data = serde_json::to_vec_pretty(&preset)
        .with_context(|| "failed to serialize preset")
        .map_err(AppError::from)?;

    let destination = PathBuf::from(&payload.destination);
    write_atomically(
        &destination,
        &JobContext::none(),
        || {
            ensure_destination_is_not_source(&destination, &meta)?;
            ensure_destination_is_not_managed(&destination, store)
        },
        |_, writer| {
            writer
                .write_all(&data)
                .context("failed to write preset")
                .map_err(AppError::from)
        },
    )
}

/// Reads a preset file and reports columns unknown to the selected project.
pub fn load_preset(
    store: &ProjectsStore,
    payload: LoadPresetPayload,
) -> Result<PresetData, AppError> {
    let path = PathBuf::from(&payload.source);
    let data = fs::read(&path)
        .with_context(|| format!("failed to read preset {}", payload.source))
        .map_err(AppError::from)?;
    let mut preset: PresetFile = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse preset {}", payload.source))
        .map_err(AppError::from)?;
    if !matches!(preset.version, 1 | 2) {
        return Err(AppError::Message("Unsupported preset version.".into()));
    }
    validate_layout(&preset)?;

    let unknown_columns = match payload.project_id {
        Some(id) => {
            let meta = store
                .try_find(&id)
                .map_err(AppError::from)?
                .ok_or_else(|| AppError::Message("Project not found.".into()))?;
            resolve_columns(&mut preset, &project_columns(store, &meta)?)?
        }
        None => Vec::new(),
    };

    Ok(PresetData {
        name: preset.name,
        condition: preset.condition,
        hidden_columns: preset.hidden_columns,
        sort_key: preset.sort_key,
        sort_direction: preset.sort_direction,
        column_order: preset.column_order,
        column_widths: preset.column_widths,
        unknown_columns,
    })
}

fn project_columns(store: &ProjectsStore, meta: &ProjectMeta) -> Result<Vec<ColumnMeta>, AppError> {
    let parquet_path = store.project_dir(&meta.id).join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let schema = read_project_schema_columns(&parquet_path).map_err(AppError::from)?;
    Ok(ProjectColumns::from_meta_and_names(meta, &schema)
        .columns()
        .to_vec())
}

fn resolve_columns(
    preset: &mut PresetFile,
    target: &[ColumnMeta],
) -> Result<Vec<String>, AppError> {
    let mut references = preset
        .condition
        .as_ref()
        .map(extract_columns)
        .unwrap_or_default();
    references.extend(preset.hidden_columns.iter().cloned());
    references.extend(preset.sort_key.iter().cloned());
    references.extend(preset.column_order.iter().flatten().cloned());
    references.extend(
        preset
            .column_widths
            .iter()
            .flat_map(|widths| widths.keys())
            .cloned(),
    );
    let same_sources = preset
        .columns
        .iter()
        .map(|column| &column.source)
        .eq(target.iter().map(|column| &column.source));
    let mut ids = HashSet::new();
    if preset.version == 2
        && preset
            .columns
            .iter()
            .any(|column| !ids.insert(column.id.to_lowercase()))
    {
        return Err(AppError::Message("Invalid preset column mapping.".into()));
    }
    let mut mapping = HashMap::new();
    let mut missing = Vec::new();
    for reference in references {
        let key = reference.to_lowercase();
        let resolved = if preset.version == 1 {
            // Old presets use source positions, as before.
            target
                .iter()
                .find(|column| column.id == reference)
                .or_else(|| target.iter().find(|column| column.id.to_lowercase() == key))
        } else if let Some(index) = preset
            .columns
            .iter()
            .position(|column| column.id.to_lowercase() == key)
        {
            let source = &preset.columns[index].source;
            if same_sources {
                Some(&target[index])
            } else {
                let mut matches = target.iter().filter(|column| &column.source == source);
                let first = matches.next();
                if first.is_none()
                    || matches.next().is_some()
                    || preset
                        .columns
                        .iter()
                        .filter(|column| &column.source == source)
                        .count()
                        != 1
                {
                    return Err(AppError::Message(format!(
                        "Preset column is missing or ambiguous: {source}"
                    )));
                }
                first
            }
        } else {
            None
        };
        if let Some(column) = resolved {
            mapping.insert(reference, column.id.clone());
        } else {
            missing.push(reference);
        }
    }
    let remap = |column: &mut String| {
        if let Some(id) = mapping.get(column.as_str()) {
            column.clone_from(id);
        }
    };
    if let Some(condition) = &mut preset.condition {
        remap_condition(condition, &remap);
    }
    for column in &mut preset.hidden_columns {
        remap(column);
    }
    if let Some(column) = &mut preset.sort_key {
        remap(column);
    }
    if let Some(order) = &mut preset.column_order {
        order.iter_mut().for_each(&remap);
    }
    if let Some(widths) = &mut preset.column_widths {
        *widths = std::mem::take(widths)
            .into_iter()
            .map(|(mut column, width)| {
                remap(&mut column);
                (column, width)
            })
            .collect();
    }
    missing.sort();
    missing.dedup();
    Ok(missing)
}

fn validate_layout(preset: &PresetFile) -> Result<(), AppError> {
    let unique_columns = |columns: Vec<&str>| {
        let mut seen = HashSet::new();
        columns
            .into_iter()
            .all(|column| !column.trim().is_empty() && seen.insert(column.to_lowercase()))
    };
    if let Some(order) = &preset.column_order {
        if !unique_columns(order.iter().map(String::as_str).collect()) {
            return Err(AppError::Message("Invalid preset column order.".into()));
        }
    }
    if let Some(widths) = &preset.column_widths {
        if !unique_columns(widths.keys().map(String::as_str).collect())
            || widths
                .values()
                .any(|width| !width.is_finite() || *width <= 0.0)
        {
            return Err(AppError::Message("Invalid preset column widths.".into()));
        }
    }
    Ok(())
}

fn remap_condition(node: &mut Node, remap: &impl Fn(&mut String)) {
    match node {
        Node::Condition(condition) => remap(&mut condition.column),
        Node::Group(group) => {
            for child in &mut group.children {
                remap_condition(child, remap);
            }
        }
    }
}
