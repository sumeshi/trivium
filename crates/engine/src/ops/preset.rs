use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::condition::Node;
use crate::error::AppError;
use crate::project_io::read_project_dataframe;
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
    /// Columns in the condition that the project does not have. Empty when no
    /// project id was supplied.
    #[serde(default)]
    pub unknown_columns: Vec<String>,
}

/// Collects every column referenced by a condition tree, in traversal order.
pub fn extract_columns(node: &Node) -> Vec<String> {
    node.columns().into_iter().map(str::to_string).collect()
}

/// Writes the current view as a portable preset file.
pub fn save_preset(store: &ProjectsStore, payload: SavePresetPayload) -> Result<(), AppError> {
    if store.find(&payload.project_id).is_none() {
        return Err(AppError::Message("Project not found.".into()));
    }

    let preset = PresetFile {
        version: 1,
        name: payload.name,
        condition: payload.condition,
        hidden_columns: payload.hidden_columns,
        sort_key: payload.sort_key,
        sort_direction: payload.sort_direction,
    };
    let data = serde_json::to_vec_pretty(&preset)
        .with_context(|| "failed to serialize preset")
        .map_err(AppError::from)?;

    let destination = PathBuf::from(&payload.destination);
    if let Some(parent) = destination.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create preset output dir {:?}", parent))
                .map_err(AppError::from)?;
        }
    }
    fs::write(&destination, data)
        .with_context(|| format!("failed to write preset {}", payload.destination))
        .map_err(AppError::from)
}

/// Reads a preset file and reports columns unknown to the selected project.
pub fn load_preset(store: &ProjectsStore, payload: LoadPresetPayload) -> Result<PresetData, AppError> {
    let path = PathBuf::from(&payload.source);
    let data = fs::read(&path)
        .with_context(|| format!("failed to read preset {}", payload.source))
        .map_err(AppError::from)?;
    let preset: PresetFile = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse preset {}", payload.source))
        .map_err(AppError::from)?;
    if preset.version != 1 {
        return Err(AppError::Message("Unsupported preset version.".into()));
    }

    let unknown_columns = match payload.project_id {
        Some(id) => {
            let meta = store
                .find(&id)
                .ok_or_else(|| AppError::Message("Project not found.".into()))?;
            match preset.condition.as_ref() {
                Some(condition) => unknown_columns(store, &meta.id, condition)?,
                None => Vec::new(),
            }
        }
        None => Vec::new(),
    };

    Ok(PresetData {
        name: preset.name,
        condition: preset.condition,
        hidden_columns: preset.hidden_columns,
        sort_key: preset.sort_key,
        sort_direction: preset.sort_direction,
        unknown_columns,
    })
}

fn unknown_columns(
    store: &ProjectsStore,
    project_id: &Uuid,
    condition: &Node,
) -> Result<Vec<String>, AppError> {
    let parquet_path = store.project_dir(project_id).join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let known: HashSet<String> = df
        .get_column_names()
        .iter()
        .map(|name| name.to_lowercase())
        .collect();

    let mut missing: Vec<String> = extract_columns(condition)
        .into_iter()
        .filter(|column| !known.contains(&column.to_lowercase()))
        .collect();
    missing.sort();
    missing.dedup();
    Ok(missing)
}
