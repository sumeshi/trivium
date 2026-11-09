use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::Context as _;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::AppError;
use crate::project_io::read_project_dataframe;
use crate::store::ProjectsStore;
use crate::value_utils::{anyvalue_to_json, anyvalue_to_search_string};

#[derive(Debug, Deserialize)]
pub struct DiffProjectsPayload {
    #[serde(rename = "projectIdA")]
    pub project_id_a: Uuid,
    #[serde(rename = "projectIdB")]
    pub project_id_b: Uuid,
    #[serde(rename = "keyColumns")]
    pub key_columns: Vec<String>,
    #[serde(default)]
    pub destination: Option<String>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct DiffEntry {
    /// key_columns の各列名 -> 代表値（serde_json::Value）。
    /// 同一キー文字列の行で型が違う場合、最初に見つかった行の値を採用する。
    pub key: HashMap<String, serde_json::Value>,
    pub count_a: usize,
    pub count_b: usize,
    /// "only_a" | "only_b" | "changed" | "same"
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct DiffResult {
    pub entries: Vec<DiffEntry>,
    /// status ごとの件数: {"only_a": n, "only_b": n, "changed": n, "same": n}
    pub status_counts: HashMap<String, usize>,
}

/// Compares two projects by key columns, preserving duplicate counts.
pub fn diff_projects(
    store: &ProjectsStore,
    payload: DiffProjectsPayload,
) -> Result<DiffResult, AppError> {
    let requested: Vec<String> = payload
        .key_columns
        .iter()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect();
    if requested.is_empty() {
        return Err(AppError::Message("Key columns must not be empty.".into()));
    }

    let df_a = load_frame(store, &payload.project_id_a)?;
    let df_b = load_frame(store, &payload.project_id_b)?;
    let names_a = resolve_key_columns(&df_a, &requested, "A")?;
    let names_b = resolve_key_columns(&df_b, &requested, "B")?;

    let accum_a = count_keys(&df_a, &names_a);
    let accum_b = count_keys(&df_b, &names_b);

    let counts = accum_a.counts.keys().chain(accum_b.counts.keys());
    let mut keys: Vec<String> = counts.cloned().collect();
    keys.sort();
    keys.dedup();

    let mut keyed: Vec<(String, DiffEntry)> = Vec::with_capacity(keys.len());
    let mut status_counts: HashMap<String, usize> = ["only_a", "only_b", "changed", "same"]
        .into_iter()
        .map(|status| (status.to_string(), 0))
        .collect();
    for key in keys {
        let count_a = *accum_a.counts.get(&key).unwrap_or(&0);
        let count_b = *accum_b.counts.get(&key).unwrap_or(&0);
        let status = match (count_a, count_b) {
            (a, b) if a > 0 && b > 0 && a == b => "same",
            (a, b) if a > 0 && b > 0 => "changed",
            (a, _) if a > 0 => "only_a",
            _ => "only_b",
        };
        let values = accum_a.values.get(&key).or(accum_b.values.get(&key));
        let mut key_map: HashMap<String, Value> = HashMap::new();
        for (name, value) in names_a.iter().zip(values.into_iter().flatten()) {
            key_map.insert(name.clone(), value.clone());
        }
        *status_counts.entry(status.to_string()).or_insert(0) += 1;
        keyed.push((
            key,
            DiffEntry {
                key: key_map,
                count_a,
                count_b,
                status: status.to_string(),
            },
        ));
    }
    keyed.sort_by(|left, right| {
        status_rank(&left.1.status)
            .cmp(&status_rank(&right.1.status))
            .then_with(|| left.0.cmp(&right.0))
    });

    if let Some(destination) = payload.destination {
        write_diff_csv(&keyed, &names_a, &destination)?;
    }

    Ok(DiffResult {
        entries: keyed.into_iter().map(|(_, entry)| entry).collect(),
        status_counts,
    })
}

#[derive(Default)]
struct KeyAccum {
    counts: HashMap<String, usize>,
    values: HashMap<String, Vec<Value>>,
}

fn load_frame(store: &ProjectsStore, id: &Uuid) -> Result<DataFrame, AppError> {
    let meta = store
        .find(id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let parquet_path = store.project_dir(&meta.id).join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    read_project_dataframe(&parquet_path).map_err(AppError::from)
}

/// Maps requested key names (case-insensitive) to the frame's actual column names.
fn resolve_key_columns(
    df: &DataFrame,
    requested: &[String],
    label: &str,
) -> Result<Vec<String>, AppError> {
    let mut lookup: HashMap<String, String> = HashMap::new();
    for name in df.get_column_names() {
        lookup.insert(name.to_lowercase(), name.to_string());
    }
    let mut resolved = Vec::with_capacity(requested.len());
    for name in requested {
        let actual = lookup.get(&name.to_lowercase()).ok_or_else(|| {
            AppError::Message(format!(
                "Key column '{}' not found in project {}.",
                name, label
            ))
        })?;
        resolved.push(actual.clone());
    }
    Ok(resolved)
}

/// Counts rows per key string and records the first row's values per key.
fn count_keys(df: &DataFrame, names: &[String]) -> KeyAccum {
    let series: Vec<Option<&Series>> = names.iter().map(|name| df.column(name).ok()).collect();
    let mut accum = KeyAccum::default();
    for row_idx in 0..df.height() {
        let mut parts: Vec<String> = Vec::with_capacity(names.len());
        let mut values: Vec<Value> = Vec::with_capacity(names.len());
        for column in &series {
            let value = column.and_then(|series| series.get(row_idx).ok());
            let text = value.as_ref().and_then(anyvalue_to_search_string);
            parts.push(text.map(|text| text.trim().to_string()).unwrap_or_default());
            values.push(value.as_ref().map(anyvalue_to_json).unwrap_or(Value::Null));
        }
        let key = parts.join("\u{1}");
        *accum.counts.entry(key.clone()).or_insert(0) += 1;
        if let Entry::Vacant(slot) = accum.values.entry(key) {
            slot.insert(values);
        }
    }
    accum
}

fn status_rank(status: &str) -> usize {
    match status {
        "only_a" => 0,
        "only_b" => 1,
        "changed" => 2,
        _ => 3,
    }
}

fn json_to_csv(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::String(text) => text.clone(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}

fn write_diff_csv(
    entries: &[(String, DiffEntry)],
    key_names: &[String],
    destination: &str,
) -> Result<(), AppError> {
    let path = PathBuf::from(destination);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create diff output dir {:?}", parent))
                .map_err(AppError::from)?;
        }
    }
    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(&path)
        .with_context(|| format!("failed to create diff CSV {}", destination))
        .map_err(AppError::from)?;
    let mut header = vec![
        "status".to_string(),
        "count_a".to_string(),
        "count_b".to_string(),
    ];
    header.extend(key_names.iter().cloned());
    writer
        .write_record(&header)
        .with_context(|| format!("failed to write diff CSV header {}", destination))
        .map_err(AppError::from)?;
    for (_, entry) in entries {
        let mut record: Vec<String> = vec![
            entry.status.clone(),
            entry.count_a.to_string(),
            entry.count_b.to_string(),
        ];
        for name in key_names {
            record.push(json_to_csv(entry.key.get(name).unwrap_or(&Value::Null)));
        }
        writer
            .write_record(&record)
            .with_context(|| format!("failed to write diff CSV row {}", destination))
            .map_err(AppError::from)?;
    }
    writer
        .flush()
        .with_context(|| format!("failed to flush diff CSV {}", destination))
        .map_err(AppError::from)
}
