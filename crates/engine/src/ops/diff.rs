use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::AppError;
use crate::export::atomic::{
    ensure_destination_is_not_managed, ensure_destination_is_not_source, write_atomically,
};
use crate::jobs::JobContext;
use crate::models::ProjectMeta;
use crate::project_io::{read_project_schema_columns, ProjectBatchReader};
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
    diff_projects_with_job(store, payload, &JobContext::none())
}

pub fn diff_projects_with_job(
    store: &ProjectsStore,
    payload: DiffProjectsPayload,
    job: &JobContext,
) -> Result<DiffResult, AppError> {
    job.check_cancelled()?;
    let requested: Vec<String> = payload
        .key_columns
        .iter()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect();
    if requested.is_empty() {
        return Err(AppError::Message("Key columns must not be empty.".into()));
    }

    let (meta_a, mut reader_a, names_a) =
        load_keys(store, &payload.project_id_a, &requested, "A", job)?;
    let accum_a = count_keys(&mut reader_a, &names_a, job)?;
    drop(reader_a);
    let (meta_b, mut reader_b, names_b) =
        load_keys(store, &payload.project_id_b, &requested, "B", job)?;
    let accum_b = count_keys(&mut reader_b, &names_b, job)?;
    drop(reader_b);

    let counts = accum_a.counts.keys().chain(accum_b.counts.keys());
    let mut keys: Vec<String> = counts.cloned().collect();
    job.check_cancelled()?;
    keys.sort();
    keys.dedup();

    let mut keyed: Vec<(String, DiffEntry)> = Vec::with_capacity(keys.len());
    let mut status_counts: HashMap<String, usize> = ["only_a", "only_b", "changed", "same"]
        .into_iter()
        .map(|status| (status.to_string(), 0))
        .collect();
    for (index, key) in keys.into_iter().enumerate() {
        if index % 1024 == 0 {
            job.check_cancelled()?;
        }
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
    job.check_cancelled()?;
    keyed.sort_by(|left, right| {
        status_rank(&left.1.status)
            .cmp(&status_rank(&right.1.status))
            .then_with(|| left.0.cmp(&right.0))
    });

    if let Some(destination) = payload.destination {
        let path = PathBuf::from(destination);
        let validate = || {
            ensure_destination_is_not_managed(&path, store)?;
            ensure_destination_is_not_source(&path, &meta_a)?;
            ensure_destination_is_not_source(&path, &meta_b)
        };
        write_diff_csv(&keyed, &names_a, &path, job, validate)?;
    }

    job.check_cancelled()?;
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

fn load_keys(
    store: &ProjectsStore,
    id: &Uuid,
    requested: &[String],
    label: &str,
    job: &JobContext,
) -> Result<(ProjectMeta, ProjectBatchReader, Vec<String>), AppError> {
    job.check_cancelled()?;
    let meta = store
        .try_find(id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let path = store.project_dir(&meta.id).join("data.parquet");
    if !path.exists() {
        return Err(AppError::Message("Project data file missing.".into()));
    }
    let schema = read_project_schema_columns(&path).map_err(AppError::from)?;
    let lookup = schema
        .iter()
        .map(|name| (name.to_lowercase(), name))
        .collect::<HashMap<_, _>>();
    let names = requested
        .iter()
        .map(|name| {
            lookup
                .get(&name.to_lowercase())
                .map(|actual| (*actual).clone())
                .ok_or_else(|| {
                    AppError::Message(format!("Column '{}' not found in project {}.", name, label))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut projection = names.clone();
    projection.sort();
    projection.dedup();
    let reader = ProjectBatchReader::open(&path, &projection).map_err(AppError::from)?;
    Ok((meta, reader, names))
}

/// Counts rows per key string and records the first row's values per key.
fn count_keys(
    reader: &mut ProjectBatchReader,
    names: &[String],
    job: &JobContext,
) -> Result<KeyAccum, AppError> {
    let mut accum = KeyAccum::default();
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch.map_err(AppError::from)?;
        let series = names
            .iter()
            .map(|name| batch.frame.column(name).expect("validated key column"))
            .collect::<Vec<_>>();
        for row_idx in 0..batch.frame.height() {
            if row_idx % 1024 == 0 {
                job.check_cancelled()?;
            }
            let mut key = String::new();
            let mut values = Vec::with_capacity(names.len());
            for column in &series {
                let value = column.get(row_idx).ok();
                let text = value
                    .as_ref()
                    .and_then(anyvalue_to_search_string)
                    .unwrap_or_default();
                let text = text.trim();
                // Length framing keeps separator characters inside values
                // distinct while preserving the legacy trim/null semantics.
                key.push_str(&text.len().to_string());
                key.push(':');
                key.push_str(text);
                values.push(value.as_ref().map(anyvalue_to_json).unwrap_or(Value::Null));
            }
            *accum.counts.entry(key.clone()).or_insert(0) += 1;
            if let Entry::Vacant(slot) = accum.values.entry(key) {
                slot.insert(values);
            }
        }
        job.report_progress(
            (batch.offset + batch.frame.height()) as u64,
            Some(reader.total_rows() as u64),
            Some("Comparing project keys".into()),
        );
    }
    job.check_cancelled()?;
    Ok(accum)
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
    destination: &Path,
    job: &JobContext,
    validate: impl Fn() -> Result<(), AppError>,
) -> Result<(), AppError> {
    write_atomically(destination, job, validate, |_, output| {
        let mut writer = csv::WriterBuilder::new()
            .has_headers(true)
            .from_writer(output);
        let mut header = vec![
            "status".to_string(),
            "count_a".to_string(),
            "count_b".to_string(),
        ];
        header.extend(key_names.iter().cloned());
        writer
            .write_record(&header)
            .context("failed to write diff CSV header")?;
        for (index, (_, entry)) in entries.iter().enumerate() {
            if index % 1024 == 0 {
                job.check_cancelled()?;
            }
            let mut record = vec![
                entry.status.clone(),
                entry.count_a.to_string(),
                entry.count_b.to_string(),
            ];
            for name in key_names {
                record.push(json_to_csv(entry.key.get(name).unwrap_or(&Value::Null)));
            }
            writer
                .write_record(&record)
                .context("failed to write diff CSV row")?;
        }
        writer.flush().context("failed to flush diff CSV")?;
        Ok(())
    })
}
