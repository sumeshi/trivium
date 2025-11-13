use std::collections::HashMap;
use std::fs::{self, File};
use std::path::PathBuf;

use anyhow::Context;
use chrono::Utc;
use polars::prelude::*;
use serde::Deserialize;
use tauri::State;
use uuid::Uuid;

use crate::{
    error::AppError,
    flags::normalize_flag_value,
    ioc::{apply_iocs_to_rows, load_ioc_entries},
    models::{FlagEntry, LoadProjectResponse, ProjectMeta, ProjectRow, ProjectSummary},
    project_io::{read_project_dataframe, write_project_dataframe},
    state::AppState,
    storage::{
        clear_ioc_flag_cache, clear_searchable_cache, compute_column_max_chars,
        load_column_metrics, load_flags, save_column_metrics, save_flags,
    },
    value_utils::anyvalue_to_search_string,
};

use super::{utils::collect_row_record, DEFAULT_PAGE_SIZE};

const COLUMN_METRICS_FILE: &str = "column_max_chars.json";

fn materialize_rows(
    df: &DataFrame,
    columns: &[String],
    row_indices: impl Iterator<Item = usize>,
    flags: &HashMap<usize, FlagEntry>,
) -> Vec<ProjectRow> {
    let mut rows = Vec::new();
    for row_idx in row_indices {
        let record = collect_row_record(df, columns, row_idx);
        let flag_entry = flags.get(&row_idx);
        rows.push(ProjectRow {
            row_index: row_idx,
            data: record,
            flag: flag_entry
                .as_ref()
                .map(|entry| normalize_flag_value(&entry.flag))
                .unwrap_or_default(),
            memo: flag_entry.and_then(|entry| entry.memo.clone()),
        });
    }
    rows
}

#[derive(Debug, Deserialize)]
pub struct CreateProjectPayload {
    pub path: String,
    pub description: Option<String>,
}

/// Lists saved projects ordered by creation time (newest first).
#[tauri::command]
pub fn list_projects(state: State<AppState>) -> Result<Vec<ProjectSummary>, String> {
    println!("[debug] list_projects called");
    let metas = state.projects.all();
    let mut result = Vec::with_capacity(metas.len());
    for meta in metas {
        result.push(ProjectSummary { meta: meta.clone() });
    }
    result.sort_by(|a, b| b.meta.created_at.cmp(&a.meta.created_at));
    Ok(result)
}

/// Creates a new project from a CSV file and persists metadata plus optional flags.
#[tauri::command]
pub fn create_project(
    state: State<AppState>,
    payload: CreateProjectPayload,
) -> Result<ProjectSummary, String> {
    let source_path = PathBuf::from(&payload.path);
    if !source_path.exists() {
        return Err(AppError::Message("Selected file no longer exists.".into()).into());
    }

    let file = File::open(&source_path)
        .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let mut df = CsvReader::new(file)
        .finish()
        .map_err(|_| AppError::Message("Failed to parse the CSV data.".into()))?;

    let mut imported_flags: HashMap<usize, FlagEntry> = HashMap::new();
    let has_safe = df.get_column_names().iter().any(|c| c == &"trivium-safe");
    let has_suspicious = df
        .get_column_names()
        .iter()
        .any(|c| c == &"trivium-suspicious");
    let has_critical = df
        .get_column_names()
        .iter()
        .any(|c| c == &"trivium-critical");
    let has_memo = df.get_column_names().iter().any(|c| c == &"trivium-memo");
    if has_safe || has_suspicious || has_critical || has_memo {
        let safe_col = if has_safe {
            df.column("trivium-safe").ok()
        } else {
            None
        };
        let suspicious_col = if has_suspicious {
            df.column("trivium-suspicious").ok()
        } else {
            None
        };
        let critical_col = if has_critical {
            df.column("trivium-critical").ok()
        } else {
            None
        };
        let memo_col = if has_memo {
            df.column("trivium-memo").ok()
        } else {
            None
        };

        let height = df.height();
        for row_idx in 0..height {
            let mut best_flag = String::new();
            if let Some(series) = critical_col.as_ref() {
                if let Ok(v) = series.get(row_idx) {
                    if let Some(text) = anyvalue_to_search_string(&v) {
                        if text != "0" && !text.is_empty() {
                            best_flag = "critical".to_string();
                        }
                    }
                }
            }
            if best_flag.is_empty() {
                if let Some(series) = suspicious_col.as_ref() {
                    if let Ok(v) = series.get(row_idx) {
                        if let Some(text) = anyvalue_to_search_string(&v) {
                            if text != "0" && !text.is_empty() {
                                best_flag = "suspicious".to_string();
                            }
                        }
                    }
                }
            }
            if best_flag.is_empty() {
                if let Some(series) = safe_col.as_ref() {
                    if let Ok(v) = series.get(row_idx) {
                        if let Some(text) = anyvalue_to_search_string(&v) {
                            if text != "0" && !text.is_empty() {
                                best_flag = "safe".to_string();
                            }
                        }
                    }
                }
            }
            let memo_val = if let Some(series) = memo_col.as_ref() {
                series
                    .get(row_idx)
                    .ok()
                    .and_then(|v| anyvalue_to_search_string(&v))
            } else {
                None
            };
            let flag_entry = FlagEntry {
                flag: best_flag.clone(),
                memo: memo_val
                    .map(|m| m.trim().to_string())
                    .filter(|m| !m.is_empty()),
            };
            if !flag_entry.flag.is_empty() || flag_entry.memo.is_some() {
                imported_flags.insert(row_idx, flag_entry);
            }
        }

        if let Ok(next) = df.drop("trivium-safe") {
            df = next;
        }
        if let Ok(next) = df.drop("trivium-suspicious") {
            df = next;
        }
        if let Ok(next) = df.drop("trivium-critical") {
            df = next;
        }
        if let Ok(next) = df.drop("trivium-memo") {
            df = next;
        }
    }

    let row_ids: Vec<i64> = (0..df.height()).map(|idx| idx as i64).collect();
    let row_id_series = Series::new("__rowid", row_ids);
    df.with_column(row_id_series)
        .map_err(|_| AppError::Message("Failed to add row ids to the dataset.".into()))?;

    let project_id = Uuid::new_v4();
    let project_dir = state.projects.project_dir(&project_id);
    if !project_dir.exists() {
        fs::create_dir_all(&project_dir)
            .with_context(|| format!("failed to create project dir {:?}", project_dir))
            .map_err(AppError::from)?;
    }

    let metadata = ProjectMeta {
        id: project_id,
        name: source_path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "Imported Project".to_string()),
        description: payload.description.clone(),
        created_at: Utc::now(),
        total_records: df.height(),
        flagged_records: imported_flags
            .values()
            .filter(|entry| !entry.flag.trim().is_empty())
            .count(),
        ioc_applied_records: 0,
        hidden_columns: Vec::new(),
    };

    let parquet_path = project_dir.join("data.parquet");
    write_project_dataframe(&parquet_path, &mut df).map_err(AppError::from)?;

    let flags_path = project_dir.join("flags.json");
    if !imported_flags.is_empty() {
        save_flags(&flags_path, &imported_flags).map_err(AppError::from)?;
    }

    state
        .projects
        .insert(metadata.clone())
        .map_err(AppError::from)?;

    Ok(ProjectSummary { meta: metadata })
}

#[derive(Debug, Deserialize)]
pub struct ProjectRequest {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
}

/// Removes a project directory and clears related caches.
#[tauri::command]
pub fn delete_project(state: State<AppState>, request: ProjectRequest) -> Result<(), String> {
    let Some(meta) = state.projects.find(&request.project_id) else {
        return Ok(());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    if let Err(err) = clear_searchable_cache(&project_dir) {
        eprintln!(
            "[cache] failed to clear searchable cache for {:?}: {:?}",
            project_dir, err
        );
    }
    if let Err(err) = clear_ioc_flag_cache(&project_dir) {
        eprintln!(
            "[cache] failed to clear IOC cache for {:?}: {:?}",
            project_dir, err
        );
    }
    if project_dir.exists() {
        fs::remove_dir_all(&project_dir)
            .with_context(|| format!("failed to remove project dir {:?}", project_dir))
            .map_err(AppError::from)?;
    }
    state.projects.remove(&meta.id).map_err(AppError::from)?;
    Ok(())
}

/// Loads project metadata, initial rows, IOC entries, and column metrics.
#[tauri::command]
pub fn load_project(
    state: State<AppState>,
    request: ProjectRequest,
) -> Result<LoadProjectResponse, String> {
    let meta = state
        .projects
        .find(&request.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = state.projects.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    if !parquet_path.exists() {
        return Err(AppError::Message("Project data file missing.".into()).into());
    }

    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .filter(|name| name != &"__rowid")
        .map(|name| name.to_string())
        .collect();

    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;

    let metrics_path = project_dir.join(COLUMN_METRICS_FILE);
    let mut column_max_chars = match load_column_metrics(&metrics_path).map_err(AppError::from)? {
        Some(map) => map,
        None => {
            let computed = compute_column_max_chars(&df);
            save_column_metrics(&metrics_path, &computed).map_err(AppError::from)?;
            computed
        }
    };
    if columns
        .iter()
        .any(|column| !column_max_chars.contains_key(column))
    {
        column_max_chars = compute_column_max_chars(&df);
        save_column_metrics(&metrics_path, &column_max_chars).map_err(AppError::from)?;
    }

    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let page_limit = usize::min(DEFAULT_PAGE_SIZE, df.height());
    let mut initial_rows = materialize_rows(&df, &columns, 0..page_limit, &flags);
    apply_iocs_to_rows(&mut initial_rows, &iocs);

    println!(
        "[debug] load_project id={} total_rows={} initial_rows={}",
        meta.id,
        df.height(),
        initial_rows.len()
    );

    let summary = ProjectSummary { meta: meta.clone() };

    Ok(LoadProjectResponse {
        project: summary,
        columns,
        hidden_columns: meta.hidden_columns.clone(),
        column_max_chars,
        iocs,
        initial_rows,
    })
}
