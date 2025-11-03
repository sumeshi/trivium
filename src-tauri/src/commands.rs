use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use tauri::State;
use uuid::Uuid;

use crate::{
    error::AppError,
    flags::{normalize_flag_value, severity_rank},
    ioc::{
        apply_iocs_to_rows, calculate_ioc_applied_records, load_ioc_entries, prepare_ioc_entries,
        read_ioc_csv, save_ioc_entries, write_ioc_csv,
    },
    models::{FlagEntry, IocEntry, LoadProjectResponse, ProjectMeta, ProjectRow, ProjectSummary},
    project_io::{read_project_dataframe, write_project_dataframe},
    search::{
        build_search_mask_boolean, ensure_searchable_text, tokenize_search_query, to_rpn, SearchToken,
    },
    state::AppState,
    storage::{
        compute_column_max_chars, load_column_metrics, load_flags, save_column_metrics, save_flags,
    },
    value_utils::{anyvalue_to_json, anyvalue_to_search_string},
};

const DEFAULT_PAGE_SIZE: usize = 250;
const COLUMN_METRICS_FILE: &str = "column_max_chars.json";

fn materialize_rows(
    df: &DataFrame,
    columns: &[String],
    row_indices: impl Iterator<Item = usize>,
    flags: &HashMap<usize, FlagEntry>,
) -> Vec<ProjectRow> {
    let mut column_series: HashMap<&str, Series> = HashMap::with_capacity(columns.len());
    for column in columns {
        if let Ok(series) = df.column(column) {
            column_series.insert(column.as_str(), series.clone());
        }
    }

    let mut rows = Vec::new();
    for row_idx in row_indices {
        let mut record = HashMap::new();
        for column in columns {
            if let Some(series) = column_series.get(column.as_str()) {
                if let Ok(value) = series.get(row_idx) {
                    record.insert(column.clone(), anyvalue_to_json(&value));
                }
            }
        }
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

fn matches_flag_filter(current_flag: &str, filter: &str) -> bool {
    match filter {
        "all" => true,
        "none" => current_flag.is_empty(),
        "priority" => current_flag == "suspicious" || current_flag == "critical",
        "safe" | "suspicious" | "critical" => current_flag == filter,
        _ => true,
    }
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
    let has_suspicious = df.get_column_names().iter().any(|c| c == &"trivium-suspicious");
    let has_critical = df.get_column_names().iter().any(|c| c == &"trivium-critical");
    let has_memo = df.get_column_names().iter().any(|c| c == &"trivium-memo");
    if has_safe || has_suspicious || has_critical || has_memo {
        let safe_col = if has_safe { df.column("trivium-safe").ok() } else { None };
        let suspicious_col = if has_suspicious { df.column("trivium-suspicious").ok() } else { None };
        let critical_col = if has_critical { df.column("trivium-critical").ok() } else { None };
        let memo_col = if has_memo { df.column("trivium-memo").ok() } else { None };

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
                memo: memo_val.map(|m| m.trim().to_string()).filter(|m| !m.is_empty()),
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

    state.projects.insert(metadata.clone()).map_err(AppError::from)?;

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
    if project_dir.exists() {
        fs::remove_dir_all(&project_dir)
            .with_context(|| format!("failed to remove project dir {:?}", project_dir))
            .map_err(AppError::from)?;
    }
    state.projects.remove(&meta.id).map_err(AppError::from)?;
    state.searchable_cache.lock().remove(&request.project_id);
    state.ioc_flag_cache.lock().remove(&request.project_id);
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
    if columns.iter().any(|column| !column_max_chars.contains_key(column)) {
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

#[derive(Debug, Deserialize)]
pub struct SaveIocsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub entries: Vec<IocEntry>,
}

#[derive(Debug, Deserialize)]
pub struct ImportIocsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct ExportIocsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub destination: String,
}

/// Normalizes and persists IOC definitions, updating cached counts.
#[tauri::command]
pub fn save_iocs(state: State<AppState>, payload: SaveIocsPayload) -> Result<(), String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let entries = prepare_ioc_entries(payload.entries);
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;

    state.ioc_flag_cache.lock().remove(&payload.project_id);

    let ioc_applied_records =
        calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state
        .projects
        .update_ioc_applied_records(&payload.project_id, ioc_applied_records)
        .map_err(AppError::from)?;

    Ok(())
}

/// Imports IOC rules from a CSV, replacing the current set.
#[tauri::command]
pub fn import_iocs(
    state: State<AppState>,
    payload: ImportIocsPayload,
) -> Result<Vec<IocEntry>, String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let source = PathBuf::from(payload.path);
    if !source.exists() {
        return Err(AppError::Message("Selected file does not exist.".into()).into());
    }
    let entries = prepare_ioc_entries(read_ioc_csv(&source).map_err(AppError::from)?);
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;

    state.ioc_flag_cache.lock().remove(&payload.project_id);

    let ioc_applied_records =
        calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state
        .projects
        .update_ioc_applied_records(&payload.project_id, ioc_applied_records)
        .map_err(AppError::from)?;

    let final_entries = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    Ok(final_entries)
}

/// Writes the current IOC set to a destination CSV file.
#[tauri::command]
pub fn export_iocs(state: State<AppState>, payload: ExportIocsPayload) -> Result<(), String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let entries = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    let destination = PathBuf::from(payload.destination);
    write_ioc_csv(&entries, &destination).map_err(AppError::from)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct UpdateFlagPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub row_index: usize,
    pub flag: String,
    pub memo: Option<String>,
}

/// Applies or clears a user flag for a single row and updates counters.
#[tauri::command]
pub fn update_flag(state: State<AppState>, payload: UpdateFlagPayload) -> Result<ProjectRow, String> {
    let Some(_) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&payload.project_id);
    let flags_path = project_dir.join("flags.json");
    let mut flags = load_flags(&flags_path).map_err(AppError::from)?;

    if payload.flag.trim().is_empty()
        && payload
            .memo
            .as_ref()
            .map(|m| m.trim().is_empty())
            .unwrap_or(true)
    {
        flags.remove(&payload.row_index);
    } else {
        flags.insert(
            payload.row_index,
            FlagEntry {
                flag: payload.flag.clone(),
                memo: payload.memo.clone(),
            },
        );
    }

    save_flags(&flags_path, &flags).map_err(AppError::from)?;

    let flagged_records = flags
        .values()
        .filter(|entry| !entry.flag.trim().is_empty())
        .count();
    state
        .projects
        .update_flagged_records(&payload.project_id, flagged_records)
        .map_err(AppError::from)?;

    let ioc_applied_records =
        calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state
        .projects
        .update_ioc_applied_records(&payload.project_id, ioc_applied_records)
        .map_err(AppError::from)?;

    let parquet_path = project_dir.join("data.parquet");
    let df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let mut record = HashMap::new();
    for column in df.get_column_names() {
        if column == "__rowid" {
            continue;
        }
        if let Ok(series) = df.column(column) {
            if let Ok(value) = series.get(payload.row_index) {
                record.insert(column.to_string(), anyvalue_to_json(&value));
            }
        }
    }

    Ok(ProjectRow {
        row_index: payload.row_index,
        data: record,
        flag: payload.flag,
        memo: payload.memo,
    })
}

#[derive(Debug, Deserialize)]
pub struct HiddenColumnsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub hidden_columns: Vec<String>,
}

/// Persists the set of hidden columns for a project and resets search cache.
#[tauri::command]
pub fn set_hidden_columns(
    state: State<AppState>,
    payload: HiddenColumnsPayload,
) -> Result<(), String> {
    let Some(_) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    state
        .projects
        .update_hidden_columns(&payload.project_id, payload.hidden_columns)
        .map_err(AppError::from)?;
    state.searchable_cache.lock().remove(&payload.project_id);
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct ExportProjectPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub destination: String,
}

/// Exports the project data with derived trivium columns to a CSV file.
#[tauri::command]
pub fn export_project(
    state: State<AppState>,
    payload: ExportProjectPayload,
) -> Result<(), String> {
    let meta = state
        .projects
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = state.projects.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    let mut df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let mut safe_flags: Vec<i32> = vec![0; df.height()];
    let mut suspicious_flags: Vec<i32> = vec![0; df.height()];
    let mut critical_flags: Vec<i32> = vec![0; df.height()];
    let mut memo_series: Vec<String> = vec![String::new(); df.height()];

    let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let column_series: HashMap<&str, &Series> = df.get_columns().iter().map(|s| (s.name(), s)).collect();

    for i in 0..df.height() {
        let mut ioc_flag = String::new();
        let mut ioc_rank = 0;
        let mut memo_tags = Vec::new();

        if !iocs.is_empty() {
            let mut row_text = String::new();
            for col_name in &column_names {
                if let Some(series) = column_series.get(col_name.as_str()) {
                    if let Ok(value) = series.get(i) {
                        if let Some(text) = anyvalue_to_search_string(&value) {
                            let lower = text.to_lowercase();
                            if !lower.is_empty() {
                                if !row_text.is_empty() {
                                    row_text.push(' ');
                                }
                                row_text.push_str(&lower);
                            }
                        }
                    }
                }
            }
            for ioc_entry in &iocs {
                let query = ioc_entry.query.trim();
                if query.is_empty() {
                    continue;
                }
                let tokens = tokenize_search_query(query);
                let mut terms: Vec<(Option<String>, String)> = Vec::new();
                for t in &tokens {
                    if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t {
                        let key = (col.clone(), text.clone());
                        if !text.is_empty() && !terms.contains(&key) {
                            terms.push(key);
                        }
                    }
                }
                if terms.is_empty() {
                    continue;
                }
                let rpn = to_rpn(&tokens);
                let mut single_per_col: HashMap<String, Vec<String>> = HashMap::new();
                for col_name in &column_names {
                    let mut s = String::new();
                    if let Some(series) = column_series.get(col_name.as_str()) {
                        if let Ok(v) = series.get(i) {
                            if let Some(t) = anyvalue_to_search_string(&v) {
                                s = t.to_lowercase();
                            }
                        }
                    }
                    single_per_col.insert(col_name.to_lowercase(), vec![s]);
                }
                let mask = build_search_mask_boolean(&rpn, &terms, &vec![row_text.clone()], Some(&single_per_col));
                let row_matches = mask.get(0).copied().unwrap_or(false);

                if row_matches {
                    let severity = normalize_flag_value(&ioc_entry.flag);
                    let severity_rank_value = severity_rank(&severity);
                    if severity_rank_value > ioc_rank {
                        ioc_rank = severity_rank_value;
                        ioc_flag = severity.clone();
                    }
                    let tag = ioc_entry.tag.trim();
                    if !tag.is_empty() {
                        let token = format!("[{}]", tag);
                        if !memo_tags.contains(&token) {
                            memo_tags.push(token);
                        }
                    }
                }
            }
        }

        let final_flag: String;
        let mut final_memo: String;

        if let Some(user_entry) = flags.get(&i) {
            final_flag = normalize_flag_value(user_entry.flag.trim());
            final_memo = user_entry.memo.clone().unwrap_or_default();
        } else {
            final_flag = ioc_flag;
            final_memo = String::new();
        }

        for tag in memo_tags {
            if !final_memo.contains(&tag) {
                if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                    final_memo.push(' ');
                }
                final_memo.push_str(&tag);
            }
        }

        match final_flag.as_str() {
            "safe" => safe_flags[i] = 1,
            "suspicious" => suspicious_flags[i] = 1,
            "critical" => critical_flags[i] = 1,
            _ => {}
        }
        memo_series[i] = final_memo;
    }

    if let Ok(next) = df.drop("__rowid") {
        df = next;
    }
    if let Ok(next) = df.drop("flag") {
        df = next;
    }
    if let Ok(next) = df.drop("memo") {
        df = next;
    }

    let mut out_cols: Vec<Series> = Vec::new();
    out_cols.push(Series::new("trivium-safe", safe_flags));
    out_cols.push(Series::new("trivium-suspicious", suspicious_flags));
    out_cols.push(Series::new("trivium-critical", critical_flags));
    out_cols.push(Series::new("trivium-memo", memo_series));
    for name in df.get_column_names() {
        if let Ok(series) = df.column(name) {
            out_cols.push(series.clone());
        }
    }
    df = DataFrame::new(out_cols).map_err(|e| AppError::Other(e.into()))?;

    let destination = PathBuf::from(payload.destination);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create export dir {:?}", parent))
            .map_err(AppError::from)?;
    }

    let file = File::create(&destination)
        .with_context(|| format!("failed to create export file {:?}", destination))
        .map_err(AppError::from)?;
    let mut writer = CsvWriter::new(BufWriter::new(file));
    let mut df_out = df.clone();
    writer
        .finish(&mut df_out)
        .context("failed to write export CSV")
        .map_err(AppError::from)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct QueryRowsPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    #[serde(default)]
    pub search: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default, rename = "flagFilter")]
    pub flag_filter: Option<String>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(rename = "sortKey", default)]
    pub sort_key: Option<String>,
    #[serde(rename = "sortDirection", default)]
    pub sort_direction: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QueryRowsResponse {
    pub rows: Vec<ProjectRow>,
    pub total_flagged: usize,
    pub total_rows: usize,
    pub total_filtered_rows: usize,
    pub offset: usize,
}

/// Streams project rows with filtering, sorting, IOC application, and pagination.
#[tauri::command]
pub fn query_project_rows(
    state: State<AppState>,
    payload: QueryRowsPayload,
) -> Result<QueryRowsResponse, String> {
    let meta = state
        .projects
        .find(&payload.project_id)
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
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let offset = payload.offset.unwrap_or(0);
    let limit = payload.limit.unwrap_or(DEFAULT_PAGE_SIZE).max(1);

    let total_rows_before_flag_filter = df.height();

    let mut rows: Vec<ProjectRow> = Vec::with_capacity(limit);
    let mut total_flagged_after_ioc: usize = 0;

    let column_names: Vec<String> = columns.clone();
    let column_series: HashMap<&str, &Series> = df.get_columns().iter().map(|s| (s.name(), s)).collect();
    let column_series_lower: HashMap<String, &Series> = df
        .get_columns()
        .iter()
        .map(|s| (s.name().to_lowercase(), s))
        .collect();

    let search_cols: Vec<String> = payload
        .columns
        .as_ref()
        .cloned()
        .unwrap_or_else(|| column_names.clone());
    let row_count = df.height();
    let cached_search = {
        let guard = state.searchable_cache.lock();
        guard.get(&meta.id).cloned()
    };
    let mut searchable_text: Option<Vec<String>> = cached_search.and_then(|cached| {
        if cached.len() == row_count && cached.iter().any(|s| !s.is_empty()) {
            Some(cached)
        } else {
            None
        }
    });
    let mut searchable_text_built = false;
    let mut per_column_text: HashMap<String, Vec<String>> = HashMap::new();

    let mut search_mask: Option<Vec<bool>> = None;
    if let Some(search_str_raw) = payload.search.as_ref().map(|s| s.trim().to_string()) {
        if !search_str_raw.is_empty() {
            let tokens = tokenize_search_query(&search_str_raw);
            let mut terms: Vec<(Option<String>, String)> = Vec::new();
            let mut needed_cols: Vec<String> = Vec::new();
            for t in &tokens {
                match t {
                    SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } => {
                        let key = (col.clone(), text.clone());
                        if !text.is_empty() && !terms.contains(&key) {
                            terms.push(key);
                        }
                        if let Some(c) = col {
                            if !needed_cols.contains(c) {
                                needed_cols.push(c.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            for c in needed_cols {
                if per_column_text.contains_key(&c) {
                    continue;
                }
                if let Some(series) = column_series_lower.get(&c) {
                    let series = *series;
                    let mut col_vec: Vec<String> = vec![String::new(); df.height()];
                    for i in 0..df.height() {
                        if let Ok(value) = series.get(i) {
                            if let Some(text) = anyvalue_to_search_string(&value) {
                                let lower = text.to_lowercase();
                                if !lower.is_empty() {
                                    col_vec[i] = lower;
                                }
                            }
                        }
                    }
                    per_column_text.insert(c.clone(), col_vec);
                }
            }
            if !terms.is_empty() {
                let rpn = to_rpn(&tokens);
                let search_text = ensure_searchable_text(
                    &mut searchable_text,
                    &mut searchable_text_built,
                    row_count,
                    &search_cols,
                    &column_series,
                );
                let mask =
                    build_search_mask_boolean(&rpn, &terms, search_text, Some(&per_column_text));
                search_mask = Some(mask);
            }
        }
    }

    let mut user_flag_vec: Vec<String> = vec![String::new(); df.height()];
    for (idx, entry) in flags.iter() {
        if *idx < df.height() {
            user_flag_vec[*idx] = normalize_flag_value(&entry.flag);
        }
    }

    let mut ioc_flag_vec: Vec<String> =
        if let Some(cached) = state.ioc_flag_cache.lock().get(&meta.id).cloned() {
            if cached.len() == df.height() {
                cached
            } else {
                vec![String::new(); df.height()]
            }
        } else {
            vec![String::new(); df.height()]
        };
    let mut sorted_iocs = iocs.clone();
    sorted_iocs.sort_by_key(|e| std::cmp::Reverse(severity_rank(&normalize_flag_value(&e.flag))));
    let need_rebuild_ioc = ioc_flag_vec.iter().all(|s| s.is_empty());
    if need_rebuild_ioc {
        for ioc_entry in &sorted_iocs {
            let query = ioc_entry.query.trim();
            if query.is_empty() {
                continue;
            }
            let tokens = tokenize_search_query(query);
            let mut terms: Vec<(Option<String>, String)> = Vec::new();
            let mut needed_cols: Vec<String> = Vec::new();
            for t in &tokens {
                if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t {
                    let key = (col.clone(), text.clone());
                    if !text.is_empty() && !terms.contains(&key) {
                        terms.push(key);
                    }
                    if let Some(c) = col {
                        if !needed_cols.contains(c) {
                            needed_cols.push(c.clone());
                        }
                    }
                }
            }
            for c in needed_cols {
                if per_column_text.contains_key(&c) {
                    continue;
                }
                if let Some(series) = column_series_lower.get(&c) {
                    let series = *series;
                    let mut col_vec: Vec<String> = vec![String::new(); df.height()];
                    for i in 0..df.height() {
                        if let Ok(value) = series.get(i) {
                            if let Some(text) = anyvalue_to_search_string(&value) {
                                let lower = text.to_lowercase();
                                if !lower.is_empty() {
                                    col_vec[i] = lower;
                                }
                            }
                        }
                    }
                    per_column_text.insert(c.clone(), col_vec);
                }
            }
            if terms.is_empty() {
                continue;
            }
            let rpn = to_rpn(&tokens);
            let search_text = ensure_searchable_text(
                &mut searchable_text,
                &mut searchable_text_built,
                row_count,
                &search_cols,
                &column_series,
            );
            let mask =
                build_search_mask_boolean(&rpn, &terms, search_text, Some(&per_column_text));
            for i in 0..df.height() {
                if !ioc_flag_vec[i].is_empty() || !user_flag_vec[i].is_empty() {
                    continue;
                }
                if mask.get(i).copied().unwrap_or(false) {
                    ioc_flag_vec[i] = normalize_flag_value(&ioc_entry.flag);
                }
            }
        }
        state.ioc_flag_cache.lock().insert(meta.id, ioc_flag_vec.clone());
    }

    let mut ordered_indices: Vec<usize> = (0..df.height()).collect();
    if let Some(sort_key) = &payload.sort_key {
        if let Ok(series) = df.column(sort_key) {
            ordered_indices.sort_by(|a, b| {
                let a_s = series
                    .get(*a)
                    .ok()
                    .and_then(|v| anyvalue_to_search_string(&v))
                    .map(|s| s.trim().replace(',', "").replace('\u{00A0}', ""));
                let b_s = series
                    .get(*b)
                    .ok()
                    .and_then(|v| anyvalue_to_search_string(&v))
                    .map(|s| s.trim().replace(',', "").replace('\u{00A0}', ""));

                let a_num = a_s.as_ref().and_then(|s| s.parse::<f64>().ok());
                let b_num = b_s.as_ref().and_then(|s| s.parse::<f64>().ok());

                if a_num.is_some() || b_num.is_some() {
                    let av = a_num.unwrap_or(f64::INFINITY);
                    let bv = b_num.unwrap_or(f64::INFINITY);
                    let mut ord = av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal);
                    if payload.sort_direction.as_deref() == Some("desc") {
                        ord = ord.reverse();
                    }
                    return ord;
                }

                let mut ord = match (&a_s, &b_s) {
                    (Some(a_str), Some(b_str)) => a_str.to_lowercase().cmp(&b_str.to_lowercase()),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                if payload.sort_direction.as_deref() == Some("desc") {
                    ord = ord.reverse();
                }
                ord
            });
        }
    }

    let mut final_flag_vec: Vec<String> = Vec::with_capacity(df.height());
    final_flag_vec.extend((0..df.height()).map(|i| {
        if !user_flag_vec[i].is_empty() {
            user_flag_vec[i].clone()
        } else {
            ioc_flag_vec[i].clone()
        }
    }));

    let mut filtered_indices: Vec<usize> = Vec::with_capacity(df.height());
    for &idx in &ordered_indices {
        let ff = &final_flag_vec[idx];
        let flag_ok = if let Some(filter) = &payload.flag_filter {
            matches_flag_filter(ff, filter)
        } else {
            true
        };
        if !flag_ok {
            continue;
        }
        if let Some(mask) = &search_mask {
            if !mask[idx] {
                continue;
            }
        }
        filtered_indices.push(idx);
    }
    for &idx in &filtered_indices {
        if !final_flag_vec[idx].trim().is_empty() {
            total_flagged_after_ioc += 1;
        }
    }
    let total_filtered_rows = filtered_indices.len();

    for &row_idx in filtered_indices.iter().skip(offset).take(limit) {
        let mut record = HashMap::new();
        for column in &column_names {
            if let Some(series) = column_series.get(column.as_str()) {
                if let Ok(value) = series.get(row_idx) {
                    record.insert(column.clone(), anyvalue_to_json(&value));
                }
            }
        }
        let user_memo = flags
            .get(&row_idx)
            .and_then(|e| e.memo.clone())
            .unwrap_or_default();
        let mut final_memo = user_memo;
        if !iocs.is_empty() && final_flag_vec[row_idx] == ioc_flag_vec[row_idx] {
            let mut memo_tags: Vec<String> = Vec::new();
            for ioc_entry in &iocs {
                let query = ioc_entry.query.trim();
                if query.is_empty() {
                    continue;
                }
                let tokens = tokenize_search_query(query);
                let mut terms: Vec<(Option<String>, String)> = Vec::new();
                let mut needed_cols: Vec<String> = Vec::new();
                for t in &tokens {
                    if let SearchToken::Term { col, text } | SearchToken::QuotedTerm { col, text } = t {
                        let key = (col.clone(), text.clone());
                        if !text.is_empty() && !terms.contains(&key) {
                            terms.push(key);
                        }
                        if let Some(c) = col {
                            if !needed_cols.contains(c) {
                                needed_cols.push(c.clone());
                            }
                        }
                    }
                }
                if terms.is_empty() {
                    continue;
                }
                let rpn = to_rpn(&tokens);
                for c in needed_cols {
                    if per_column_text.contains_key(&c) {
                        continue;
                    }
                    if let Some(series) = column_series_lower.get(&c) {
                        let series = *series;
                        let mut col_vec: Vec<String> = vec![String::new(); df.height()];
                        for i in 0..df.height() {
                            if let Ok(value) = series.get(i) {
                                if let Some(text) = anyvalue_to_search_string(&value) {
                                    let lower = text.to_lowercase();
                                    if !lower.is_empty() {
                                        col_vec[i] = lower;
                                    }
                                }
                            }
                        }
                        per_column_text.insert(c.clone(), col_vec);
                    }
                }
                let mut single_per_col: HashMap<String, Vec<String>> = HashMap::new();
                for (col, vec_texts) in per_column_text.iter() {
                    if let Some(v) = vec_texts.get(row_idx) {
                        single_per_col.insert(col.to_lowercase(), vec![v.clone()]);
                    }
                }
                let search_text = ensure_searchable_text(
                    &mut searchable_text,
                    &mut searchable_text_built,
                    row_count,
                    &search_cols,
                    &column_series,
                );
                let row_search_text = search_text[row_idx].clone();
                let single_mask = build_search_mask_boolean(
                    &rpn,
                    &terms,
                    &vec![row_search_text],
                    Some(&single_per_col),
                );
                if single_mask.get(0).copied().unwrap_or(false) {
                    let tag = ioc_entry.tag.trim();
                    if !tag.is_empty() {
                        let token = format!("[{}]", tag);
                        if !memo_tags.contains(&token) {
                            memo_tags.push(token);
                        }
                    }
                }
            }
            for tag in memo_tags {
                if !final_memo.contains(&tag) {
                    if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                        final_memo.push(' ');
                    }
                    final_memo.push_str(&tag);
                }
            }
        }
        rows.push(ProjectRow {
            row_index: row_idx,
            data: record,
            flag: final_flag_vec[row_idx].clone(),
            memo: if final_memo.is_empty() { None } else { Some(final_memo) },
        });
    }

    if searchable_text_built {
        if let Some(ref built) = searchable_text {
            state.searchable_cache.lock().insert(meta.id, built.clone());
        }
    }

    Ok(QueryRowsResponse {
        rows,
        total_flagged: total_flagged_after_ioc,
        total_rows: total_rows_before_flag_filter,
        total_filtered_rows,
        offset,
    })
}
