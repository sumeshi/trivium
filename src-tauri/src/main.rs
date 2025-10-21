#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows release builds
use std::{
    collections::{HashMap},
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use csv::{ReaderBuilder, WriterBuilder};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tauri::{Manager, State};
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_PAGE_SIZE: usize = 250;

#[derive(Debug, Error)]
enum AppError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<AppError> for String {
    fn from(err: AppError) -> Self {
        err.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectMeta {
    id: Uuid,
    name: String,
    description: Option<String>,
    created_at: DateTime<Utc>,
    total_records: usize,
    #[serde(default)]
    flagged_records: usize,
    #[serde(default)]
    ioc_applied_records: usize,
    hidden_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectSummary {
    meta: ProjectMeta,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectRow {
    row_index: usize,
    data: HashMap<String, Value>,
    flag: String,
    memo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IocEntry {
    flag: String,
    tag: String,
    query: String,
}

#[derive(Debug, Clone, Serialize)]
struct LoadProjectResponse {
    project: ProjectSummary,
    columns: Vec<String>,
    hidden_columns: Vec<String>,
    column_max_chars: HashMap<String, usize>,
    iocs: Vec<IocEntry>,
    initial_rows: Vec<ProjectRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FlagEntry {
    flag: String,
    memo: Option<String>,
}

#[derive(Debug)]
struct ProjectsStore {
    root_dir: PathBuf,
    meta_path: PathBuf,
    inner: Mutex<Vec<ProjectMeta>>,
}

impl ProjectsStore {
    fn new(root_dir: PathBuf) -> Result<Self> {
        let projects_dir = root_dir.join("projects");
        fs::create_dir_all(&projects_dir)
            .with_context(|| format!("failed to create projects dir at {:?}", projects_dir))?;

        let meta_path = root_dir.join("projects.json");
        let mut projects: Vec<ProjectMeta> = if meta_path.exists() {
            let data = fs::read(&meta_path)
                .with_context(|| format!("failed to read metadata file {:?}", meta_path))?;
            serde_json::from_slice(&data)
                .with_context(|| format!("failed to parse metadata file {:?}", meta_path))?
        } else {
            Vec::new()
        };

        // Migrate existing project data (calculate if flagged_records is 0)
        let mut needs_save = false;
        for project in &mut projects {
            if project.flagged_records == 0 {
                let project_dir = root_dir.join("projects").join(project.id.to_string());
                let flags_path = project_dir.join("flags.json");
                if flags_path.exists() {
                    if let Ok(flags) = load_flags(&flags_path) {
                        project.flagged_records = flags
                            .values()
                            .filter(|entry| !entry.flag.trim().is_empty())
                            .count();
                        needs_save = true;
                    }
                }
            }
        }
        
        // Save migrated data
        if needs_save {
            let data = serde_json::to_vec_pretty(&projects)
                .with_context(|| format!("failed to serialize metadata to {:?}", meta_path))?;
            fs::write(&meta_path, data)
                .with_context(|| format!("failed to write metadata file {:?}", meta_path))?;
        }

        Ok(Self {
            root_dir,
            meta_path,
            inner: Mutex::new(projects),
        })
    }

    fn all(&self) -> Vec<ProjectMeta> {
        self.inner.lock().clone()
    }

    fn insert(&self, project: ProjectMeta) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.push(project);
        self.persist_locked(&guard)
    }

    fn update_hidden_columns(&self, id: &Uuid, hidden_columns: Vec<String>) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(meta) = guard.iter_mut().find(|meta| &meta.id == id) {
            meta.hidden_columns = hidden_columns;
        }
        self.persist_locked(&guard)
    }

    fn update_flagged_records(&self, id: &Uuid, flagged_records: usize) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(meta) = guard.iter_mut().find(|meta| &meta.id == id) {
            meta.flagged_records = flagged_records;
        }
        self.persist_locked(&guard)
    }

    fn update_ioc_applied_records(&self, id: &Uuid, ioc_applied_records: usize) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(meta) = guard.iter_mut().find(|meta| &meta.id == id) {
            meta.ioc_applied_records = ioc_applied_records;
        }
        self.persist_locked(&guard)
    }

    fn remove(&self, id: &Uuid) -> Result<()> {
        let mut guard = self.inner.lock();
        guard.retain(|meta| &meta.id != id);
        self.persist_locked(&guard)
    }

    fn find(&self, id: &Uuid) -> Option<ProjectMeta> {
        self.inner.lock().iter().find(|meta| &meta.id == id).cloned()
    }

    fn persist_locked(&self, guard: &[ProjectMeta]) -> Result<()> {
        let data = serde_json::to_vec_pretty(guard)?;
        fs::write(&self.meta_path, data)
            .with_context(|| format!("failed to write metadata file {:?}", self.meta_path))
    }

    fn project_dir(&self, id: &Uuid) -> PathBuf {
        self.root_dir.join("projects").join(id.to_string())
    }
}

struct AppState {
    projects: ProjectsStore,
}

impl AppState {
    fn new(app: &tauri::App<tauri::Wry>) -> Result<Self> {
        let base_dir = tauri::api::path::app_local_data_dir(&app.config())
            .context("failed to resolve app data dir")?
            .join("trivium");
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("failed to create app data dir {:?}", base_dir))?;
        let projects = ProjectsStore::new(base_dir)?;
        Ok(Self { projects })
    }
}

fn load_flags(path: &Path) -> Result<HashMap<usize, FlagEntry>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let data = fs::read(path)
        .with_context(|| format!("failed to read flags file {:?}", path))?;
    let map: HashMap<usize, FlagEntry> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse flags file {:?}", path))?;
    Ok(map)
}

fn save_flags(path: &Path, flags: &HashMap<usize, FlagEntry>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create flag dir {:?}", parent))?;
    }
    let data = serde_json::to_vec_pretty(flags)?;
    fs::write(path, data)
        .with_context(|| format!("failed to write flags file {:?}", path))
}

fn anyvalue_to_json(value: &AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(v) => Value::Bool(*v),
        AnyValue::Int8(v) => Value::from(*v),
        AnyValue::Int16(v) => Value::from(*v),
        AnyValue::Int32(v) => Value::from(*v),
        AnyValue::Int64(v) => Value::from(*v),
        AnyValue::UInt8(v) => Value::from(*v),
        AnyValue::UInt16(v) => Value::from(*v),
        AnyValue::UInt32(v) => Value::from(*v),
        AnyValue::UInt64(v) => Value::from(*v),
        AnyValue::Float32(v) => Value::from(f64::from(*v)),
        AnyValue::Float64(v) => Value::from(*v),
        AnyValue::String(v) => Value::String(v.to_string()),
        AnyValue::Date(v) => Value::String(v.to_string()),
        AnyValue::Datetime(v, _, _) => Value::String(v.to_string()),
        AnyValue::Time(v) => Value::String(v.to_string()),
        AnyValue::List(series) => {
            let values: Vec<Value> = series.iter().map(|v| anyvalue_to_json(&v)).collect();
            Value::Array(values)
        }
        other => Value::String(other.to_string()),
    }
}

fn value_display_length(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::String(text) => text.chars().count(),
        Value::Number(number) => number.to_string().chars().count(),
        Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value)
            .map(|text| text.chars().count())
            .unwrap_or(0),
    }
}

fn normalize_flag_value(flag: &str) -> String {
    let trimmed = flag.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    match trimmed.to_lowercase().as_str() {
        "safe" => "safe".to_string(),
        "suspicious" => "suspicious".to_string(),
        "critical" => "critical".to_string(),
        "◯" => "safe".to_string(),
        "?" => "suspicious".to_string(),
        "✗" => "critical".to_string(),
        _ => String::new(),
    }
}

fn severity_rank(value: &str) -> u8 {
    match value {
        "critical" => 3,
        "suspicious" => 2,
        "safe" => 1,
        _ => 0,
    }
}

fn value_to_search_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).ok(),
    }
}

fn anyvalue_to_search_string(value: &AnyValue) -> Option<String> {
    match value {
        AnyValue::Null => None,
        AnyValue::Boolean(v) => Some(v.to_string()),
        AnyValue::Int8(v) => Some(v.to_string()),
        AnyValue::Int16(v) => Some(v.to_string()),
        AnyValue::Int32(v) => Some(v.to_string()),
        AnyValue::Int64(v) => Some(v.to_string()),
        AnyValue::UInt8(v) => Some(v.to_string()),
        AnyValue::UInt16(v) => Some(v.to_string()),
        AnyValue::UInt32(v) => Some(v.to_string()),
        AnyValue::UInt64(v) => Some(v.to_string()),
        AnyValue::Float32(v) => Some(f64::from(*v).to_string()),
        AnyValue::Float64(v) => Some(v.to_string()),
        AnyValue::String(v) => Some(v.to_string()),
        AnyValue::StringOwned(v) => Some(v.to_string()),
        AnyValue::Datetime(_, _, _) => Some(value.to_string()),
        AnyValue::Date(_) => Some(value.to_string()),
        AnyValue::Time(_) => Some(value.to_string()),
        AnyValue::List(series) => {
            let mut parts: Vec<String> = Vec::new();
            for inner in series.iter() {
                if let Some(text) = anyvalue_to_search_string(&inner) {
                    parts.push(text);
                }
            }
            if parts.is_empty() {
                None
            } else {
                Some(parts.join(","))
            }
        }
        other => Some(other.to_string()),
    }
}


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

fn row_contains_query(row: &ProjectRow, query: &str) -> bool {
    if query.trim().is_empty() {
        return false;
    }
    let needle = query.to_lowercase();
    row.data.values().any(|value| {
        value_to_search_string(value)
            .map(|text| text.to_lowercase().contains(&needle))
            .unwrap_or(false)
    })
}

fn calculate_ioc_applied_records(project_dir: &Path) -> Result<usize> {
    let parquet_path = project_dir.join("data.parquet");
    let df = read_project_dataframe(&parquet_path)?;
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path)?;
    let iocs = load_ioc_entries(project_dir)?;
    
    let mut ioc_applied_count = 0;
    for row_idx in 0..df.height() {
        let flag_entry = flags.get(&row_idx);
        let user_flag = flag_entry
            .as_ref()
            .map(|entry| normalize_flag_value(&entry.flag))
            .unwrap_or_default();
        
        // Count IOC applications (only when no user flag exists)
        if severity_rank(&user_flag) == 0 && !iocs.is_empty() {
            let mut has_ioc_match = false;
            
            for entry in &iocs {
                let query = entry.query.trim();
                if query.is_empty() {
                    continue;
                }
                
                // Check if row matches IOC query
                for column in df.get_column_names() {
                    if column == "__rowid" {
                        continue;
                    }
                    if let Ok(series) = df.column(column) {
                        if let Ok(value) = series.get(row_idx) {
                            if let Some(text) = anyvalue_to_search_string(&value) {
                                if text.to_lowercase().contains(&query.to_lowercase()) {
                                    has_ioc_match = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                
                if has_ioc_match {
                    break;
                }
            }
            
            if has_ioc_match {
                ioc_applied_count += 1;
            }
        }
    }
    
    Ok(ioc_applied_count)
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

fn apply_iocs_to_rows(rows: &mut [ProjectRow], entries: &[IocEntry]) {
    if entries.is_empty() {
        return;
    }
    for row in rows {
        let mut best_flag = normalize_flag_value(&row.flag);
        let mut best_rank = severity_rank(&best_flag);
        let mut memo = row.memo.clone().unwrap_or_default();
        let mut memo_changed = false;

        for entry in entries {
            let query = entry.query.trim();
            if query.is_empty() {
                continue;
            }
            if !row_contains_query(row, query) {
                continue;
            }

            let severity = normalize_flag_value(&entry.flag);
            let severity_rank_value = severity_rank(&severity);
            // If user already set a flag, keep it (user wins). Only apply IOC when no user flag.
            if best_rank == 0 && severity_rank_value > 0 {
                best_rank = severity_rank_value;
                best_flag = severity.clone();
            }

            let tag = entry.tag.trim();
            if !tag.is_empty() {
                let token = format!("[{}]", tag);
                if !memo.contains(&token) {
                    if !memo.is_empty() && !memo.ends_with(' ') {
                        memo.push(' ');
                    }
                    memo.push_str(&token);
                    memo_changed = true;
                }
            }
        }

        // Persist the resolved flag into the row only if it was empty before
        if severity_rank(&normalize_flag_value(&row.flag)) == 0 && best_rank > 0 {
            row.flag = best_flag.clone();
        }
        if memo_changed {
            let trimmed = memo.trim().to_string();
            row.memo = if trimmed.is_empty() { None } else { Some(trimmed) };
        }
    }
}

fn load_ioc_entries(project_dir: &Path) -> Result<Vec<IocEntry>> {
    let path = project_dir.join("iocs.json");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = fs::read(&path)
        .with_context(|| format!("failed to read ioc file {:?}", path))?;
    let mut entries: Vec<IocEntry> = serde_json::from_slice(&data)
        .with_context(|| format!("failed to parse ioc file {:?}", path))?;
    for entry in &mut entries {
        entry.flag = normalize_flag_value(&entry.flag);
        entry.tag = entry.tag.trim().to_string();
        entry.query = entry.query.trim().to_string();
    }
    Ok(entries)
}

fn save_ioc_entries(project_dir: &Path, entries: &[IocEntry]) -> Result<()> {
    let path = project_dir.join("iocs.json");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to prepare ioc dir {:?}", parent))?;
    }
    let data = serde_json::to_vec_pretty(entries)
        .context("failed to serialize ioc entries")?;
    fs::write(&path, data)
        .with_context(|| format!("failed to write ioc file {:?}", path))
}

fn read_ioc_csv(path: &Path) -> Result<Vec<IocEntry>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("failed to open IOC CSV {:?}", path))?;
    let mut entries = Vec::new();
    for record in reader.records() {
        let record = record.with_context(|| "failed to read IOC CSV record")?;
        let flag_value = record.get(0).unwrap_or("").trim().to_string();
        let tag = record.get(1).unwrap_or("").trim().to_string();
        let query = record.get(2).unwrap_or("").trim().to_string();
        if query.is_empty() {
            continue;
        }
        entries.push(IocEntry {
            flag: normalize_flag_value(&flag_value),
            tag,
            query,
        });
    }
    Ok(entries)
}

fn write_ioc_csv(entries: &[IocEntry], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create export dir {:?}", parent))?;
    }
    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("failed to create IOC CSV {:?}", path))?;
    writer
        .write_record(["flag", "tag", "query"])
        .context("failed to write IOC CSV header")?;
    for entry in entries {
        writer
            .write_record([entry.flag.as_str(), entry.tag.as_str(), entry.query.as_str()])
            .context("failed to write IOC CSV row")?;
    }
    writer.flush().context("failed to flush IOC CSV writer")
}

fn read_project_dataframe(path: &Path) -> Result<DataFrame> {
    ParquetReader::new(File::open(path)?)
        .finish()
        .context("failed to read parquet file")
}

fn write_project_dataframe(path: &Path, df: &mut DataFrame) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("failed to create parquet file {:?}", path))?;
    let writer = ParquetWriter::new(file);
    writer.finish(df).context("failed to write parquet file")?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct CreateProjectPayload {
    path: String,
    description: Option<String>,
}

#[tauri::command]
fn list_projects(state: State<AppState>) -> Result<Vec<ProjectSummary>, String> {
    println!("[debug] list_projects called");
    let metas = state.projects.all();
    let mut result = Vec::with_capacity(metas.len());
    for meta in metas {
        result.push(ProjectSummary {
            meta: meta.clone(),
        });
    }
    result.sort_by(|a, b| b.meta.created_at.cmp(&a.meta.created_at));
    Ok(result)
}

#[tauri::command]
fn create_project(state: State<AppState>, payload: CreateProjectPayload) -> Result<ProjectSummary, String> {
    let source_path = PathBuf::from(&payload.path);
    if !source_path.exists() {
        return Err(AppError::Message("Selected file no longer exists.".into()).into());
    }

    let file = File::open(&source_path)
        .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let mut df = CsvReader::new(file)
        .finish()
        .map_err(|_| AppError::Message("Failed to parse the CSV data.".into()))?;

    // Import flags/memo from CSV if trivium-* columns are present
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
            // priority: critical > suspicious > safe
            if let Some(series) = critical_col.as_ref() {
                if let Ok(v) = series.get(row_idx) { if let Some(text) = anyvalue_to_search_string(&v) { if text != "0" && !text.is_empty() { best_flag = "critical".to_string(); } } }
            }
            if best_flag.is_empty() {
                if let Some(series) = suspicious_col.as_ref() {
                    if let Ok(v) = series.get(row_idx) { if let Some(text) = anyvalue_to_search_string(&v) { if text != "0" && !text.is_empty() { best_flag = "suspicious".to_string(); } } }
                }
            }
            if best_flag.is_empty() {
                if let Some(series) = safe_col.as_ref() {
                    if let Ok(v) = series.get(row_idx) { if let Some(text) = anyvalue_to_search_string(&v) { if text != "0" && !text.is_empty() { best_flag = "safe".to_string(); } } }
                }
            }
            let memo_val = if let Some(series) = memo_col.as_ref() { series.get(row_idx).ok().and_then(|v| anyvalue_to_search_string(&v)) } else { None };
            if !best_flag.is_empty() || memo_val.as_deref().map(|m| !m.trim().is_empty()).unwrap_or(false) {
                imported_flags.insert(row_idx, FlagEntry { flag: best_flag, memo: memo_val.map(|m| m.trim().to_string()).filter(|m| !m.is_empty()) });
            }
        }

        // Drop trivium-* columns before persisting parquet
        for name in &["trivium-safe", "trivium-suspicious", "trivium-critical", "trivium-memo"] {
            if let Ok(next) = df.drop(name) { df = next; }
        }
    }

    let project_id = Uuid::new_v4();
    let project_dir = state.projects.project_dir(&project_id);
    fs::create_dir_all(&project_dir)
        .with_context(|| format!("failed to create project dir {:?}", project_dir))
        .map_err(AppError::from)?;

    let parquet_path = project_dir.join("data.parquet");
    write_project_dataframe(&parquet_path, &mut df).map_err(AppError::from)?;

    // Persist imported flags if any
    if !imported_flags.is_empty() {
        let flags_path = state.projects.project_dir(&project_id).join("flags.json");
        save_flags(&flags_path, &imported_flags).map_err(AppError::from)?;
        // update flagged_records
        let flagged_records = imported_flags.values().filter(|e| !e.flag.trim().is_empty()).count();
        state.projects.update_flagged_records(&project_id, flagged_records).map_err(AppError::from)?;
    }

    let project_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("Untitled")
        .to_string();

    let meta = ProjectMeta {
        id: project_id,
        name: project_name,
        description: payload.description,
        created_at: Utc::now(),
        total_records: df.height(),
        flagged_records: 0,
        ioc_applied_records: 0,
        hidden_columns: Vec::new(),
    };

    state.projects.insert(meta.clone()).map_err(AppError::from)?;
    let summary = ProjectSummary {
        meta: meta.clone(),
    };
    Ok(summary)
}

#[derive(Debug, Deserialize)]
struct ProjectRequest {
    #[serde(rename = "projectId")]
    project_id: Uuid,
}

#[derive(Debug, Deserialize)]
struct QueryRowsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    #[serde(rename = "flagFilter", default)]
    flag_filter: Option<String>,
    #[serde(rename = "search", default)]
    search: Option<String>,
    #[serde(default)]
    columns: Option<Vec<String>>,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(rename = "sortKey", default)]
    sort_key: Option<String>,
    #[serde(rename = "sortDirection", default)]
    sort_direction: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SaveIocsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    entries: Vec<IocEntry>,
}

#[derive(Debug, Deserialize)]
struct ImportIocsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    path: String,
}

#[derive(Debug, Deserialize)]
struct ExportIocsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    destination: String,
}

#[derive(Debug, Serialize)]
struct QueryRowsResponse {
    rows: Vec<ProjectRow>,
    total_flagged: usize,
    total_rows: usize,
    total_filtered_rows: usize,
    offset: usize,
}

#[tauri::command]
fn delete_project(state: State<AppState>, request: ProjectRequest) -> Result<(), String> {
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
    Ok(())
}

#[tauri::command]
fn load_project(state: State<AppState>, request: ProjectRequest) -> Result<LoadProjectResponse, String> {
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

    let mut column_max_chars: HashMap<String, usize> = columns
        .iter()
        .map(|name| {
            let header_len = name.chars().count();
            (name.clone(), header_len)
        })
        .collect();

    // Calculate maximum string length for all data
    for column in &columns {
        if let Ok(series) = df.column(column) {
            for value in series.iter() {
                let json_value = anyvalue_to_json(&value);
                let display_len = value_display_length(&json_value);
                if let Some(entry) = column_max_chars.get_mut(column) {
                    if display_len > *entry {
                        *entry = display_len;
                    }
                }
            }
        }
    }

    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;

    let page_limit = usize::min(DEFAULT_PAGE_SIZE, df.height());
    let mut initial_rows = materialize_rows(&df, &columns, 0..page_limit, &flags);
    apply_iocs_to_rows(&mut initial_rows, &iocs);

    // Note: Do NOT persist IOC-applied flags here; persistence happens only via explicit updates

    println!(
        "[debug] load_project id={} total_rows={} initial_rows={}",
        meta.id,
        df.height(),
        initial_rows.len()
    );

    let summary = ProjectSummary {
        meta: meta.clone(),
    };

    Ok(LoadProjectResponse {
        project: summary,
        columns,
        hidden_columns: meta.hidden_columns.clone(),
        column_max_chars,
        iocs,
        initial_rows,
    })
}


#[tauri::command]
fn query_project_rows(state: State<AppState>, payload: QueryRowsPayload) -> Result<QueryRowsResponse, String> {
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

    // Stream rows: count filtered matches and only materialize page rows (IOC applied per-row)
    let mut rows: Vec<ProjectRow> = Vec::with_capacity(limit);
    let mut total_flagged_after_ioc: usize = 0;

    // Precompute column names and a lookup for series to speed per-row checks
    let column_names: Vec<String> = columns.clone();
    let column_series: HashMap<&str, &Series> = df.get_columns().iter().map(|s| (s.name(), s)).collect();

    // Prepare ordered row indices; if sorting requested, sort indices by the sort column
    let mut ordered_indices: Vec<usize> = (0..df.height()).collect();
    if let Some(sort_key) = &payload.sort_key {
        if let Ok(series) = df.column(sort_key) {
            // Use custom comparison logic with numeric parsing support
            ordered_indices.sort_by(|a, b| {
                // Get values as strings where possible and normalize
                let a_s = series.get(*a).ok().and_then(|v| anyvalue_to_search_string(&v)).map(|s| s.trim().replace(",", "").replace('\u{00A0}', ""));
                let b_s = series.get(*b).ok().and_then(|v| anyvalue_to_search_string(&v)).map(|s| s.trim().replace(",", "").replace('\u{00A0}', ""));

                // Try numeric comparison when either side parses
                let a_num = a_s.as_ref().and_then(|s| s.parse::<f64>().ok());
                let b_num = b_s.as_ref().and_then(|s| s.parse::<f64>().ok());

                if a_num.is_some() || b_num.is_some() {
                    let av = a_num.unwrap_or(f64::INFINITY);
                    let bv = b_num.unwrap_or(f64::INFINITY);
                    let mut ord = av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal);
                    if payload.sort_direction.as_deref() == Some("desc") { ord = ord.reverse(); }
                    return ord;
                }

                // Fallback: case-insensitive string compare
                let mut ord = match (&a_s, &b_s) {
                    (Some(a_str), Some(b_str)) => a_str.to_lowercase().cmp(&b_str.to_lowercase()),
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                if payload.sort_direction.as_deref() == Some("desc") { ord = ord.reverse(); }
                ord
            });
        }
    }

    // First pass: apply IOCs and filter to all rows to count filtered totals
    let mut filtered_rows_data: Vec<(usize, String, Option<String>, Vec<String>)> = Vec::new();
    
    for row_idx in &ordered_indices {
        let flag_entry = flags.get(row_idx);
        let user_flag = flag_entry
            .as_ref()
            .map(|entry| normalize_flag_value(&entry.flag))
            .unwrap_or_default();
        let user_memo = flag_entry.and_then(|entry| entry.memo.clone());

        let mut ioc_flag = String::new();
        let mut ioc_rank = 0;
        let mut memo_tags: Vec<String> = Vec::new();
        if !iocs.is_empty() {
            for ioc_entry in &iocs {
                let query = ioc_entry.query.trim();
                if query.is_empty() { continue; }
                let query_lower = query.to_lowercase();

                let mut row_matches = false;
                for col_name in &column_names {
                    if let Some(series) = column_series.get(col_name.as_str()) {
                        if let Ok(value) = series.get(*row_idx) {
                            if let Some(text) = anyvalue_to_search_string(&value) {
                                if text.to_lowercase().contains(&query_lower) {
                                    row_matches = true;
                                    break;
                                }
                            }
                        }
                    }
                }

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

        let final_flag = if !user_flag.is_empty() { user_flag.clone() } else { ioc_flag.clone() };

        // Flag filter match
        let matches_flag = if let Some(filter) = &payload.flag_filter {
            matches_flag_filter(&final_flag, filter)
        } else {
            true
        };

        // Search filter match (case-insensitive). If payload.columns specified, search only those, else search all columns.
        let matches_search = if let Some(search_str) = payload.search.as_ref().map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty()) {
            let search_cols: Vec<String> = payload.columns.as_ref().cloned().unwrap_or_else(|| column_names.clone());
            let mut found = false;
            for col in &search_cols {
                if let Some(series) = column_series.get(col.as_str()) {
                    if let Ok(value) = series.get(*row_idx) {
                        if let Some(text) = anyvalue_to_search_string(&value) {
                            if text.to_lowercase().contains(&search_str) {
                                found = true;
                                break;
                            }
                        }
                    }
                }
            }
            found
        } else {
            true
        };

        let matches = matches_flag && matches_search;

        if matches {
            if !final_flag.trim().is_empty() {
                total_flagged_after_ioc += 1;
            }
            filtered_rows_data.push((*row_idx, final_flag, user_memo, memo_tags));
        }
    }

    let total_filtered_rows = filtered_rows_data.len();

    // Second pass: materialize only the requested page
    for (row_idx, final_flag, user_memo, memo_tags) in filtered_rows_data.iter().skip(offset).take(limit) {
        let mut record = HashMap::new();
        for column in &column_names {
            if let Some(series) = column_series.get(column.as_str()) {
                if let Ok(value) = series.get(*row_idx) {
                    record.insert(column.clone(), anyvalue_to_json(&value));
                }
            }
        }
        let mut final_memo = user_memo.clone().unwrap_or_default();
        for tag in memo_tags {
            if !final_memo.contains(tag) {
                if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                    final_memo.push(' ');
                }
                final_memo.push_str(tag);
            }
        }
        rows.push(ProjectRow {
            row_index: *row_idx,
            data: record,
            flag: final_flag.clone(),
            memo: if final_memo.is_empty() { None } else { Some(final_memo) },
        });
    }

    // (debug logs removed)

    Ok(QueryRowsResponse {
        rows,
        total_flagged: total_flagged_after_ioc,
        total_rows: total_rows_before_flag_filter,
        total_filtered_rows,
        offset,
    })
}

#[tauri::command]
fn save_iocs(state: State<AppState>, payload: SaveIocsPayload) -> Result<(), String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let mut entries: Vec<IocEntry> = payload
        .entries
        .into_iter()
        .map(|entry| IocEntry {
            flag: normalize_flag_value(&entry.flag),
            tag: entry.tag.trim().to_string(),
            query: entry.query.trim().to_string(),
        })
        .filter(|entry| !entry.query.is_empty())
        .collect();
    entries.sort_by(|a, b| a.tag.cmp(&b.tag));
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;
    
        // Update ioc_applied_records
    let ioc_applied_records = calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state.projects.update_ioc_applied_records(&payload.project_id, ioc_applied_records).map_err(AppError::from)?;
    
    Ok(())
}

#[tauri::command]
fn import_iocs(state: State<AppState>, payload: ImportIocsPayload) -> Result<Vec<IocEntry>, String> {
    let Some(meta) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&meta.id);
    let source = PathBuf::from(payload.path);
    if !source.exists() {
        return Err(AppError::Message("Selected file does not exist.".into()).into());
    }
    let mut entries = read_ioc_csv(&source).map_err(AppError::from)?;
    entries.sort_by(|a, b| a.tag.cmp(&b.tag));
    save_ioc_entries(&project_dir, &entries).map_err(AppError::from)?;
    
        // Update ioc_applied_records
    let ioc_applied_records = calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state.projects.update_ioc_applied_records(&payload.project_id, ioc_applied_records).map_err(AppError::from)?;
    
    let final_entries = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    Ok(final_entries)
}

#[tauri::command]
fn export_iocs(state: State<AppState>, payload: ExportIocsPayload) -> Result<(), String> {
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
struct UpdateFlagPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    row_index: usize,
    flag: String,
    memo: Option<String>,
}

#[tauri::command]
fn update_flag(state: State<AppState>, payload: UpdateFlagPayload) -> Result<ProjectRow, String> {
    let Some(_) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    let project_dir = state.projects.project_dir(&payload.project_id);
    let flags_path = project_dir.join("flags.json");
    let mut flags = load_flags(&flags_path).map_err(AppError::from)?;

    if payload.flag.trim().is_empty() && payload.memo.as_ref().map(|m| m.trim().is_empty()).unwrap_or(true) {
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

        // Update flagged_records (user flags only)
    let flagged_records = flags
        .values()
        .filter(|entry| !entry.flag.trim().is_empty())
        .count();
    state.projects.update_flagged_records(&payload.project_id, flagged_records).map_err(AppError::from)?;
    
        // Update ioc_applied_records
    let ioc_applied_records = calculate_ioc_applied_records(&project_dir).map_err(AppError::from)?;
    state.projects.update_ioc_applied_records(&payload.project_id, ioc_applied_records).map_err(AppError::from)?;

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
struct HiddenColumnsPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    hidden_columns: Vec<String>,
}

#[tauri::command]
fn set_hidden_columns(state: State<AppState>, payload: HiddenColumnsPayload) -> Result<(), String> {
    let Some(_) = state.projects.find(&payload.project_id) else {
        return Err(AppError::Message("Project not found.".into()).into());
    };
    state
        .projects
        .update_hidden_columns(&payload.project_id, payload.hidden_columns)
        .map_err(AppError::from)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct ExportProjectPayload {
    #[serde(rename = "projectId")]
    project_id: Uuid,
    destination: String,
}

#[tauri::command]
fn export_project(state: State<AppState>, payload: ExportProjectPayload) -> Result<(), String> {
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
        // 1. Always check for IOC matches to get potential flag and memo tags.
        let mut ioc_flag = String::new();
        let mut ioc_rank = 0;
        let mut memo_tags = Vec::new();

        if !iocs.is_empty() {
            for ioc_entry in &iocs {
                let query = ioc_entry.query.trim();
                if query.is_empty() {
                    continue;
                }
                let query_lower = query.to_lowercase();
                let mut row_matches = false;
                for col_name in &column_names {
                    if let Some(series) = column_series.get(col_name.as_str()) {
                        if let Ok(value) = series.get(i) {
                            if let Some(text) = anyvalue_to_search_string(&value) {
                                if text.to_lowercase().contains(&query_lower) {
                                    row_matches = true;
                                    break;
                                }
                            }
                        }
                    }
                }

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

        // 2. Check for a user-set flag and determine final flag/memo.
        let final_flag: String;
        let mut final_memo: String;

        if let Some(user_entry) = flags.get(&i) {
            // User flag exists. It wins for the flag value.
            final_flag = normalize_flag_value(user_entry.flag.trim());
            final_memo = user_entry.memo.clone().unwrap_or_default();
        } else {
            // No user flag. Use the IOC flag.
            final_flag = ioc_flag;
            final_memo = String::new();
        }

        // 3. Append IOC memo tags to the final memo, avoiding duplicates.
        for tag in memo_tags {
            if !final_memo.contains(&tag) {
                if !final_memo.is_empty() && !final_memo.ends_with(' ') {
                    final_memo.push(' ');
                }
                final_memo.push_str(&tag);
            }
        }

        // 4. Populate the output vectors.
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
    // Prepend columns at the front in order: trivium-safe, trivium-suspicious, trivium-critical, trivium-memo
    let mut out_cols: Vec<Series> = Vec::new();
    out_cols.push(Series::new("trivium-safe", safe_flags));
    out_cols.push(Series::new("trivium-suspicious", suspicious_flags));
    out_cols.push(Series::new("trivium-critical", critical_flags));
    out_cols.push(Series::new("trivium-memo", memo_series));
    for name in df.get_column_names() {
        if let Ok(series) = df.column(name) { out_cols.push(series.clone()); }
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

fn main() {
    tauri::Builder::new()
        .setup(|app| {
            let state = AppState::new(app)?;
            app.manage(state);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_projects,
            create_project,
            delete_project,
            load_project,
            query_project_rows,
            save_iocs,
            import_iocs,
            export_iocs,
            update_flag,
            set_hidden_columns,
            export_project
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
