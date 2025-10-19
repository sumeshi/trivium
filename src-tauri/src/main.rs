use std::{
    collections::{HashMap, HashSet},
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

        // 既存のプロジェクトデータを移行（flagged_recordsが0の場合は計算）
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
        
        // 移行したデータを保存
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
    fn new(app: &tauri::App) -> Result<Self> {
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

#[derive(Clone)]
struct RowCandidate {
    row_index: usize,
    flag: String,
    memo: Option<String>,
    sort_value: Option<SortComparable>,
}

#[derive(Clone, Debug)]
enum SortComparable {
    Null,
    Bool(bool),
    Number(f64),
    Text(String),
}

impl SortComparable {
    fn from_any_value(value: &AnyValue) -> Self {
        match value {
            AnyValue::Null => SortComparable::Null,
            AnyValue::Boolean(v) => SortComparable::Bool(*v),
            AnyValue::Int8(v) => SortComparable::Number(f64::from(*v)),
            AnyValue::Int16(v) => SortComparable::Number(f64::from(*v)),
            AnyValue::Int32(v) => SortComparable::Number(f64::from(*v)),
            AnyValue::Int64(v) => SortComparable::Number(*v as f64),
            AnyValue::UInt8(v) => SortComparable::Number(f64::from(*v)),
            AnyValue::UInt16(v) => SortComparable::Number(f64::from(*v)),
            AnyValue::UInt32(v) => SortComparable::Number(*v as f64),
            AnyValue::UInt64(v) => SortComparable::Number(*v as f64),
            AnyValue::Float32(v) => SortComparable::Number(f64::from(*v)),
            AnyValue::Float64(v) => SortComparable::Number(*v),
            AnyValue::String(v) => SortComparable::Text(v.to_string()),
            AnyValue::StringOwned(v) => SortComparable::Text(v.to_string()),
            AnyValue::Datetime(_, _, _) => SortComparable::Text(value.to_string()),
            AnyValue::Date(_) => SortComparable::Text(value.to_string()),
            AnyValue::Time(_) => SortComparable::Text(value.to_string()),
            AnyValue::List(series) => {
                let joined = series
                    .iter()
                    .filter_map(|inner| anyvalue_to_search_string(&inner))
                    .collect::<Vec<String>>()
                    .join(",");
                SortComparable::Text(joined)
            }
            _ => SortComparable::Text(value.to_string()),
        }
    }

    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (SortComparable::Null, SortComparable::Null) => Ordering::Equal,
            (SortComparable::Null, _) => Ordering::Greater,
            (_, SortComparable::Null) => Ordering::Less,
            (SortComparable::Bool(left), SortComparable::Bool(right)) => left.cmp(right),
            (SortComparable::Number(left), SortComparable::Number(right)) => left
                .partial_cmp(right)
                .unwrap_or(Ordering::Equal),
            (SortComparable::Text(left), SortComparable::Text(right)) => left.cmp(right),
            (SortComparable::Bool(left), SortComparable::Number(right)) => {
                (*left as i32 as f64).partial_cmp(right).unwrap_or(Ordering::Equal)
            }
            (SortComparable::Number(left), SortComparable::Bool(right)) => {
                left.partial_cmp(&(*right as i32 as f64)).unwrap_or(Ordering::Equal)
            }
            (SortComparable::Bool(left), SortComparable::Text(right)) => {
                left.to_string().cmp(right)
            }
            (SortComparable::Text(left), SortComparable::Bool(right)) => {
                left.cmp(&right.to_string())
            }
            (SortComparable::Number(left), SortComparable::Text(right)) => {
                left.to_string().cmp(right)
            }
            (SortComparable::Text(left), SortComparable::Number(right)) => {
                left.cmp(&right.to_string())
            }
        }
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
            if severity_rank_value > best_rank {
                best_rank = severity_rank_value;
                if severity_rank_value > 0 {
                    best_flag = severity.clone();
                }
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

        if best_rank > 0 {
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

    let project_id = Uuid::new_v4();
    let project_dir = state.projects.project_dir(&project_id);
    fs::create_dir_all(&project_dir)
        .with_context(|| format!("failed to create project dir {:?}", project_dir))
        .map_err(AppError::from)?;

    let parquet_path = project_dir.join("data.parquet");
    write_project_dataframe(&parquet_path, &mut df).map_err(AppError::from)?;

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
    project_id: Uuid,
}

#[derive(Debug, Deserialize)]
struct QueryRowsPayload {
    project_id: Uuid,
    #[serde(default)]
    search: Option<String>,
    #[serde(default)]
    flag_filter: Option<String>,
    #[serde(default)]
    visible_columns: Option<Vec<String>>,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    sort_key: Option<String>,
    #[serde(default)]
    sort_direction: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SaveIocsPayload {
    project_id: Uuid,
    entries: Vec<IocEntry>,
}

#[derive(Debug, Deserialize)]
struct ImportIocsPayload {
    project_id: Uuid,
    path: String,
}

#[derive(Debug, Deserialize)]
struct ExportIocsPayload {
    project_id: Uuid,
    destination: String,
}

#[derive(Debug, Serialize)]
struct QueryRowsResponse {
    rows: Vec<ProjectRow>,
    total_flagged: usize,
    total_rows: usize,
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

    // 全データを対象にして最大文字列長を計算
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

fn matches_flag_filter(current_flag: &str, filter: &str) -> bool {
    match filter {
        "all" => true,
        "none" => current_flag.is_empty(),
        "priority" => current_flag == "suspicious" || current_flag == "critical",
        "safe" | "suspicious" | "critical" => current_flag == filter,
        _ => true,
    }
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
    let search_columns: Vec<String> = if let Some(visible) = payload.visible_columns.clone() {
        let visible_set: HashSet<String> = visible.into_iter().collect();
        columns
            .iter()
            .filter(|column| visible_set.contains(*column))
            .cloned()
            .collect()
    } else {
        columns.clone()
    };
    let flags_path = project_dir.join("flags.json");
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let iocs = load_ioc_entries(&project_dir).map_err(AppError::from)?;
    let search_value = payload
        .search
        .as_ref()
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty());
    let flag_filter = payload
        .flag_filter
        .as_ref()
        .map(|value| value.trim().to_lowercase());

    let offset = payload.offset.unwrap_or(0);
    let limit = payload.limit.unwrap_or(DEFAULT_PAGE_SIZE).max(1);
    let sort_key = payload
        .sort_key
        .as_ref()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty());
    let sort_desc = payload
        .sort_direction
        .as_ref()
        .map(|value| value.eq_ignore_ascii_case("desc"))
        .unwrap_or(false);

    let mut column_series: HashMap<&str, Series> = HashMap::with_capacity(columns.len());
    for column in &columns {
        if let Ok(series) = df.column(column) {
            column_series.insert(column.as_str(), series.clone());
        }
    }

    let mut matches: Vec<RowCandidate> = Vec::new();

    for row_idx in 0..df.height() {
        let mut matches_search = search_value.is_none();

        if let Some(search_lower) = &search_value {
            if !search_columns.is_empty() {
                for column in &search_columns {
                    if let Some(series) = column_series.get(column.as_str()) {
                        if let Ok(value) = series.get(row_idx) {
                            if let Some(candidate) = anyvalue_to_search_string(&value) {
                                if candidate.to_lowercase().contains(search_lower) {
                                    matches_search = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                if !matches_search {
                    continue;
                }
            } else if !matches_search {
                continue;
            }
        }

        let flag_entry = flags.get(&row_idx);
        let normalized_flag = flag_entry
            .as_ref()
            .map(|entry| normalize_flag_value(&entry.flag))
            .unwrap_or_default();

        if let Some(filter) = &flag_filter {
            if !matches_flag_filter(normalized_flag.as_str(), filter) {
                continue;
            }
        }

        let sort_value = sort_key
            .as_ref()
            .and_then(|key| column_series.get(key.as_str()))
            .and_then(|series| series.get(row_idx).ok())
            .map(|value| SortComparable::from_any_value(&value));

        matches.push(RowCandidate {
            row_index: row_idx,
            flag: normalized_flag,
            memo: flag_entry.and_then(|entry| entry.memo.clone()),
            sort_value,
        });
    }

    if let Some(_) = sort_key {
        matches.sort_by(|a, b| {
            let ordering = match (&a.sort_value, &b.sort_value) {
                (Some(left), Some(right)) => left.cmp(right),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.row_index.cmp(&b.row_index),
            };
            if sort_desc {
                ordering.reverse()
            } else {
                ordering
            }
        });
    } else {
        matches.sort_by(|a, b| a.row_index.cmp(&b.row_index));
    }

    let total_rows = matches.len();
    let total_flagged_all = matches
        .iter()
        .filter(|candidate| !candidate.flag.trim().is_empty())
        .count();
    let start = usize::min(offset, total_rows);
    let end = usize::min(start + limit, total_rows);

    let mut rows: Vec<ProjectRow> = Vec::with_capacity(end.saturating_sub(start));
    for candidate in matches.into_iter().skip(start).take(limit) {
        let mut record = HashMap::new();
        for column in &columns {
            if let Some(series) = column_series.get(column.as_str()) {
                if let Ok(value) = series.get(candidate.row_index) {
                    record.insert(column.clone(), anyvalue_to_json(&value));
                }
            }
        }
        rows.push(ProjectRow {
            row_index: candidate.row_index,
            data: record,
            flag: candidate.flag,
            memo: candidate.memo,
        });
    }

    apply_iocs_to_rows(&mut rows, &iocs);

    println!(
        "[debug] query_project_rows id={} offset={} limit={} rows={} total_rows={}",
        meta.id,
        start,
        limit,
        rows.len(),
        total_rows
    );

    Ok(QueryRowsResponse {
        rows,
        total_flagged: total_flagged_all,
        total_rows,
        offset: start,
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
    Ok(())
}

#[tauri::command]
fn import_iocs(state: State<AppState>, payload: ImportIocsPayload) -> Result<(), String> {
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
    Ok(())
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

    // flagged_recordsを更新
    let flagged_records = flags
        .values()
        .filter(|entry| !entry.flag.trim().is_empty())
        .count();
    state.projects.update_flagged_records(&payload.project_id, flagged_records).map_err(AppError::from)?;

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

    let mut positive_flags: Vec<i32> = vec![0; df.height()];
    let mut maybe_flags: Vec<i32> = vec![0; df.height()];
    let mut negative_flags: Vec<i32> = vec![0; df.height()];
    let mut memo_series: Vec<String> = vec![String::new(); df.height()];
    for (index, entry) in flags {
        if index < memo_series.len() {
            let trimmed_flag = entry.flag.trim();
            match trimmed_flag {
                "◯" => positive_flags[index] = 1,
                "?" => maybe_flags[index] = 1,
                "✗" => negative_flags[index] = 1,
                _ => {}
            }
            memo_series[index] = entry.memo.unwrap_or_default();
        }
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
    df.with_column(Series::new("trivium-positive", positive_flags))
        .map_err(|e| AppError::Message(format!("failed to append positive flag column: {}", e)))?;
    df.with_column(Series::new("trivium-maybe", maybe_flags))
        .map_err(|e| AppError::Message(format!("failed to append maybe flag column: {}", e)))?;
    df.with_column(Series::new("trivium-negative", negative_flags))
        .map_err(|e| AppError::Message(format!("failed to append negative flag column: {}", e)))?;
    df.with_column(Series::new("trivium-memo", memo_series))
        .map_err(|e| AppError::Message(format!("failed to append memo column: {}", e)))?;

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
    tauri::Builder::default()
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
