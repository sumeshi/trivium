use std::{
    collections::HashMap,
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tauri::{Manager, State};
use thiserror::Error;
use uuid::Uuid;

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
    hidden_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectSummary {
    meta: ProjectMeta,
    flagged_records: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectRow {
    row_index: usize,
    data: HashMap<String, Value>,
    flag: String,
    memo: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct LoadProjectResponse {
    project: ProjectSummary,
    columns: Vec<String>,
    rows: Vec<ProjectRow>,
    hidden_columns: Vec<String>,
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
        let projects: Vec<ProjectMeta> = if meta_path.exists() {
            let data = fs::read(&meta_path)
                .with_context(|| format!("failed to read metadata file {:?}", meta_path))?;
            serde_json::from_slice(&data)
                .with_context(|| format!("failed to parse metadata file {:?}", meta_path))?
        } else {
            Vec::new()
        };

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
        let project_dir = state.projects.project_dir(&meta.id);
        let flags_path = project_dir.join("flags.json");
        let flags = load_flags(&flags_path).map_err(AppError::from)?;
        let flagged_records = flags
            .values()
            .filter(|entry| !entry.flag.trim().is_empty())
            .count();
        result.push(ProjectSummary {
            meta: meta.clone(),
            flagged_records,
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
        hidden_columns: Vec::new(),
    };

    state.projects.insert(meta.clone()).map_err(AppError::from)?;
    let summary = ProjectSummary {
        meta: meta.clone(),
        flagged_records: 0,
    };
    Ok(summary)
}

#[derive(Debug, Deserialize)]
struct ProjectRequest {
    project_id: Uuid,
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

    let mut rows = Vec::with_capacity(df.height());
    let columns_ref = df.get_column_names();
    for row_idx in 0..df.height() {
        let mut record = HashMap::new();
        for column in &columns_ref {
            if *column == "__rowid" {
                continue;
            }
            if let Ok(series) = df.column(column) {
                if let Ok(value) = series.get(row_idx) {
                    record.insert(column.to_string(), anyvalue_to_json(&value));
                }
            }
        }
        let flag_entry = flags.get(&row_idx);
        rows.push(ProjectRow {
            row_index: row_idx,
            data: record,
            flag: flag_entry
                .map(|entry| entry.flag.clone())
                .unwrap_or_default(),
            memo: flag_entry.and_then(|entry| entry.memo.clone()),
        });
    }

    let summary = ProjectSummary {
        flagged_records: flags
            .values()
            .filter(|entry| !entry.flag.trim().is_empty())
            .count(),
        meta: meta.clone(),
    };

    Ok(LoadProjectResponse {
        project: summary,
        columns,
        rows,
        hidden_columns: meta.hidden_columns.clone(),
    })
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

    let mut circle_flags: Vec<i32> = vec![0; df.height()];
    let mut question_flags: Vec<i32> = vec![0; df.height()];
    let mut cross_flags: Vec<i32> = vec![0; df.height()];
    let mut memo_series: Vec<String> = vec![String::new(); df.height()];
    for (index, entry) in flags {
        if index < memo_series.len() {
            let trimmed_flag = entry.flag.trim();
            match trimmed_flag {
                "◯" => circle_flags[index] = 1,
                "?" => question_flags[index] = 1,
                "✗" => cross_flags[index] = 1,
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
    df.with_column(Series::new("trivium-circle", circle_flags))
        .map_err(|e| AppError::Message(format!("failed to append circle flag column: {}", e)))?;
    df.with_column(Series::new("trivium-question", question_flags))
        .map_err(|e| AppError::Message(format!("failed to append question flag column: {}", e)))?;
    df.with_column(Series::new("trivium-cross", cross_flags))
        .map_err(|e| AppError::Message(format!("failed to append cross flag column: {}", e)))?;
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
            update_flag,
            set_hidden_columns,
            export_project
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
