use std::fs;
use std::path::PathBuf;

use anyhow::Context;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    error::AppError,
    import::csv,
    ioc::{apply_ioc_outcomes_to_rows, cached_ioc_outcomes, load_ioc_entries},
    models::{LoadProjectResponse, ProjectMeta, ProjectSummary},
    project_io::{read_project_dataframe, write_project_dataframe},
    storage::{
        clear_ioc_flag_cache, clear_searchable_cache, compute_column_max_chars,
        load_column_metrics, load_flags, save_column_metrics, save_flags,
    },
    store::ProjectsStore,
};

use super::util::{materialize_rows, DEFAULT_PAGE_SIZE};

const COLUMN_METRICS_FILE: &str = "column_max_chars.json";

#[derive(Debug, Deserialize)]
pub struct CreateProjectPayload {
    pub path: String,
    pub description: Option<String>,
}

/// Lists saved projects ordered by creation time (newest first).
pub fn list_projects(store: &ProjectsStore) -> Result<Vec<ProjectSummary>, AppError> {
    println!("[debug] list_projects called");
    let metas = store.all();
    let mut result = Vec::with_capacity(metas.len());
    for meta in metas {
        result.push(ProjectSummary { meta: meta.clone() });
    }
    result.sort_by(|a, b| b.meta.created_at.cmp(&a.meta.created_at));
    Ok(result)
}

/// Creates a new project from a CSV file and persists metadata plus optional flags.
pub fn create_project(
    store: &ProjectsStore,
    payload: CreateProjectPayload,
) -> Result<ProjectSummary, AppError> {
    let source_path = PathBuf::from(&payload.path);
    if !source_path.exists() {
        return Err(AppError::Message("Selected file no longer exists.".into()).into());
    }

    let (mut df, imported_flags) = csv::load(&source_path)?;

    let project_id = Uuid::new_v4();
    let project_dir = store.project_dir(&project_id);
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

    store
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
pub fn delete_project(store: &ProjectsStore, request: ProjectRequest) -> Result<(), AppError> {
    let Some(meta) = store.find(&request.project_id) else {
        return Ok(());
    };
    let project_dir = store.project_dir(&meta.id);
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
    store.remove(&meta.id).map_err(AppError::from)?;
    Ok(())
}

/// Loads project metadata, initial rows, IOC entries, and column metrics.
pub fn load_project(
    store: &ProjectsStore,
    request: ProjectRequest,
) -> Result<LoadProjectResponse, AppError> {
    let meta = store
        .find(&request.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let project_dir = store.project_dir(&meta.id);
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
    let ioc_outcomes = cached_ioc_outcomes(&project_dir, &df, &iocs);

    let page_limit = usize::min(DEFAULT_PAGE_SIZE, df.height());
    let mut initial_rows = materialize_rows(&df, &columns, 0..page_limit, &flags);
    apply_ioc_outcomes_to_rows(&mut initial_rows, &ioc_outcomes);

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
