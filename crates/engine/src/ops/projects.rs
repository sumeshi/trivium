use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    analysis::infer_column_kinds,
    error::AppError,
    import::csv,
    import::json,
    import::jsonl,
    ioc::{apply_ioc_outcomes_to_rows, cached_ioc_outcomes, load_ioc_entries},
    models::{LoadProjectResponse, ProjectMeta, ProjectSummary},
    project_io::{read_project_dataframe, write_project_dataframe, ProjectColumns},
    storage::{
        clear_ioc_flag_cache, clear_searchable_cache, compute_column_max_chars,
        load_color_rules, load_column_metrics, load_flags, save_column_metrics, save_flags,
    },
    store::ProjectsStore,
};

use sha2::{Digest, Sha256};

use super::util::{materialize_rows, DEFAULT_PAGE_SIZE};

const COLUMN_METRICS_FILE: &str = "column_max_chars.json";
/// Fixed-size streaming buffer. The whole source is never held in memory just
/// to compute the hash.
const HASH_BUFFER_SIZE: usize = 64 * 1024;
/// Name of the copied source inside the staging directory. The parsers read
/// these bytes, so the hash and the parsed content are the same snapshot.
const STAGED_SOURCE_FILE: &str = "source";

/// Removes an in-progress staging directory on every exit path, including a
/// panic. It is disarmed only after the completed directory has been renamed
/// into place.
struct StagingDir {
    path: PathBuf,
    keep: bool,
}

impl StagingDir {
    fn create(path: PathBuf) -> Result<Self, AppError> {
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create staging dir {:?}", path))
            .map_err(AppError::from)?;
        Ok(Self { path, keep: true })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn disarm(&mut self) {
        self.keep = false;
    }
}

impl Drop for StagingDir {
    fn drop(&mut self) {
        if self.keep {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

struct CopiedSource {
    path: PathBuf,
    sha256: String,
    size: u64,
}

/// Copies the source into the staging directory while hashing the same bytes
/// with a fixed-size buffer. The source itself is opened read-only and is
/// never modified or moved.
fn copy_source_and_hash(source: &Path, destination: &Path) -> Result<CopiedSource, AppError> {
    let mut input = File::open(source)
        .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let before = input
        .metadata()
        .map_err(|e| AppError::Message(format!("Failed to read file metadata: {}", e)))?;

    let mut output = File::create(destination).map_err(|e| {
        AppError::Message(format!("Failed to create temporary import file: {}", e))
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; HASH_BUFFER_SIZE];
    let mut size = 0u64;
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(|e| AppError::Message(format!("Failed to write temporary import file: {}", e)))?;
        hasher.update(&buffer[..read]);
        size += read as u64;
    }
    drop(output);

    // Detect a source that changed between the copy start and end. The parser
    // reads the copied bytes, so a changed source must not be recorded as the
    // imported snapshot.
    let after = fs::metadata(source)
        .map_err(|e| AppError::Message(format!("Failed to read file metadata: {}", e)))?;
    let modified_changed = match (before.modified(), after.modified()) {
        (Ok(before), Ok(after)) => before != after,
        _ => false,
    };
    if before.len() != after.len() || size != before.len() || modified_changed {
        return Err(AppError::Message(
            "Source file changed during import. Try again.".into(),
        ));
    }

    Ok(CopiedSource {
        path: destination.to_path_buf(),
        sha256: format!("{:x}", hasher.finalize()),
        size,
    })
}

#[derive(Debug, Deserialize)]
pub struct CreateProjectPayload {
    pub path: String,
    pub description: Option<String>,
    #[serde(default)]
    pub delimiter: Option<String>,
    /// Only an explicit Trivium export re-import lifts `trivium-*` columns into
    /// annotations. The columns themselves stay in the data either way.
    #[serde(default)]
    pub restore_annotations: Option<bool>,
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

/// Creates a new project from a CSV, JSON, JSONL, or NDJSON file and persists
/// metadata plus optional flags.
pub fn create_project(
    store: &ProjectsStore,
    payload: CreateProjectPayload,
) -> Result<ProjectSummary, AppError> {
    let source_path = PathBuf::from(&payload.path);
    if !source_path.exists() {
        return Err(AppError::Message("Selected file no longer exists.".into()).into());
    }

    let restore_annotations = payload.restore_annotations.unwrap_or(false);
    // The extension decides the parser; a malformed JSON/JSONL file must fail
    // instead of falling back to the CSV reader.
    let extension = source_path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    let source_format = match extension.as_deref() {
        Some("json") => "json",
        Some("jsonl") => "jsonl",
        Some("ndjson") => "ndjson",
        _ => "csv",
    };

    let project_id = Uuid::new_v4();
    let mut staging = StagingDir::create(store.staging_dir(&project_id))?;
    let copied = copy_source_and_hash(&source_path, &staging.path().join(STAGED_SOURCE_FILE))?;

    // Parse the copied snapshot, not the source path, so the hash recorded in
    // metadata always covers the bytes that were actually imported.
    let imported = match source_format {
        "json" => json::load(&copied.path)?,
        "jsonl" | "ndjson" => jsonl::load(&copied.path)?,
        _ => csv::load(
            &copied.path,
            csv::build_delimiter(payload.delimiter.as_deref())?,
            restore_annotations,
        )?,
    };
    let mut df = imported.frame;
    let imported_flags = imported.flags;

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
        source_sha256: Some(copied.sha256),
        source_file_name: source_path
            .file_name()
            .map(|name| name.to_string_lossy().to_string()),
        source_format: Some(source_format.to_string()),
        source_size: Some(copied.size),
        hidden_columns: Vec::new(),
        // New projects always use internal column ids and the `__original`
        // record column. Legacy version 0 projects are only read.
        storage_version: 2,
        columns: imported.columns,
    };

    let parquet_path = staging.path().join("data.parquet");
    write_project_dataframe(&parquet_path, &mut df).map_err(AppError::from)?;

    let flags_path = staging.path().join("flags.json");
    if !imported_flags.is_empty() {
        save_flags(&flags_path, &imported_flags).map_err(AppError::from)?;
    }

    // The copied input is not part of the project. Remove it before the
    // completed staging directory is renamed into place.
    fs::remove_file(&copied.path)
        .map_err(|e| AppError::Message(format!("Failed to remove temporary import file: {}", e)))?;

    let project_dir = store.project_dir(&project_id);
    if project_dir.exists() {
        return Err(AppError::Message(format!(
            "Project directory already exists: {:?}",
            project_dir
        ))
        .into());
    }
    fs::rename(staging.path(), &project_dir)
        .with_context(|| format!("failed to move staged project to {:?}", project_dir))
        .map_err(AppError::from)?;
    staging.disarm();

    // Register the project only after its data, metadata, and directory are
    // complete. If registration fails, do not leave a project-looking folder.
    if let Err(err) = store.insert(metadata.clone()) {
        let _ = fs::remove_dir_all(&project_dir);
        return Err(AppError::from(err));
    }

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
    let project_columns = ProjectColumns::from_meta(&meta, &df);
    let columns = project_columns.ids();

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

    // Like query coloring, a broken colors.json must not block project loading.
    let color_rules = match load_color_rules(&project_dir) {
        Ok(rules) => rules.unwrap_or_default(),
        Err(err) => {
            eprintln!(
                "[colors] failed to load color rules for {:?}: {:?}",
                project_dir, err
            );
            Vec::new()
        }
    };

    let page_limit = usize::min(DEFAULT_PAGE_SIZE, df.height());
    let mut initial_rows =
        materialize_rows(&df, project_columns.columns(), 0..page_limit, &flags);
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
        columns_meta: project_columns.columns().to_vec(),
        hidden_columns: meta.hidden_columns.clone(),
        column_max_chars,
        column_kinds: infer_column_kinds(&df)
            .into_iter()
            .map(|(name, kind)| (name, kind.as_str().to_string()))
            .collect(),
        iocs,
        color_rules,
        initial_rows,
    })
}
