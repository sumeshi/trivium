use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    error::AppError,
    import::csv,
    import::jsonl,
    jobs::JobContext,
    models::{ColumnMeta, FlagEntry, ProjectMeta, ProjectSummary},
    project_io::{read_project_schema_columns, BatchDataWriter},
    storage::{save_column_metrics, save_flags},
    store::ProjectsStore,
};

use sha2::{Digest, Sha256};

use super::query::cache_remove_project;
mod load;
mod profile;

pub use load::{
    load_column_kinds_with_job, load_project, load_project_metadata_with_job, load_project_with_job,
};
use profile::{save_import_profile, KindAccumulator};

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

/// Result of parsing one source file into the staging Parquet file. CSV also
/// records the leading-sample width estimate so `load_project` does not scan
/// the full frame again.
struct ImportedSource {
    columns: Vec<ColumnMeta>,
    total_records: usize,
    flags: HashMap<usize, FlagEntry>,
    column_max_chars: Option<HashMap<String, usize>>,
    column_kinds: HashMap<String, String>,
}

/// Streams JSON/JSONL records through the fixed schema from pass 1 and writes
/// each bounded batch as its own Parquet row group.
fn import_json_batches(
    source: &Path,
    parquet_path: &Path,
    format: jsonl::RecordFormat,
    job: &JobContext,
) -> Result<ImportedSource, AppError> {
    let importer = jsonl::JsonImporter::open(source, format, jsonl::BatchLimits::default(), job)?;
    let columns = importer.columns().to_vec();
    let mut writer = BatchDataWriter::create(parquet_path, &columns).map_err(AppError::from)?;
    let ids = columns
        .iter()
        .map(|column| column.id.clone())
        .collect::<Vec<_>>();
    let mut kinds = KindAccumulator::default();
    let summary = importer.write_batches(job, |batch| {
        kinds.observe(batch, &ids, job)?;
        writer.write_batch(batch).map_err(AppError::from)
    })?;
    writer.finish().map_err(AppError::from)?;

    Ok(ImportedSource {
        columns: summary.columns,
        total_records: summary.rows,
        flags: summary.flags,
        column_max_chars: Some(summary.column_max_chars),
        column_kinds: kinds.finish(&ids),
    })
}

/// Copies the source into the staging directory while hashing the same bytes
/// with a fixed-size buffer. The source itself is opened read-only and is
/// never modified or moved.
fn copy_source_and_hash(
    source: &Path,
    destination: &Path,
    job: &JobContext,
) -> Result<CopiedSource, AppError> {
    job.check_cancelled()?;
    let mut input =
        File::open(source).map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
    let before = input
        .metadata()
        .map_err(|e| AppError::Message(format!("Failed to read file metadata: {}", e)))?;

    let mut output = File::create(destination)
        .map_err(|e| AppError::Message(format!("Failed to create temporary import file: {}", e)))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; HASH_BUFFER_SIZE];
    let mut size = 0u64;
    loop {
        job.check_cancelled()?;
        let read = input
            .read(&mut buffer)
            .map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read]).map_err(|e| {
            AppError::Message(format!("Failed to write temporary import file: {}", e))
        })?;
        hasher.update(&buffer[..read]);
        size += read as u64;
        job.report_progress(size, Some(before.len()), Some("Copying source".into()));
    }
    job.check_cancelled()?;
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
    let metas = store.try_all().map_err(AppError::from)?;
    let mut result = Vec::with_capacity(metas.len());
    for meta in metas {
        result.push(ProjectSummary { meta: meta.clone() });
    }
    result.sort_by_key(|summary| std::cmp::Reverse(summary.meta.created_at));
    Ok(result)
}

/// Creates a new project from a CSV, JSON, JSONL, or NDJSON file and persists
/// metadata plus optional flags.
pub fn create_project(
    store: &ProjectsStore,
    payload: CreateProjectPayload,
) -> Result<ProjectSummary, AppError> {
    create_project_with_job(store, payload, &JobContext::none())
}

/// [`create_project`] with cancellation checkpoints and progress reporting.
///
/// CSV import checks cancellation at every batch boundary. JSON/JSONL uses two
/// passes: pass 1 collects the column set and pass 2 writes bounded batches,
/// both observing cancellation.
pub fn create_project_with_job(
    store: &ProjectsStore,
    payload: CreateProjectPayload,
    job: &JobContext,
) -> Result<ProjectSummary, AppError> {
    job.check_cancelled()?;
    let source_path = PathBuf::from(&payload.path);
    if !source_path.exists() {
        return Err(AppError::Message("Selected file no longer exists.".into()));
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
    job.report_progress(0, Some(4), Some("Copying source".into()));
    let copied = copy_source_and_hash(&source_path, &staging.path().join(STAGED_SOURCE_FILE), job)?;
    job.check_cancelled()?;

    // Parse the copied snapshot, not the source path, so the hash recorded in
    // metadata always covers the bytes that were actually imported.
    job.report_progress(1, Some(4), Some("Importing data".into()));
    let parquet_path = staging.path().join("data.parquet");
    let imported = match source_format {
        "json" => import_json_batches(&copied.path, &parquet_path, jsonl::RecordFormat::Json, job)?,
        "jsonl" | "ndjson" => import_json_batches(
            &copied.path,
            &parquet_path,
            jsonl::RecordFormat::JsonLines,
            job,
        )?,
        _ => {
            // CSV is parsed in bounded batches and written one row group at a
            // time. Neither the source bytes nor all records stay in memory.
            let mut importer = csv::CsvImporter::open(
                &copied.path,
                csv::build_delimiter(payload.delimiter.as_deref())?,
                csv::BatchLimits::default(),
            )?
            .with_annotations(restore_annotations);
            let columns = importer.columns().to_vec();
            let mut writer =
                BatchDataWriter::create(&parquet_path, &columns).map_err(AppError::from)?;
            let ids = columns
                .iter()
                .map(|column| column.id.clone())
                .collect::<Vec<_>>();
            let mut kinds = KindAccumulator::default();
            while let Some(batch) = importer.next_batch(job)? {
                kinds.observe(&batch, &ids, job)?;
                writer.write_batch(&batch).map_err(AppError::from)?;
            }
            writer.finish().map_err(AppError::from)?;
            let summary = importer.into_summary();
            ImportedSource {
                columns: summary.columns,
                total_records: summary.rows,
                flags: summary.flags,
                column_max_chars: Some(summary.column_max_chars),
                column_kinds: kinds.finish(&ids),
            }
        }
    };
    job.check_cancelled()?;

    let metadata = ProjectMeta {
        id: project_id,
        name: source_path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "Imported Project".to_string()),
        description: payload.description.clone(),
        created_at: Utc::now(),
        total_records: imported.total_records,
        flagged_records: imported
            .flags
            .values()
            .filter(|entry| entry.is_flagged())
            .count(),
        ioc_applied_records: 0,
        source_sha256: Some(copied.sha256),
        source_file_name: source_path
            .file_name()
            .map(|name| name.to_string_lossy().to_string()),
        source_format: Some(source_format.to_string()),
        source_size: Some(copied.size),
        source_path: Some(
            fs::canonicalize(&source_path)
                .unwrap_or_else(|_| source_path.clone())
                .to_string_lossy()
                .to_string(),
        ),
        hidden_columns: Vec::new(),
        // New projects always use internal column ids and the `__original`
        // record column. Legacy version 0 projects are only read.
        storage_version: 2,
        columns: imported.columns,
    };

    if let Some(column_max_chars) = &imported.column_max_chars {
        save_column_metrics(&staging.path().join(COLUMN_METRICS_FILE), column_max_chars)
            .map_err(AppError::from)?;
    }

    let schema = read_project_schema_columns(&parquet_path).map_err(AppError::from)?;
    save_import_profile(
        staging.path(),
        metadata.total_records,
        &schema,
        imported.column_kinds,
        imported.column_max_chars.unwrap_or_default(),
    )?;

    let flags_path = staging.path().join("flags.json");
    if !imported.flags.is_empty() {
        save_flags(&flags_path, &imported.flags).map_err(AppError::from)?;
    }

    // The copied input is not part of the project. Remove it before the
    // completed staging directory is renamed into place.
    fs::remove_file(&copied.path)
        .map_err(|e| AppError::Message(format!("Failed to remove temporary import file: {}", e)))?;

    // Metadata belongs to the same staged case database as its settings and
    // annotations. Publishing the directory then exposes one complete case;
    // no post-rename registry write or crash window remains.
    crate::storage::insert_project_meta(staging.path(), &metadata).map_err(AppError::from)?;

    // Last cancellation point before the project becomes visible. A cancel
    // after this line is not observed; the import completes and is registered.
    job.report_progress(3, Some(4), Some("Creating project".into()));
    job.check_cancelled()?;

    let project_dir = store.project_dir(&project_id);
    if project_dir.exists() {
        return Err(AppError::Message(format!(
            "Project directory already exists: {:?}",
            project_dir
        )));
    }
    fs::rename(staging.path(), &project_dir)
        .with_context(|| format!("failed to move staged project to {:?}", project_dir))
        .map_err(AppError::from)?;
    staging.disarm();

    Ok(ProjectSummary { meta: metadata })
}

#[derive(Debug, Deserialize)]
pub struct ProjectRequest {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
}

/// Removes a project directory and clears related caches.
pub fn delete_project(store: &ProjectsStore, request: ProjectRequest) -> Result<(), AppError> {
    let Some(meta) = store
        .try_find(&request.project_id)
        .map_err(AppError::from)?
    else {
        return Ok(());
    };
    let project_dir = store.project_dir(&meta.id);
    // The scanned case index follows this atomic directory publication. A
    // failed rename leaves the entire case visible and unchanged. Connections
    // used by try_find have already closed; no database tombstone is written.
    let trash_root = store.trash_root();
    fs::create_dir_all(&trash_root)
        .with_context(|| {
            format!("failed to create deletion workspace {trash_root:?}; saved case was preserved")
        })
        .map_err(AppError::from)?;
    let trash_path = trash_root.join(format!("{}-{}", meta.id, Uuid::new_v4()));
    fs::rename(&project_dir, &trash_path)
        .with_context(|| format!("failed to move case to deletion workspace {trash_path:?}; saved case was preserved"))
        .map_err(AppError::from)?;
    cache_remove_project(&meta.id);

    // Logical deletion committed at the rename. Cleanup errors must not
    // report that a still-visible case exists or restore a partially removed
    // directory. Keep the exact owned trash path for explicit manual cleanup.
    if let Err(error) = fs::remove_dir_all(&trash_path) {
        eprintln!("[projects] case {} was deleted, but cleanup failed at {trash_path:?}: {error}. Remaining files are retained there for manual cleanup.", meta.id);
    }
    Ok(())
}
