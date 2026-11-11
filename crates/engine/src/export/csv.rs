use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::annotations;
use crate::{
    error::AppError,
    jobs::JobContext,
    models::{FlagEntry, ProjectMeta},
    ops::query::{data_columns, ordered_row_indices, resolve_target},
    project_io::{is_internal_column_for_version, read_project_dataframe, ProjectColumns},
    storage::load_flags,
    store::ProjectsStore,
};

/// Rows written per CSV batch. Cancellation is checked between batches.
const EXPORT_BATCH_ROWS: usize = 10_000;

/// Export selection.
///
/// `scope = "filtered"` (default) exports the rows selected by the flattened
/// `QuerySpec`; `"all"` exports every row in the requested sort order;
/// `"selected"` intersects `rowIndices` with the `QuerySpec` result and keeps
/// the result order. Input/output column restriction is `columns`; search scope
/// is `QuerySpec.searchColumns`.
///
/// `includeAnnotations = true` (default) adds the independent `trivium-*`
/// columns. `false` writes only data columns.
#[derive(Debug, Deserialize)]
pub struct ExportProjectPayload {
    #[serde(rename = "projectId")]
    pub project_id: Uuid,
    pub destination: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(flatten)]
    pub spec: crate::ops::query::QuerySpec,
    /// Output only these base columns (derived trivium columns are kept).
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Required when `scope = "selected"`. Duplicates are ignored and ids are
    /// emitted in the applied `QuerySpec` order.
    #[serde(default, rename = "rowIndices")]
    pub row_indices: Option<Vec<usize>>,
    /// Whether to add the derived annotation columns.
    #[serde(default, rename = "includeAnnotations")]
    pub include_annotations: Option<bool>,
}

/// Result of one export.
#[derive(Debug, Clone, Serialize)]
pub struct ExportResult {
    /// Number of data rows written (excluding the header).
    pub rows: usize,
    /// Header labels in output order.
    pub columns: Vec<String>,
    /// Non-fatal notes, such as an annotation column renamed to avoid a
    /// collision with a data column.
    pub warnings: Vec<String>,
}

/// Exports the project data with derived trivium columns to a CSV file.
pub fn export_project(
    store: &ProjectsStore,
    payload: ExportProjectPayload,
) -> Result<ExportResult, AppError> {
    export_project_with_job(store, payload, &JobContext::none())
}

/// [`export_project`] with cancellation checkpoints and progress reporting.
///
/// The CSV is written to a sibling temporary file and renamed over the
/// destination only after every batch, the flush, and a final cancellation
/// check succeed. A failure or cancellation leaves the previous destination
/// untouched and removes the temporary file.
pub fn export_project_with_job(
    store: &ProjectsStore,
    payload: ExportProjectPayload,
    job: &JobContext,
) -> Result<ExportResult, AppError> {
    job.check_cancelled()?;
    let meta = store
        .find(&payload.project_id)
        .ok_or_else(|| AppError::Message("Project not found.".into()))?;
    let destination = PathBuf::from(&payload.destination);
    ensure_destination_is_not_source(&destination, &meta)?;

    let project_dir = store.project_dir(&meta.id);
    let parquet_path = project_dir.join("data.parquet");
    job.report_progress(0, Some(3), Some("Reading project data".into()));
    let mut df = read_project_dataframe(&parquet_path).map_err(AppError::from)?;
    job.check_cancelled()?;
    let flags_path = store.flags_path(&meta.id);
    let flags = load_flags(&flags_path).map_err(AppError::from)?;
    let project_columns = ProjectColumns::from_meta(&meta, &df);
    let all_columns = data_columns(&df, meta.storage_version);

    job.report_progress(1, Some(3), Some("Resolving export rows".into()));
    let scope = payload.scope.as_deref().unwrap_or("filtered");
    let final_indices = resolve_export_rows(&df, scope, &payload, &all_columns, &flags, job)?;
    job.check_cancelled()?;

    // Apply the output column restriction after the target is fixed. Only the
    // internal storage columns are removed; input columns that happen to be
    // named `flag` or `memo` are ordinary data.
    if let Some(columns) = &payload.columns {
        let selected: Vec<String> = columns
            .iter()
            .filter(|name| all_columns.contains(name))
            .cloned()
            .collect();
        df = df
            .select(&selected)
            .map_err(|e| AppError::Message(format!("Failed to select columns: {}", e)))?;
    }
    let internal_columns: Vec<String> = df
        .get_column_names()
        .iter()
        .filter(|name| is_internal_column_for_version(name, meta.storage_version))
        .map(|name| name.to_string())
        .collect();
    for internal in internal_columns {
        if let Ok(next) = df.drop(&internal) {
            df = next;
        }
    }

    let data_headers: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|name| project_columns.label(name))
        .collect();
    let mut warnings = Vec::new();
    let include_annotations = payload.include_annotations.unwrap_or(true);
    let annotation_plan = if include_annotations {
        annotations::plan_annotation_columns(&data_headers, &df, &mut warnings)
    } else {
        Vec::new()
    };

    let annotation_data = if include_annotations {
        Some(annotations::build_annotations(&df, &flags))
    } else {
        None
    };

    let mut out_cols: Vec<Series> = Vec::new();
    if let Some(annotation_data) = annotation_data {
        out_cols.extend(annotations::annotation_series(
            annotation_data,
            &annotation_plan,
        ));
    }
    for name in df.get_column_names() {
        if let Ok(series) = df.column(name) {
            out_cols.push(series.clone());
        }
    }
    df = DataFrame::new(out_cols).map_err(|e| AppError::Other(e.into()))?;

    let index_column = IdxCa::from_vec(
        "",
        final_indices.iter().map(|idx| *idx as IdxSize).collect(),
    );
    df = df
        .take(&index_column)
        .map_err(|e| AppError::Message(format!("Failed to select export rows: {}", e)))?;

    let headers: Vec<String> = annotation_plan
        .iter()
        .map(|planned| planned.header.clone())
        .chain(data_headers)
        .collect();
    let rows = df.height();

    job.report_progress(2, Some(3), Some("Writing CSV".into()));
    write_csv_atomically(&destination, &headers, &mut df, job)?;

    job.report_progress(3, Some(3), Some("Export complete".into()));
    Ok(ExportResult {
        rows,
        columns: headers,
        warnings,
    })
}

fn resolve_export_rows(
    df: &DataFrame,
    scope: &str,
    payload: &ExportProjectPayload,
    all_columns: &[String],
    flags: &std::collections::HashMap<usize, FlagEntry>,
    job: &JobContext,
) -> Result<Vec<usize>, AppError> {
    match scope {
        "all" => Ok(ordered_row_indices(
            df,
            payload.spec.sort_key.as_deref(),
            payload.spec.sort_direction.as_deref(),
        )),
        "filtered" => Ok(resolve_target(df, &payload.spec, all_columns, flags, job)?.filtered),
        "selected" => {
            let requested = payload
                .row_indices
                .as_ref()
                .ok_or_else(|| AppError::Message("Selected export requires row indices.".into()))?;
            if requested.is_empty() {
                return Err(AppError::Message(
                    "Select at least one row to export.".into(),
                ));
            }
            if let Some(invalid) = requested.iter().find(|idx| **idx >= df.height()) {
                return Err(AppError::Message(format!(
                    "Selected row id {} is out of range.",
                    invalid
                )));
            }
            let selected: HashSet<usize> = requested.iter().copied().collect();
            // The QuerySpec result is the base order. Refuse a selection that
            // is no longer inside that result instead of silently writing a
            // smaller file while the dialog still shows the selected count.
            let result: Vec<usize> =
                resolve_target(df, &payload.spec, all_columns, flags, job)?
                    .filtered
                    .into_iter()
                    .filter(|idx| selected.contains(idx))
                    .collect();
            let outside: Vec<usize> = selected
                .iter()
                .copied()
                .filter(|idx| !result.contains(idx))
                .collect();
            if !outside.is_empty() {
                return Err(AppError::Message(format!(
                    "Selected rows are no longer in the current result: {:?}. Select again.",
                    outside
                )));
            }
            Ok(result)
        }
        other => Err(AppError::Message(format!(
            "Unknown export scope: {}.",
            other
        ))),
    }
}

fn canonical_path(path: &Path) -> Option<PathBuf> {
    if let Ok(canonical) = fs::canonicalize(path) {
        return Some(canonical);
    }
    let file_name = path.file_name()?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Some(fs::canonicalize(parent).ok()?.join(file_name))
}

fn ensure_destination_is_not_source(
    destination: &Path,
    meta: &ProjectMeta,
) -> Result<(), AppError> {
    let Some(source) = &meta.source_path else {
        return Ok(());
    };
    let source = PathBuf::from(source);
    let source = fs::canonicalize(&source).unwrap_or(source);
    let Some(destination) = canonical_path(destination) else {
        return Ok(());
    };
    if destination == source {
        return Err(AppError::Message(
            "Choose a different file name: the destination is the source evidence file.".into(),
        ));
    }
    Ok(())
}

/// Removes the temporary file on every path that does not commit it.
struct PendingFile {
    path: PathBuf,
    keep: bool,
}

impl PendingFile {
    fn new(path: PathBuf) -> Self {
        Self { path, keep: true }
    }

    fn disarm(&mut self) {
        self.keep = false;
    }
}

impl Drop for PendingFile {
    fn drop(&mut self) {
        if self.keep {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn write_csv_atomically(
    destination: &Path,
    headers: &[String],
    df: &mut DataFrame,
    job: &JobContext,
) -> Result<(), AppError> {
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create export dir {:?}", parent))
        .map_err(AppError::from)?;
    let file_name = destination
        .file_name()
        .ok_or_else(|| AppError::Message("Export destination has no file name.".into()))?;
    let temp_path = parent.join(format!(
        "{}.tmp.{}",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    let mut pending = PendingFile::new(temp_path.clone());

    let file = File::create(&temp_path)
        .with_context(|| format!("failed to create export file {:?}", temp_path))
        .map_err(AppError::from)?;
    let mut writer = BufWriter::new(file);

    // Polars rejects duplicate DataFrame column names, so write the header
    // separately to keep duplicate source display labels intact.
    let mut header_bytes = Vec::new();
    {
        let mut header_writer = csv::Writer::from_writer(&mut header_bytes);
        header_writer
            .write_record(headers)
            .context("failed to write export CSV header")?;
        header_writer
            .flush()
            .context("failed to flush export CSV header")?;
    }
    writer
        .write_all(&header_bytes)
        .context("failed to write export CSV header")?;

    let mut written = 0usize;
    while written < df.height() {
        job.check_cancelled()?;
        let len = EXPORT_BATCH_ROWS.min(df.height() - written);
        let mut batch = df.slice(written as i64, len);
        CsvWriter::new(&mut writer)
            .include_header(false)
            .finish(&mut batch)
            .context("failed to write export CSV")
            .map_err(AppError::from)?;
        written += len;
    }
    writer
        .flush()
        .context("failed to flush export CSV")
        .map_err(AppError::from)?;
    drop(writer);

    // The last cancellation point before the existing destination changes.
    job.check_cancelled()?;
    fs::rename(&temp_path, destination)
        .with_context(|| format!("failed to finalize export file {:?}", destination))
        .map_err(AppError::from)?;
    pending.disarm();
    Ok(())
}
