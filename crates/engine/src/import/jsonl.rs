use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use polars::prelude::*;

use crate::error::AppError;
use crate::import::json_source::{for_each_record, value_to_cell, visit_fields, visit_leaves};
use crate::import::options::{ImportOptions, ImportReport};
use crate::import::{column_id, empty_frame};
use crate::jobs::JobContext;
use crate::models::{ColumnMeta, FlagEntry, ImportedData};
use crate::storage::COLUMN_WIDTH_SAMPLE_ROWS;

pub use crate::import::json_source::RecordFormat;
mod batch;
mod parent_cells;
use batch::{Batch, Record};
use parent_cells::ParentCells;

/// Maximum number of records in one import batch.
pub const DEFAULT_BATCH_ROWS: usize = 50_000;
/// Retained batch buffer budget, including missing-cell slots, vector capacity
/// and string capacity. Parsing, schema and the emitted Polars frame use extra
/// memory. A single oversized record still forms a batch of its own.
pub const DEFAULT_BATCH_BYTES: usize = 8 * 1024 * 1024;
/// Bound dense schema overhead before any row-by-column buffers are allocated.
pub const MAX_JSON_COLUMNS: usize = 4_096;
/// How many scanned records pass between progress reports during pass 1.
const PROGRESS_RECORD_INTERVAL: usize = 1_024;

/// Batch sizing for one JSON/JSONL import.
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_BATCH_ROWS,
            max_bytes: DEFAULT_BATCH_BYTES,
        }
    }
}

impl BatchLimits {
    fn normalized(self) -> Self {
        Self {
            max_rows: self.max_rows.max(1),
            max_bytes: self.max_bytes.max(1),
        }
    }
}

/// Display label derived from an absolute JSON Pointer. The leading slash is
/// dropped, but escapes are kept so `/a.b` and `/a/b` stay distinguishable.
fn pointer_label(pointer: &str) -> String {
    pointer.strip_prefix('/').unwrap_or(pointer).to_string()
}

/// Summary of a completed streaming JSON import.
pub struct JsonImportSummary {
    pub columns: Vec<ColumnMeta>,
    pub rows: usize,
    pub flags: HashMap<usize, FlagEntry>,
    pub column_max_chars: HashMap<String, usize>,
    pub report: ImportReport,
}

/// Two-pass JSON/JSONL importer.
///
/// Pass 1 scans the file to collect the JSON Pointer column union in first
/// appearance order and counts records. Pass 2 re-reads the same file and fills
/// bounded batches against that fixed schema. Neither pass keeps the parsed
/// records. `create_project_with_job` hashes and stages the source before this
/// importer runs; the size check before pass 2 rejects a changed snapshot.
pub struct JsonImporter {
    path: PathBuf,
    format: RecordFormat,
    columns: Vec<ColumnMeta>,
    column_index: HashMap<String, usize>,
    limits: BatchLimits,
    expected_size: u64,
    expected_rows: usize,
    column_max_chars: HashMap<String, usize>,
    sample_rows: usize,
    recover: bool,
    report: ImportReport,
}

impl JsonImporter {
    /// Runs pass 1 and fixes the column set.
    pub fn open(
        path: &Path,
        format: RecordFormat,
        limits: BatchLimits,
        job: &JobContext,
    ) -> Result<Self, AppError> {
        Self::open_with_options(path, format, limits, &ImportOptions::default(), job)
    }

    pub fn open_with_options(
        path: &Path,
        format: RecordFormat,
        limits: BatchLimits,
        options: &ImportOptions,
        job: &JobContext,
    ) -> Result<Self, AppError> {
        job.check_cancelled()?;
        let metadata = fs::metadata(path)
            .map_err(|e| AppError::Message(format!("Failed to read file metadata: {}", e)))?;
        let mut order: Vec<String> = Vec::new();
        let mut seen: HashMap<String, usize> = HashMap::new();
        let mut rows = 0usize;

        let report = for_each_record(path, format, options.recover, job, |record_no, value| {
            rows += 1;
            let mut too_wide = false;
            visit_leaves(&value, "", &mut |pointer, _| {
                if !too_wide && !seen.contains_key(&pointer) {
                    if order.len() == MAX_JSON_COLUMNS {
                        too_wide = true;
                        return;
                    }
                    seen.insert(pointer.clone(), order.len());
                    order.push(pointer);
                }
            });
            if too_wide {
                return Err(AppError::Message(format!(
                    "JSON record {} exceeds the limit of {} flattened columns. No columns were discarded.",
                    record_no, MAX_JSON_COLUMNS
                )));
            }
            if rows.is_multiple_of(PROGRESS_RECORD_INTERVAL) {
                job.report_progress(rows as u64, None, Some("Scanning JSON".into()));
            }
            job.check_cancelled()?;
            Ok(())
        })?;
        job.check_cancelled()?;

        let columns: Vec<ColumnMeta> = order
            .iter()
            .enumerate()
            .map(|(position, pointer)| ColumnMeta {
                id: column_id(position),
                label: pointer_label(pointer),
                source: pointer.clone(),
                order: position,
            })
            .collect();
        let column_index = order
            .iter()
            .enumerate()
            .map(|(position, pointer)| (pointer.clone(), position))
            .collect();
        let column_max_chars = columns
            .iter()
            .map(|column| (column.id.clone(), column.id.chars().count()))
            .collect();

        Ok(Self {
            path: path.to_path_buf(),
            format,
            columns,
            column_index,
            limits: limits.normalized(),
            expected_size: metadata.len(),
            expected_rows: rows,
            column_max_chars,
            sample_rows: COLUMN_WIDTH_SAMPLE_ROWS,
            recover: options.recover,
            report,
        })
    }

    pub fn columns(&self) -> &[ColumnMeta] {
        &self.columns
    }

    /// Runs pass 2 and hands each bounded frame to `write`.
    ///
    /// Values follow the existing JSONL rules: numbers keep their source text,
    /// booleans become `true`/`false`, null and an absent key stay null, arrays
    /// and objects stay JSON text in one cell, and `__original` keeps the whole
    /// record. Cancellation is checked per record, so a request made from the
    /// previous batch's progress callback stops at the next boundary.
    pub fn write_batches<F>(
        self,
        job: &JobContext,
        mut write: F,
    ) -> Result<JsonImportSummary, AppError>
    where
        F: FnMut(&DataFrame) -> Result<(), AppError>,
    {
        let JsonImporter {
            path,
            format,
            columns,
            column_index,
            limits,
            expected_size,
            expected_rows,
            mut column_max_chars,
            sample_rows,
            recover,
            report,
        } = self;

        job.check_cancelled()?;
        let current_size = fs::metadata(&path)
            .map_err(|e| AppError::Message(format!("Failed to read file metadata: {}", e)))?
            .len();
        if current_size != expected_size {
            return Err(AppError::Message(
                "Input file changed between import passes. Try again.".into(),
            ));
        }

        let mut batch = Batch::new(columns.len());
        let mut rows_read = 0usize;

        let second_report = for_each_record(&path, format, recover, job, |record_no, value| {
            job.check_cancelled()?;
            let mut cells = Vec::new();
            let mut parents = ParentCells::new(DEFAULT_BATCH_BYTES);
            let mut cell_error = None;
            visit_fields(&value, "", &mut |pointer, field| {
                if cell_error.is_some() {
                    return;
                }
                if let Some(index) = column_index.get(pointer) {
                    // These object values were formerly absent from the leaf
                    // batch. Bound only this additional duplicated text; the
                    // existing oversized scalar/array record contract stays.
                    let cell = if matches!(field, serde_json::Value::Object(map) if !map.is_empty())
                    {
                        match parents.serialize(field, record_no) {
                            Ok(text) => Some(text),
                            Err(error) => {
                                cell_error = Some(error);
                                return;
                            }
                        }
                    } else {
                        value_to_cell(field)
                    };
                    cells.push((*index, cell));
                }
            });
            if let Some(error) = cell_error {
                return Err(error);
            }
            let record = Record {
                cells,
                original: value.to_string(),
                row_id: rows_read as i64,
                // Physical JSONL line or 1-based JSON array element.
                source_record: record_no as i64,
            };

            // The initial table layout only needs a leading sample, matching
            // the CSV importer.
            if rows_read < sample_rows {
                for (position, text) in &record.cells {
                    if let (Some(current), Some(text)) = (
                        column_max_chars.get_mut(&columns[*position].id),
                        text.as_ref(),
                    ) {
                        *current = (*current).max(text.chars().count());
                    }
                }
            }

            if batch.len() > 0 && batch.would_exceed(&record, limits.max_bytes) {
                let frame = batch.take_frame(&columns)?;
                write(&frame)?;
                job.report_progress(rows_read as u64, None, Some("Reading JSON".into()));
                job.check_cancelled()?;
            }
            batch.push(record)?;
            rows_read += 1;
            if batch.len() >= limits.max_rows || batch.allocated_bytes() >= limits.max_bytes {
                let frame = batch.take_frame(&columns)?;
                write(&frame)?;
                job.report_progress(rows_read as u64, None, Some("Reading JSON".into()));
                job.check_cancelled()?;
            }
            Ok(())
        })?;

        if batch.len() > 0 {
            let frame = batch.take_frame(&columns)?;
            write(&frame)?;
            job.report_progress(rows_read as u64, None, Some("Reading JSON".into()));
        }

        if rows_read != expected_rows || second_report != report {
            return Err(AppError::Message(
                "The JSON record count changed between import passes.".into(),
            ));
        }

        Ok(JsonImportSummary {
            columns,
            rows: rows_read,
            flags: HashMap::new(),
            column_max_chars,
            report,
        })
    }
}

/// Reads a whole JSONL file into one frame.
///
/// Production import uses [`JsonImporter`] and writes each batch, so this
/// convenience path exists for direct callers and tests.
pub fn load(path: &Path) -> Result<ImportedData, AppError> {
    load_with_format(path, RecordFormat::JsonLines)
}

/// Shared frame-loading wrapper for `json::load` and `jsonl::load`.
pub(crate) fn load_with_format(
    path: &Path,
    format: RecordFormat,
) -> Result<ImportedData, AppError> {
    let job = JobContext::none();
    let importer = JsonImporter::open(path, format, BatchLimits::default(), &job)?;
    let mut frame: Option<DataFrame> = None;
    let summary = importer.write_batches(&job, |batch| {
        frame = Some(match frame.take() {
            None => batch.clone(),
            Some(mut accumulated) => {
                accumulated.vstack_mut(batch).map_err(|_| {
                    AppError::Message(
                        "The JSON record count changed while building the dataset.".into(),
                    )
                })?;
                accumulated
            }
        });
        Ok(())
    })?;

    let frame = match frame {
        Some(frame) => frame,
        None => empty_frame(&summary.columns)?,
    };

    Ok(ImportedData {
        frame,
        columns: summary.columns,
        flags: summary.flags,
    })
}
