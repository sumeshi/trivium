use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use polars::prelude::*;

use crate::error::AppError;
use crate::import::json_source::{for_each_record, value_to_cell, visit_leaves};
use crate::jobs::JobContext;
use crate::models::{ColumnMeta, FlagEntry, ImportedData};
use crate::project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID};
use crate::storage::COLUMN_WIDTH_SAMPLE_ROWS;

pub use crate::import::json_source::RecordFormat;

/// Maximum number of records in one import batch.
pub const DEFAULT_BATCH_ROWS: usize = 50_000;
/// Approximate payload bytes (cell text plus the `__original` JSON) in one
/// batch. A single record larger than the limit still forms a batch of its own.
pub const DEFAULT_BATCH_BYTES: usize = 8 * 1024 * 1024;
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

/// Internal column id assigned to a source path by its first appearance.
fn column_id(position: usize) -> String {
    format!("c{:06}", position + 1)
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
}

impl JsonImporter {
    /// Runs pass 1 and fixes the column set.
    pub fn open(
        path: &Path,
        format: RecordFormat,
        limits: BatchLimits,
        job: &JobContext,
    ) -> Result<Self, AppError> {
        job.check_cancelled()?;
        let metadata = fs::metadata(path)
            .map_err(|e| AppError::Message(format!("Failed to read file metadata: {}", e)))?;
        let mut order: Vec<String> = Vec::new();
        let mut seen: HashMap<String, usize> = HashMap::new();
        let mut rows = 0usize;

        for_each_record(path, format, |_record_no, value| {
            rows += 1;
            let mut paths = Vec::new();
            visit_leaves(&value, "", &mut |pointer, _| paths.push(pointer));
            for pointer in paths {
                if !seen.contains_key(&pointer) {
                    seen.insert(pointer.clone(), order.len());
                    order.push(pointer);
                }
            }
            if rows % PROGRESS_RECORD_INTERVAL == 0 {
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

        let mut cells: Vec<Vec<Option<String>>> = vec![Vec::new(); columns.len()];
        let mut originals: Vec<String> = Vec::new();
        let mut row_ids: Vec<i64> = Vec::new();
        let mut rows_read = 0usize;
        let mut batch_rows = 0usize;
        let mut estimated_bytes = 0usize;

        for_each_record(&path, format, |_record_no, value| {
            job.check_cancelled()?;
            let row_position = row_ids.len();
            for values in &mut cells {
                values.push(None);
            }
            visit_leaves(&value, "", &mut |pointer, leaf| {
                if let Some(index) = column_index.get(pointer.as_str()) {
                    cells[*index][row_position] = value_to_cell(leaf);
                }
            });

            let original = value.to_string();
            estimated_bytes += original.len() + 8;
            for values in &cells {
                if let Some(Some(text)) = values.last() {
                    estimated_bytes += text.len() + 1;
                }
            }

            // The initial table layout only needs a leading sample, matching
            // the CSV importer.
            if rows_read < sample_rows {
                for (position, values) in cells.iter().enumerate() {
                    let length = values
                        .last()
                        .and_then(|value| value.as_ref())
                        .map(|text| text.chars().count())
                        .unwrap_or(0);
                    if let Some(current) = column_max_chars.get_mut(&columns[position].id) {
                        if length > *current {
                            *current = length;
                        }
                    }
                }
            }

            originals.push(original);
            row_ids.push(rows_read as i64);
            rows_read += 1;
            batch_rows += 1;

            if batch_rows >= limits.max_rows || estimated_bytes >= limits.max_bytes {
                let frame = build_batch(&columns, &mut cells, &mut originals, &mut row_ids)?;
                write(&frame)?;
                job.report_progress(rows_read as u64, None, Some("Reading JSON".into()));
                batch_rows = 0;
                estimated_bytes = 0;
            }
            Ok(())
        })?;

        if batch_rows > 0 {
            let frame = build_batch(&columns, &mut cells, &mut originals, &mut row_ids)?;
            write(&frame)?;
            job.report_progress(rows_read as u64, None, Some("Reading JSON".into()));
        }

        if rows_read != expected_rows {
            return Err(AppError::Message(
                "The JSON record count changed between import passes.".into(),
            ));
        }

        Ok(JsonImportSummary {
            columns,
            rows: rows_read,
            flags: HashMap::new(),
            column_max_chars,
        })
    }
}

/// Builds one frame from the accumulated column values. The vectors are taken,
/// leaving them empty for the next batch.
fn build_batch(
    columns: &[ColumnMeta],
    cells: &mut [Vec<Option<String>>],
    originals: &mut Vec<String>,
    row_ids: &mut Vec<i64>,
) -> Result<DataFrame, AppError> {
    let mut series: Vec<Series> = Vec::with_capacity(columns.len() + 2);
    for (column, values) in columns.iter().zip(cells.iter_mut()) {
        series.push(Series::new(column.id.as_str(), std::mem::take(values)));
    }
    series.push(Series::new(
        INTERNAL_ORIGINAL,
        std::mem::take(originals),
    ));
    series.push(Series::new(INTERNAL_ROW_ID, std::mem::take(row_ids)));
    DataFrame::new(series)
        .map_err(|_| AppError::Message("Failed to build the JSON dataset.".into()))
}

fn empty_frame(columns: &[ColumnMeta]) -> Result<DataFrame, AppError> {
    let mut series: Vec<Series> = columns
        .iter()
        .map(|column| Series::new(column.id.as_str(), Vec::<Option<String>>::new()))
        .collect();
    series.push(Series::new(INTERNAL_ORIGINAL, Vec::<String>::new()));
    series.push(Series::new(INTERNAL_ROW_ID, Vec::<i64>::new()));
    DataFrame::new(series)
        .map_err(|_| AppError::Message("Failed to build the JSON dataset.".into()))
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
