use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use polars::prelude::*;

use crate::annotation_columns::{leading_block, ANNOTATION_COLUMNS};
use crate::error::AppError;
use crate::import::diagnostics::{self, IssueSink};
use crate::import::empty_frame;
use crate::import::options::{ImportIssueAction, ImportOptions, ImportReport};
use crate::jobs::JobContext;
use crate::models::{ColumnMeta, FlagEntry, ImportedData};
use crate::project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD};
use crate::storage::COLUMN_WIDTH_SAMPLE_ROWS;

mod annotations;
mod batch_budget;
mod reader;
use annotations::{flag_entry, FlagColumns};

/// Maximum number of records in one import batch. The byte limit can stop a
/// batch earlier; the row limit keeps narrow tables from growing the batch
/// bookkeeping without bound.
pub const DEFAULT_BATCH_ROWS: usize = 50_000;
/// Retained batch buffers, including cell slots, vector capacity and text.
/// Parsing, the incoming record and Polars conversion use extra memory. One
/// oversized record still forms its own batch because it cannot be split.
pub const DEFAULT_BATCH_BYTES: usize = 8 * 1024 * 1024;
/// Source columns, excluding one complete leading Trivium annotation block.
pub const MAX_CSV_COLUMNS: usize = 4_096;
/// Read buffer for the UTF-16 adapter and the CSV reader. The UTF-16 adapter
/// reads through `BufReader`, so surrogate pairs and CRLF can cross `read`
/// calls without changing record boundaries.
pub const DEFAULT_READ_BUFFER_BYTES: usize = 64 * 1024;

/// Batch and buffer sizing for one CSV import.
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub read_buffer: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_BATCH_ROWS,
            max_bytes: DEFAULT_BATCH_BYTES,
            read_buffer: DEFAULT_READ_BUFFER_BYTES,
        }
    }
}

impl BatchLimits {
    fn normalized(self) -> Self {
        Self {
            max_rows: self.max_rows.max(1),
            max_bytes: self.max_bytes.max(1),
            read_buffer: self.read_buffer.max(1),
        }
    }
}

/// Maps a delimiter option string to a single-byte CSV separator.
///
/// The plan fixes an empty or omitted option to a comma; automatic detection
/// is not offered because it is not implemented as a user-visible choice.
pub fn build_delimiter(delimiter: Option<&str>) -> Result<Option<u8>, AppError> {
    let Some(text) = delimiter else {
        return Ok(Some(b','));
    };
    if text.is_empty() {
        return Ok(Some(b','));
    }
    let bytes = text.as_bytes();
    if bytes.len() == 1 && valid_delimiter(bytes[0]) {
        Ok(Some(bytes[0]))
    } else {
        Err(invalid_delimiter())
    }
}

fn valid_delimiter(byte: u8) -> bool {
    byte.is_ascii() && !matches!(byte, b'\r' | b'\n' | b'"')
}

fn invalid_delimiter() -> AppError {
    AppError::Message(
        "Invalid delimiter. Use one ASCII character other than CR, LF, or a double quote.".into(),
    )
}

fn validate_column_count(headers: &csv::StringRecord) -> Result<(), AppError> {
    validate_record_width(headers, headers.len(), "CSV header")
}

fn validate_record_width(
    headers: &csv::StringRecord,
    width: usize,
    location: &str,
) -> Result<(), AppError> {
    let leading = headers
        .iter()
        .take(ANNOTATION_COLUMNS.len())
        .collect::<Vec<_>>();
    let annotations = leading_block(&leading).map_or(0, |_| ANNOTATION_COLUMNS.len());
    let source_columns = width.saturating_sub(annotations);
    if source_columns > MAX_CSV_COLUMNS {
        return Err(AppError::Message(format!(
            "{location} has {source_columns} data columns; the limit is {MAX_CSV_COLUMNS} (plus a complete leading Trivium annotation block). No columns were discarded."
        )));
    }
    Ok(())
}

/// Summary of a completed streaming import.
pub struct CsvImportSummary {
    pub columns: Vec<ColumnMeta>,
    pub rows: usize,
    pub flags: HashMap<usize, FlagEntry>,
    pub column_max_chars: HashMap<String, usize>,
    pub report: ImportReport,
}

/// Pull-based CSV importer.
///
/// The parser owns only the CSV reader; it never holds the source file, a
/// decoded copy of it, and the completed frame at the same time. Each
/// [`next_batch`](Self::next_batch) call returns one bounded frame, so the
/// caller can write the batch and drop it before reading the next one.
pub struct CsvImporter {
    reader: csv::Reader<Box<dyn Read>>,
    pending: Option<csv::StringRecord>,
    columns: Vec<ColumnMeta>,
    flag_columns: FlagColumns,
    restore_annotations: bool,
    recover: bool,
    report: ImportReport,
    diagnostics: Option<IssueSink>,
    limits: BatchLimits,
    rows_read: usize,
    flags: HashMap<usize, FlagEntry>,
    column_max_chars: HashMap<String, usize>,
    sample_rows: usize,
    finished: bool,
}

impl CsvImporter {
    pub fn open(path: &Path, delimiter: Option<u8>, limits: BatchLimits) -> Result<Self, AppError> {
        Self::open_with_options(
            path,
            delimiter,
            limits,
            &ImportOptions::default(),
            &JobContext::none(),
        )
    }

    pub fn open_with_options(
        path: &Path,
        delimiter: Option<u8>,
        limits: BatchLimits,
        options: &ImportOptions,
        job: &JobContext,
    ) -> Result<Self, AppError> {
        Self::open_with_diagnostics(path, delimiter, limits, options, job, None)
    }

    pub fn open_with_diagnostics(
        path: &Path,
        delimiter: Option<u8>,
        limits: BatchLimits,
        options: &ImportOptions,
        job: &JobContext,
        diagnostics: Option<IssueSink>,
    ) -> Result<Self, AppError> {
        let limits = limits.normalized();
        let prepared =
            reader::prepare(path, delimiter, limits, options, job, diagnostics.as_ref())?;
        let flag_columns = FlagColumns::from_columns(&prepared.columns);
        let column_max_chars = prepared
            .columns
            .iter()
            .map(|column| (column.id.clone(), column.id.chars().count()))
            .collect();
        Ok(Self {
            reader: prepared.reader,
            pending: None,
            columns: prepared.columns,
            flag_columns,
            restore_annotations: false,
            recover: options.recover,
            limits,
            rows_read: 0,
            flags: HashMap::new(),
            column_max_chars,
            sample_rows: COLUMN_WIDTH_SAMPLE_ROWS,
            finished: false,
            report: prepared.report,
            diagnostics,
        })
    }

    pub fn with_annotations(mut self, restore_annotations: bool) -> Self {
        self.restore_annotations = restore_annotations;
        self
    }

    pub fn columns(&self) -> &[ColumnMeta] {
        &self.columns
    }

    /// Reads at most one batch. The batch stops at the row or estimated-byte
    /// limit, checks cancellation at the boundary, and reports the global row
    /// count through the job context. `None` means the input is exhausted.
    pub fn next_batch(&mut self, job: &JobContext) -> Result<Option<DataFrame>, AppError> {
        // Cancel is observed at the batch boundary, including a request made
        // from the previous batch's progress callback.
        job.check_cancelled()?;
        if self.finished {
            return Ok(None);
        }

        let Self {
            reader,
            pending,
            columns: column_meta,
            flag_columns,
            restore_annotations,
            recover,
            report,
            diagnostics,
            limits,
            rows_read,
            flags,
            column_max_chars,
            sample_rows,
            finished,
        } = self;

        let mut cells: Vec<Vec<Option<String>>> = vec![Vec::new(); column_meta.len()];
        let mut originals: Vec<String> = Vec::new();
        let mut row_ids: Vec<i64> = Vec::new();
        let mut source_records: Vec<i64> = Vec::new();
        let mut text_bytes = 0usize;
        let mut batch_rows = 0usize;
        let mut exhausted = false;

        {
            let mut records = reader.records();
            loop {
                job.check_cancelled()?;
                let record = match pending.take() {
                    Some(record) => record,
                    None => match records.next() {
                        None => {
                            exhausted = true;
                            break;
                        }
                        Some(Err(err)) => {
                            job.check_cancelled()?;
                            if *recover {
                                if let Some(line) = reader::unreadable_tail(&err) {
                                    report.unreadable_tail = true;
                                    diagnostics::issue(
                                        report,
                                        diagnostics.as_ref(),
                                        Some(line),
                                        ImportIssueAction::UnreadableTail,
                                        err.to_string(),
                                    );
                                    exhausted = true;
                                    break;
                                }
                                if matches!(err.kind(), csv::ErrorKind::Utf8 { .. }) {
                                    report.source_records += 1;
                                    report.skipped_records += 1;
                                    diagnostics::issue(
                                        report,
                                        diagnostics.as_ref(),
                                        err.position().map(|position| position.line() as usize),
                                        ImportIssueAction::Skipped,
                                        err.to_string(),
                                    );
                                    continue;
                                }
                            }
                            return Err(AppError::Message(format!(
                                "Failed to parse the CSV data: {}",
                                err
                            )));
                        }
                        Some(Ok(record)) => record,
                    },
                };
                if record.len() > column_meta.len()
                    || (!*recover && record.len() != column_meta.len())
                {
                    return Err(AppError::Message(
                        "Failed to parse the CSV data: unequal record length.".into(),
                    ));
                }
                let row_index = *rows_read;
                let source_line = record
                    .position()
                    .map(|position| position.line() as usize)
                    .unwrap_or(
                        row_index
                            + if report.has_headers == Some(false) {
                                1
                            } else {
                                2
                            },
                    );
                let annotation = if *restore_annotations && flag_columns.any() {
                    match flag_entry(*flag_columns, &record) {
                        Ok(entry) => entry,
                        Err(error) if *recover => {
                            report.source_records += 1;
                            report.skipped_records += 1;
                            diagnostics::issue(
                                report,
                                diagnostics.as_ref(),
                                Some(source_line),
                                ImportIssueAction::Skipped,
                                format!("Invalid restored annotation: {error}"),
                            );
                            continue;
                        }
                        Err(error) => {
                            return Err(AppError::Message(format!(
                                "Invalid annotation at CSV record {} (line {}): {}",
                                row_index + 1,
                                source_line,
                                error
                            )))
                        }
                    }
                } else {
                    None
                };
                let values = record.iter().map(str::to_owned).collect::<Vec<_>>();
                let original_cells: Vec<&str> = record.iter().collect();
                let original = serde_json::to_string(&original_cells).map_err(|_| {
                    AppError::Message("Failed to store the original CSV record.".into())
                })?;
                let incoming_bytes = values.iter().fold(original.capacity(), |bytes, value| {
                    bytes.saturating_add(value.capacity())
                });
                if batch_rows > 0
                    && batch_budget::retained_bytes(
                        &cells,
                        &originals,
                        &row_ids,
                        &source_records,
                        text_bytes.saturating_add(incoming_bytes),
                        true,
                    ) > limits.max_bytes
                {
                    *pending = Some(record);
                    break;
                }

                // The initial table layout only needs a leading sample. The
                // full input is never scanned just to size the columns.
                if row_index < *sample_rows {
                    for (order, cell) in record.iter().enumerate() {
                        let length = cell.chars().count();
                        if let Some(current) = column_max_chars.get_mut(&column_meta[order].id) {
                            if length > *current {
                                *current = length;
                            }
                        }
                    }
                }

                let mut values = values.into_iter();
                for column in &mut cells {
                    batch_budget::reserve_next(column)?;
                    column.push(values.next());
                }
                batch_budget::reserve_next(&mut originals)?;
                batch_budget::reserve_next(&mut row_ids)?;
                batch_budget::reserve_next(&mut source_records)?;
                text_bytes = text_bytes.saturating_add(incoming_bytes);
                originals.push(original);
                row_ids.push(row_index as i64);
                // Physical line of the record's first field. The csv reader
                // accounts for quotes and embedded newlines.
                source_records.push(source_line as i64);

                if let Some(entry) = annotation {
                    flags.insert(row_index, entry);
                }

                report.source_records += 1;
                report.imported_records += 1;
                if record.len() < column_meta.len() {
                    report.repaired_records += 1;
                    diagnostics::issue(report, diagnostics.as_ref(), Some(source_line), ImportIssueAction::Repaired,
                        format!("Record has {} fields; {} missing trailing fields were retained as null.", record.len(), column_meta.len() - record.len()));
                }
                *rows_read += 1;
                batch_rows += 1;
                if batch_rows >= limits.max_rows {
                    break;
                }
            }
        }

        if exhausted {
            *finished = true;
            if *rows_read == 0 && (report.skipped_records > 0 || report.unreadable_tail) {
                return Err(AppError::Message(
                    "No readable CSV records were found; no project was created.".into(),
                ));
            }
        }
        if batch_rows == 0 {
            return Ok(None);
        }

        let mut series: Vec<Series> = Vec::with_capacity(column_meta.len() + 3);
        for (column, values) in column_meta.iter().zip(cells) {
            series.push(Series::new(column.id.as_str(), values));
        }
        series.push(Series::new(INTERNAL_ORIGINAL, originals));
        series.push(Series::new(INTERNAL_ROW_ID, row_ids));
        series.push(Series::new(INTERNAL_SOURCE_RECORD, source_records));
        let frame = DataFrame::new(series)
            .map_err(|_| AppError::Message("Failed to build the dataset.".into()))?;

        job.report_progress(*rows_read as u64, None, Some("Reading CSV".into()));
        Ok(Some(frame))
    }

    pub fn into_summary(self) -> CsvImportSummary {
        CsvImportSummary {
            columns: self.columns,
            rows: self.rows_read,
            flags: self.flags,
            column_max_chars: self.column_max_chars,
            report: self.report,
        }
    }
}

/// Reads a whole CSV into one frame.
///
/// The production import uses [`CsvImporter`] and writes each batch, so this
/// convenience path exists for direct callers and tests that need a frame.
/// It still parses incrementally; only the accumulated frame remains.
pub fn load(
    path: &Path,
    delimiter: Option<u8>,
    restore_annotations: bool,
) -> Result<ImportedData, AppError> {
    let job = JobContext::none();
    let mut importer = CsvImporter::open(path, delimiter, BatchLimits::default())?
        .with_annotations(restore_annotations);
    let columns = importer.columns().to_vec();
    let mut frame: Option<DataFrame> = None;
    while let Some(batch) = importer.next_batch(&job)? {
        frame = Some(match frame {
            None => batch,
            Some(mut accumulated) => {
                accumulated.vstack_mut(&batch).map_err(|_| {
                    AppError::Message(
                        "The CSV row count changed while building the dataset.".into(),
                    )
                })?;
                accumulated
            }
        });
    }
    let summary = importer.into_summary();
    let frame = match frame {
        Some(frame) => frame,
        None => empty_frame(&columns)?,
    };

    Ok(ImportedData {
        frame,
        columns: summary.columns,
        flags: summary.flags,
    })
}
