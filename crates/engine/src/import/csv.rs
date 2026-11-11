use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use polars::prelude::*;

use crate::error::AppError;
use crate::import::utf16::{detect_bom, Encoding, Utf16Reader};
use crate::import::{column_id, empty_frame};
use crate::jobs::JobContext;
use crate::models::{ColumnMeta, FlagEntry, ImportedData};
use crate::project_io::{INTERNAL_ORIGINAL, INTERNAL_ROW_ID, INTERNAL_SOURCE_RECORD};
use crate::storage::COLUMN_WIDTH_SAMPLE_ROWS;

/// Maximum number of records in one import batch. The byte limit can stop a
/// batch earlier; the row limit keeps narrow tables from growing the batch
/// bookkeeping without bound.
pub const DEFAULT_BATCH_ROWS: usize = 50_000;
/// Approximate payload bytes (cell text plus the `__original` JSON) in one
/// import batch. This is deliberately a soft limit: one record larger than the
/// limit still forms a batch of its own, because a record cannot be split.
pub const DEFAULT_BATCH_BYTES: usize = 8 * 1024 * 1024;
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

/// Maps a delimiter option string to a single-byte polars separator.
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
    match text {
        "\t" => return Ok(Some(b'\t')),
        "," => return Ok(Some(b',')),
        ";" => return Ok(Some(b';')),
        "|" => return Ok(Some(b'|')),
        _ => {}
    }
    let bytes = text.as_bytes();
    if bytes.len() == 1 && bytes[0].is_ascii() {
        Ok(Some(bytes[0]))
    } else {
        Err(AppError::Message(format!("Invalid delimiter: {}.", text)))
    }
}

/// Internal column id assigned to a source column by its position.
/// Source column positions of the optional annotation side channel.
#[derive(Clone, Copy, Debug, Default)]
struct FlagColumns {
    safe: Option<usize>,
    suspicious: Option<usize>,
    critical: Option<usize>,
    memo: Option<usize>,
    tags: Option<usize>,
    bookmarked: Option<usize>,
}

impl FlagColumns {
    fn find(columns: &[ColumnMeta], name: &str) -> Option<usize> {
        columns
            .iter()
            .find(|column| column.source == name)
            .map(|column| column.order)
    }

    fn from_columns(columns: &[ColumnMeta]) -> Self {
        Self {
            safe: Self::find(columns, "trivium-safe"),
            suspicious: Self::find(columns, "trivium-suspicious"),
            critical: Self::find(columns, "trivium-critical"),
            memo: Self::find(columns, "trivium-memo"),
            tags: Self::find(columns, "trivium-tags"),
            bookmarked: Self::find(columns, "trivium-bookmarked"),
        }
    }

    fn any(self) -> bool {
        self.safe.is_some()
            || self.suspicious.is_some()
            || self.critical.is_some()
            || self.memo.is_some()
            || self.tags.is_some()
            || self.bookmarked.is_some()
    }
}

fn flag_value(record: &csv::StringRecord, index: Option<usize>) -> Option<String> {
    index
        .and_then(|column| record.get(column))
        .map(str::to_string)
        .filter(|text| !text.is_empty() && text != "0")
}

fn flag_entry(columns: FlagColumns, record: &csv::StringRecord) -> Option<FlagEntry> {
    let flag = flag_value(record, columns.critical)
        .map(|_| "critical".to_string())
        .or_else(|| flag_value(record, columns.suspicious).map(|_| "suspicious".to_string()))
        .or_else(|| flag_value(record, columns.safe).map(|_| "safe".to_string()))
        .unwrap_or_default();
    let memo_value = columns
        .memo
        .and_then(|column| record.get(column))
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);
    // `trivium-tags` is a `;`-separated list. A tag that itself contains `;`
    // is split on re-import; the export format has no escaping for it.
    let tags: Vec<String> = columns
        .tags
        .and_then(|column| record.get(column))
        .map(|text| {
            text.split(';')
                .map(str::trim)
                .filter(|tag| !tag.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let bookmarked = columns
        .bookmarked
        .and_then(|column| record.get(column))
        .map(|text| text.trim() == "1")
        .unwrap_or(false);
    if flag.is_empty() && memo_value.is_none() && tags.is_empty() && !bookmarked {
        return None;
    }
    Some(FlagEntry {
        flag,
        memo: memo_value,
        tags,
        bookmarked,
    })
}

/// Summary of a completed streaming import.
pub struct CsvImportSummary {
    pub columns: Vec<ColumnMeta>,
    pub rows: usize,
    pub flags: HashMap<usize, FlagEntry>,
    pub column_max_chars: HashMap<String, usize>,
}

/// Pull-based CSV importer.
///
/// The parser owns only the CSV reader; it never holds the source file, a
/// decoded copy of it, and the completed frame at the same time. Each
/// [`next_batch`](Self::next_batch) call returns one bounded frame, so the
/// caller can write the batch and drop it before reading the next one.
pub struct CsvImporter {
    reader: csv::Reader<Box<dyn Read>>,
    columns: Vec<ColumnMeta>,
    flag_columns: FlagColumns,
    restore_annotations: bool,
    limits: BatchLimits,
    rows_read: usize,
    flags: HashMap<usize, FlagEntry>,
    column_max_chars: HashMap<String, usize>,
    sample_rows: usize,
    finished: bool,
}

impl CsvImporter {
    pub fn open(path: &Path, delimiter: Option<u8>, limits: BatchLimits) -> Result<Self, AppError> {
        let limits = limits.normalized();
        let mut file = File::open(path)
            .map_err(|e| AppError::Message(format!("Failed to open file: {}", e)))?;
        let (encoding, skip) = detect_bom(&mut file)
            .map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;
        file.seek(SeekFrom::Start(skip))
            .map_err(|e| AppError::Message(format!("Failed to read file: {}", e)))?;

        // `None` keeps the existing comma default for the parser. The polars
        // reader previously performed automatic detection; the raw cell reader
        // needs a concrete separator, so the same default is applied here.
        let separator = delimiter.unwrap_or(b',');
        let input: Box<dyn Read> = match encoding {
            Encoding::Utf8 => Box::new(file),
            Encoding::Utf16Le => Box::new(Utf16Reader::new(
                BufReader::with_capacity(limits.read_buffer, file),
                false,
            )),
            Encoding::Utf16Be => Box::new(Utf16Reader::new(
                BufReader::with_capacity(limits.read_buffer, file),
                true,
            )),
        };
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(separator)
            .flexible(false)
            .buffer_capacity(limits.read_buffer)
            .from_reader(input);
        let headers = reader
            .headers()
            .map_err(|_| AppError::Message("Failed to parse the CSV data.".into()))?
            .clone();
        let source_headers: Vec<String> = headers.iter().map(|header| header.to_string()).collect();
        if source_headers.is_empty() {
            return Err(AppError::Message("The CSV data has no columns.".into()));
        }

        let columns: Vec<ColumnMeta> = source_headers
            .iter()
            .enumerate()
            .map(|(order, header)| ColumnMeta {
                id: column_id(order),
                label: if header.is_empty() {
                    format!("column {}", order + 1)
                } else {
                    header.clone()
                },
                source: header.clone(),
                order,
            })
            .collect();
        let flag_columns = FlagColumns::from_columns(&columns);
        let column_max_chars = columns
            .iter()
            .map(|column| (column.id.clone(), column.id.chars().count()))
            .collect();

        Ok(Self {
            reader,
            columns,
            flag_columns,
            restore_annotations: false,
            limits,
            rows_read: 0,
            flags: HashMap::new(),
            column_max_chars,
            sample_rows: COLUMN_WIDTH_SAMPLE_ROWS,
            finished: false,
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
            columns: column_meta,
            flag_columns,
            restore_annotations,
            limits,
            rows_read,
            flags,
            column_max_chars,
            sample_rows,
            finished,
        } = self;

        let mut cells: Vec<Vec<String>> = vec![Vec::new(); column_meta.len()];
        let mut originals: Vec<String> = Vec::new();
        let mut row_ids: Vec<i64> = Vec::new();
        let mut source_records: Vec<i64> = Vec::new();
        let mut estimated_bytes = 0usize;
        let mut batch_rows = 0usize;
        let mut exhausted = false;

        {
            let mut records = reader.records();
            loop {
                let record = match records.next() {
                    None => {
                        exhausted = true;
                        break;
                    }
                    Some(Err(err)) => {
                        return Err(AppError::Message(format!(
                            "Failed to parse the CSV data: {}",
                            err
                        )));
                    }
                    Some(Ok(record)) => record,
                };
                if record.len() != column_meta.len() {
                    return Err(AppError::Message(
                        "Failed to parse the CSV data: unequal record length.".into(),
                    ));
                }
                let row_index = *rows_read;

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

                for (order, cell) in record.iter().enumerate() {
                    cells[order].push(cell.to_string());
                    estimated_bytes += cell.len() + 1;
                }
                let original_cells: Vec<&str> = record.iter().collect();
                let original = serde_json::to_string(&original_cells).map_err(|_| {
                    AppError::Message("Failed to store the original CSV record.".into())
                })?;
                estimated_bytes += original.len() + 8;
                originals.push(original);
                row_ids.push(row_index as i64);
                // Physical line of the record's first field. The csv reader
                // accounts for quotes and embedded newlines.
                source_records.push(
                    record
                        .position()
                        .map(|position| position.line() as i64)
                        .unwrap_or((row_index as i64) + 2),
                );

                if *restore_annotations && flag_columns.any() {
                    if let Some(entry) = flag_entry(*flag_columns, &record) {
                        flags.insert(row_index, entry);
                    }
                }

                *rows_read += 1;
                batch_rows += 1;
                if batch_rows >= limits.max_rows || estimated_bytes >= limits.max_bytes {
                    break;
                }
            }
        }

        if exhausted {
            *finished = true;
        }
        if batch_rows == 0 {
            return Ok(None);
        }

        let mut series: Vec<Series> = Vec::with_capacity(column_meta.len() + 2);
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
