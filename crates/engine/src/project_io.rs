use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};
use polars::io::parquet::write::BatchedWriter;
use polars::prelude::{
    AnyValue, DataFrame, DataType, Field, NewChunkedArray, ParquetReader, ParquetWriter, Schema,
    SerReader, UInt32Chunked,
};
use serde_json::Value;

use crate::models::{ColumnMeta, ProjectMeta};
use crate::value_utils::anyvalue_to_search_string;

/// Internal row id column. It is present in every storage version.
pub const INTERNAL_ROW_ID: &str = "__rowid";
/// Original record JSON column. Only the new storage format writes it; legacy
/// projects keep the source only in the flattened, typed columns.
pub const INTERNAL_ORIGINAL: &str = "__original";

/// Internal columns are never part of the data column list, search scope,
/// output columns, or analysis inputs.
pub fn is_internal_column(name: &str) -> bool {
    name == INTERNAL_ROW_ID || name == INTERNAL_ORIGINAL
}

/// Number of [`read_project_dataframe`] calls since process start.
///
/// Test instrumentation only: annotation updates must not increment this.
pub static PROJECT_DATA_READS: AtomicU64 = AtomicU64::new(0);
/// Number of full-frame parquet reads since process start.
///
/// Test instrumentation only. [`read_project_dataframe`] increments this;
/// page and column projections do not.
pub static PROJECT_FULL_READS: AtomicU64 = AtomicU64::new(0);
/// Number of page reads made through [`read_page_dataframe`] since process
/// start, including reads that serve an arbitrary row-id page.
///
/// Test instrumentation only.
pub static PROJECT_PAGE_READS: AtomicU64 = AtomicU64::new(0);

fn last_page_projection_store() -> &'static Mutex<Vec<String>> {
    static PROJECTION: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    PROJECTION.get_or_init(|| Mutex::new(Vec::new()))
}

fn last_column_read_projection_store() -> &'static Mutex<Vec<String>> {
    static PROJECTION: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    PROJECTION.get_or_init(|| Mutex::new(Vec::new()))
}

fn record_projection(store: &'static Mutex<Vec<String>>, columns: &[String]) {
    let mut recorded = store
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    recorded.clear();
    recorded.extend_from_slice(columns);
}

/// Reads the counter maintained by [`read_project_dataframe`]. Test-only.
pub fn project_data_reads() -> u64 {
    PROJECT_DATA_READS.load(Ordering::Relaxed)
}

/// Full-frame reads made through [`read_project_dataframe`]. Test-only.
pub fn project_full_reads() -> u64 {
    PROJECT_FULL_READS.load(Ordering::Relaxed)
}

/// Page reads made through [`read_page_dataframe`]. Test-only.
pub fn project_page_reads() -> u64 {
    PROJECT_PAGE_READS.load(Ordering::Relaxed)
}

/// Column projection of the most recent page read. Test-only.
pub fn last_page_projection() -> Vec<String> {
    last_page_projection_store()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// Column projection of the most recent [`read_project_columns`] call.
/// Test-only.
pub fn last_column_read_projection() -> Vec<String> {
    last_column_read_projection_store()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// Resets [`PROJECT_DATA_READS`] and the recorded projections. Test-only.
pub fn reset_project_data_reads() {
    PROJECT_DATA_READS.store(0, Ordering::Relaxed);
}

/// Resets both full-read and page-read counters and their projections.
/// Test-only.
pub fn reset_project_read_counters() {
    PROJECT_DATA_READS.store(0, Ordering::Relaxed);
    PROJECT_FULL_READS.store(0, Ordering::Relaxed);
    PROJECT_PAGE_READS.store(0, Ordering::Relaxed);
    last_page_projection_store()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    last_column_read_projection_store()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

pub fn read_project_dataframe(path: &Path) -> Result<DataFrame> {
    PROJECT_DATA_READS.fetch_add(1, Ordering::Relaxed);
    PROJECT_FULL_READS.fetch_add(1, Ordering::Relaxed);
    ParquetReader::new(File::open(path)?)
        .finish()
        .context("failed to read parquet file")
}

/// All column names in the file, including internal storage columns.
///
/// This reads the parquet footer only; it never materializes rows.
pub fn read_project_schema_columns(path: &Path) -> Result<Vec<String>> {
    let mut reader = ParquetReader::new(File::open(path)?);
    let schema = reader.schema().context("failed to read parquet schema")?;
    Ok(schema
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect())
}

/// Row count from the parquet footer, without materializing any column.
pub fn read_project_row_count(path: &Path) -> Result<usize> {
    let mut reader = ParquetReader::new(File::open(path)?);
    reader
        .num_rows()
        .context("failed to read parquet row count")
}

/// Reads only `columns`, for every row, without the internal record columns
/// unless they are requested.
///
/// The query resolver uses this for search, condition, and sort columns. It is
/// a column projection, not a full-frame read, and does not increment the
/// full-read counter.
pub fn read_project_columns(path: &Path, columns: &[String]) -> Result<DataFrame> {
    record_projection(last_column_read_projection_store(), columns);
    ParquetReader::new(File::open(path)?)
        .with_columns(Some(columns.to_vec()))
        .finish()
        .context("failed to read parquet columns")
}

/// Reads one contiguous page with a column projection.
///
/// polars 0.40's [`ParquetReader`] exposes `with_columns` and `with_n_rows`,
/// but no row offset or row-group selection. This implementation therefore
/// reads the projected column chunks for the row groups covering the first
/// `offset + limit` rows, then drops the rows before `offset`. It never calls
/// the all-column `finish()` used by [`read_project_dataframe`].
///
/// A single importer batch is one row group, so a page near the start avoids
/// the later groups; a page in the last group still decodes the earlier
/// projected groups. This is the documented fallback for this polars version.
pub fn read_page_dataframe(
    path: &Path,
    columns: &[String],
    offset: usize,
    limit: usize,
) -> Result<DataFrame> {
    if limit == 0 {
        return Ok(DataFrame::empty());
    }
    PROJECT_PAGE_READS.fetch_add(1, Ordering::Relaxed);
    record_projection(last_page_projection_store(), columns);
    let end = offset.saturating_add(limit);
    let df = ParquetReader::new(File::open(path)?)
        .with_columns(Some(columns.to_vec()))
        .with_n_rows(Some(end))
        .finish()
        .context("failed to read parquet page")?;
    let start = offset.min(df.height());
    Ok(df.slice(start as i64, limit))
}

/// Reads the requested original row ids into a page-sized frame in that order.
///
/// The rows are fetched as one range from the smallest to the largest id and
/// then reordered in memory. This avoids materializing a full frame when the
/// ids cluster, while a random sort that spans the file still reads the
/// projected prefix range. The column projection is the same page projection
/// used by [`read_page_dataframe`].
pub fn read_page_rows_dataframe(
    path: &Path,
    columns: &[String],
    row_indices: &[usize],
) -> Result<DataFrame> {
    let Some(min) = row_indices.iter().copied().min() else {
        return Ok(DataFrame::empty());
    };
    let max = row_indices.iter().copied().max().unwrap_or(min);
    let range = read_page_dataframe(path, columns, min, max - min + 1)?;
    if range.height() == row_indices.len() && row_indices.iter().copied().eq(min..=max) {
        return Ok(range);
    }
    let take_idx = UInt32Chunked::from_iter_values(
        "take_idx",
        row_indices.iter().map(|row| (row - min) as u32),
    );
    range.take(&take_idx).context("failed to select page rows")
}

pub fn write_project_dataframe(path: &Path, df: &mut DataFrame) -> Result<()> {
    let file =
        File::create(path).with_context(|| format!("failed to create parquet file {:?}", path))?;
    let writer = ParquetWriter::new(file);
    writer.finish(df).context("failed to write parquet file")?;
    Ok(())
}

/// Writes project Parquet one bounded batch at a time.
///
/// polars 0.40 exposes [`ParquetWriter::batched`], which writes every
/// [`write_batch`](Self::write_batch) call as its own row group and only
/// writes the footer on [`finish`](Self::finish). The schema is fixed from the
/// CSV header so every batch has the same columns: the imported data columns,
/// then `__original`, then `__rowid`.
pub struct BatchDataWriter {
    inner: BatchedWriter<BufWriter<File>>,
}

impl BatchDataWriter {
    pub fn create(path: &Path, columns: &[ColumnMeta]) -> Result<Self> {
        let fields: Vec<Field> = columns
            .iter()
            .map(|column| Field::new(&column.id, DataType::String))
            .chain([
                Field::new(INTERNAL_ORIGINAL, DataType::String),
                Field::new(INTERNAL_ROW_ID, DataType::Int64),
            ])
            .collect();
        let schema: Schema = fields.into_iter().collect();
        let file = File::create(path)
            .with_context(|| format!("failed to create parquet file {:?}", path))?;
        let inner = ParquetWriter::new(BufWriter::new(file))
            .batched(&schema)
            .context("failed to create parquet writer")?;
        Ok(Self { inner })
    }

    pub fn write_batch(&mut self, df: &DataFrame) -> Result<()> {
        self.inner
            .write_batch(df)
            .context("failed to write parquet batch")
    }

    pub fn finish(self) -> Result<()> {
        self.inner
            .finish()
            .context("failed to finish parquet file")?;
        Ok(())
    }
}

/// Thin id/label/source mapping for a project's data columns.
///
/// New projects read the metadata recorded in `ProjectMeta`. Legacy projects
/// have no metadata, so ids and labels are synthesized from the frame names;
/// no new id is guessed for them.
#[derive(Debug, Clone)]
pub struct ProjectColumns {
    columns: Vec<ColumnMeta>,
}

impl ProjectColumns {
    pub fn from_meta(meta: &ProjectMeta, df: &DataFrame) -> Self {
        let names: Vec<String> = df
            .get_column_names()
            .into_iter()
            .map(|name| name.to_string())
            .collect();
        Self::from_meta_and_names(meta, &names)
    }

    /// Builds the column mapping without reading any rows.
    ///
    /// `names` are the parquet schema column names, including internal
    /// columns. Legacy projects derive ids and labels from them.
    pub fn from_meta_and_names(meta: &ProjectMeta, names: &[String]) -> Self {
        if meta.columns.is_empty() {
            return Self::from_names(names);
        }
        let columns = meta
            .columns
            .iter()
            .filter(|column| !is_internal_column(&column.id))
            .cloned()
            .collect();
        Self { columns }
    }

    fn from_names(names: &[String]) -> Self {
        let columns = names
            .iter()
            .filter(|name| !is_internal_column(name))
            .enumerate()
            .map(|(order, name)| ColumnMeta {
                id: name.to_string(),
                label: name.to_string(),
                source: name.to_string(),
                order,
            })
            .collect();
        Self { columns }
    }

    pub fn columns(&self) -> &[ColumnMeta] {
        &self.columns
    }

    pub fn ids(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|column| column.id.clone())
            .collect()
    }

    /// Display label for an id. Unknown ids fall back to the id, which keeps
    /// legacy callers working without a lookup.
    pub fn label(&self, id: &str) -> String {
        self.columns
            .iter()
            .find(|column| column.id == id)
            .map(|column| column.label.clone())
            .unwrap_or_else(|| id.to_string())
    }
}

/// Reads and parses the `__original` column, one JSON value per row.
///
/// CSV projects store a string array of the original cells; JSON projects
/// store the original object. JSON numbers are parsed by `serde_json` without
/// rounding, and a missing key stays missing while an explicit null stays null.
pub fn read_original_records(project_dir: &Path) -> Result<Vec<Value>> {
    let df = read_project_dataframe(&project_dir.join("data.parquet"))?;
    let series = df.column(INTERNAL_ORIGINAL).with_context(|| {
        format!(
            "project at {:?} has no {} column",
            project_dir, INTERNAL_ORIGINAL
        )
    })?;
    let mut records = Vec::with_capacity(series.len());
    for index in 0..series.len() {
        let value = series.get(index)?;
        let parsed = match value {
            AnyValue::Null => Value::Null,
            other => {
                let text = anyvalue_to_search_string(&other).unwrap_or_default();
                serde_json::from_str(&text)
                    .with_context(|| format!("failed to parse original record at row {}", index))?
            }
        };
        records.push(parsed);
    }
    Ok(records)
}
