use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use polars::io::parquet::write::BatchedWriter;
use polars::prelude::{
    AnyValue, DataFrame, DataType, Field, ParquetReader, ParquetWriter, Schema, SerReader,
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

/// Number of full project-data reads since process start.
///
/// Test instrumentation only: annotation updates must not increment this.
pub static PROJECT_DATA_READS: AtomicU64 = AtomicU64::new(0);

/// Reads the counter maintained by [`read_project_dataframe`]. Test-only.
pub fn project_data_reads() -> u64 {
    PROJECT_DATA_READS.load(Ordering::Relaxed)
}

/// Resets the counter. Test-only.
pub fn reset_project_data_reads() {
    PROJECT_DATA_READS.store(0, Ordering::Relaxed);
}

pub fn read_project_dataframe(path: &Path) -> Result<DataFrame> {
    PROJECT_DATA_READS.fetch_add(1, Ordering::Relaxed);
    ParquetReader::new(File::open(path)?)
        .finish()
        .context("failed to read parquet file")
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
        if meta.columns.is_empty() {
            return Self::from_frame(df);
        }
        let columns = meta
            .columns
            .iter()
            .filter(|column| !is_internal_column(&column.id))
            .cloned()
            .collect();
        Self { columns }
    }

    fn from_frame(df: &DataFrame) -> Self {
        let columns = df
            .get_column_names()
            .into_iter()
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
        self.columns.iter().map(|column| column.id.clone()).collect()
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
