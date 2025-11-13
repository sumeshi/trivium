use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};
use polars::prelude::{DataFrame, ParquetReader, ParquetWriter, SerReader};

pub fn read_project_dataframe(path: &Path) -> Result<DataFrame> {
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
