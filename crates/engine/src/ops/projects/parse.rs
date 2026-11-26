use std::{collections::HashMap, path::Path};

use super::profile::KindAccumulator;
use crate::{
    error::AppError,
    import::{
        csv,
        diagnostics::IssueSink,
        jsonl,
        options::{ImportOptions, ImportReport},
    },
    jobs::JobContext,
    models::{ColumnMeta, FlagEntry},
    project_io::BatchDataWriter,
};

pub(super) struct ImportedSource {
    pub columns: Vec<ColumnMeta>,
    pub total_records: usize,
    pub flags: HashMap<usize, FlagEntry>,
    pub column_max_chars: Option<HashMap<String, usize>>,
    pub column_kinds: HashMap<String, String>,
    pub report: ImportReport,
}

pub(super) struct ParseOptions<'a> {
    pub format: &'a str,
    pub delimiter: Option<&'a str>,
    pub restore_annotations: bool,
    pub options: &'a ImportOptions,
    pub diagnostics: Option<IssueSink>,
}

pub(super) fn import_batches(
    source: &Path,
    parquet_path: &Path,
    config: ParseOptions<'_>,
    job: &JobContext,
) -> Result<ImportedSource, AppError> {
    let ParseOptions {
        format,
        delimiter,
        restore_annotations,
        options,
        diagnostics,
    } = config;
    if format == "csv" {
        return import_csv(
            source,
            parquet_path,
            delimiter,
            restore_annotations,
            options,
            job,
            diagnostics,
        );
    }
    let format = if format == "json" {
        jsonl::RecordFormat::Json
    } else {
        jsonl::RecordFormat::JsonLines
    };
    let importer = jsonl::JsonImporter::open_with_diagnostics(
        source,
        format,
        jsonl::BatchLimits::default(),
        options,
        job,
        diagnostics,
    )?;
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
        report: summary.report,
    })
}

fn import_csv(
    source: &Path,
    parquet_path: &Path,
    delimiter: Option<&str>,
    restore_annotations: bool,
    options: &ImportOptions,
    job: &JobContext,
    diagnostics: Option<IssueSink>,
) -> Result<ImportedSource, AppError> {
    let mut importer = csv::CsvImporter::open_with_diagnostics(
        source,
        csv::build_delimiter(delimiter)?,
        csv::BatchLimits::default(),
        options,
        job,
        diagnostics,
    )?
    .with_annotations(restore_annotations);
    let columns = importer.columns().to_vec();
    let mut writer = BatchDataWriter::create(parquet_path, &columns).map_err(AppError::from)?;
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
    Ok(ImportedSource {
        columns: summary.columns,
        total_records: summary.rows,
        flags: summary.flags,
        column_max_chars: Some(summary.column_max_chars),
        column_kinds: kinds.finish(&ids),
        report: summary.report,
    })
}
