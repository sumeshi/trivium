//! Source-order search keeps one decoded batch and matching source IDs only.
use std::{collections::HashMap, path::Path};

use polars::prelude::{DataFrame, DataType, Series};

use crate::{
    error::AppError,
    jobs::JobContext,
    models::FlagEntry,
    project_io::{ProjectBatchReader, INTERNAL_ROW_ID, PROJECT_BATCH_ROWS},
};

use super::{
    page::flag_population,
    search::PreparedQuery,
    target::{append_matching_rows, record_resolution, QuerySpec, ResolvedTarget},
};

pub(super) fn resolve_source_order(
    path: &Path,
    total_rows: usize,
    columns: &[String],
    spec: &QuerySpec,
    prepared: &PreparedQuery,
    flags: &HashMap<usize, FlagEntry>,
    job: &JobContext,
) -> Result<ResolvedTarget, AppError> {
    record_resolution();
    let mut filtered = Vec::new();
    let mut annotated_filtered = 0;
    if columns.is_empty() {
        // Tag/flag/bookmark-only membership needs no Parquet data, including
        // legacy files without __rowid. Bound the synthetic count anchor too.
        for offset in (0..total_rows).step_by(PROJECT_BATCH_ROWS) {
            job.check_cancelled()?;
            let frame = DataFrame::new(vec![Series::full_null(
                INTERNAL_ROW_ID,
                PROJECT_BATCH_ROWS.min(total_rows - offset),
                &DataType::Null,
            )])
            .map_err(|error| AppError::from(anyhow::Error::from(error)))?;
            annotated_filtered +=
                append_matching_rows(&frame, offset, spec, prepared, flags, &mut filtered, job)?;
        }
    } else {
        let mut reader = ProjectBatchReader::open(path, columns).map_err(AppError::from)?;
        while let Some(batch) = reader.next_with_job(job) {
            let batch = batch.map_err(AppError::from)?;
            annotated_filtered += append_matching_rows(
                &batch.frame,
                batch.offset,
                spec,
                prepared,
                flags,
                &mut filtered,
                job,
            )?;
            job.report_progress(
                (batch.offset + batch.frame.height()) as u64,
                Some(total_rows as u64),
                Some("Evaluating query rows".into()),
            );
            // `batch` (and its mask/borrowed cells) is released here, before
            // the reader decodes the next batch. No vstack materialization.
        }
    }
    job.check_cancelled()?;
    let annotated_total = flags
        .iter()
        .filter(|(row, entry)| **row < total_rows && flag_population(&entry.flag) != 8)
        .count();
    Ok(ResolvedTarget {
        filtered,
        annotated_filtered,
        annotated_total,
    })
}
