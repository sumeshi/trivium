use std::{mem::size_of, path::Path, sync::Arc};

use crate::{
    error::AppError,
    formats::{CompiledFormat, FormatOccurrence},
    jobs::JobContext,
    ops::query::QuerySpec,
    project_io::ProjectBatchReader,
    store::ProjectsStore,
    value_utils::bounded_text::present_text,
};

use super::{
    aggregate::{limit_error, Accumulator, Limits},
    cache::{Revision, Snapshot},
    ExtractPayload, ExtractResult, ExtractScope, ExtractValue,
};
use crate::ops::analysis_data::{AnalysisSource, SelectedRows};

const MAX_METADATA_BYTES: usize = 512 * 1024;
const MAX_COLUMNS: usize = 4096;

pub(super) trait Visitor {
    fn available(&self) -> usize;
    /// False stops a provenance page after finding its next-page sentinel.
    fn occurrence(
        &mut self,
        row: usize,
        column: &str,
        value: &str,
        start: usize,
        end: usize,
        transient: usize,
        matcher: &CompiledFormat,
    ) -> Result<bool, AppError>;
}

pub(super) fn build(
    store: &ProjectsStore,
    payload: ExtractPayload,
    limits: Limits,
    job: &JobContext,
) -> Result<Snapshot, AppError> {
    job.check_cancelled()?;
    if payload.columns.is_empty() || payload.columns.len() > MAX_COLUMNS {
        return Err(AppError::Message(
            "Choose between 1 and 4,096 data columns to extract.".into(),
        ));
    }
    let metadata_bytes = metadata_size(&payload)?;
    let matcher = CompiledFormat::new(&payload.format)?;
    let path = store.project_dir(&payload.project_id).join("data.parquet");
    let revision = Revision::read(&path)?;
    // Display order has no effect on membership or value frequency. Avoid
    // materializing a sorted column merely to scan in physical source order.
    let mut spec = match payload.scope {
        ExtractScope::All => QuerySpec::default(),
        ExtractScope::Filtered => payload.spec.clone(),
    };
    spec.sort_key = None;
    spec.sort_direction = None;
    let source = AnalysisSource::open(store, payload.project_id, &spec, job)?;
    let columns = source.columns(&payload.columns, "extraction columns")?;
    let mut unique = std::collections::HashSet::new();
    if columns.iter().any(|column| !unique.insert(column)) {
        return Err(AppError::Message(
            "Choose each extraction column only once.".into(),
        ));
    }
    let total_rows = source.target.total_rows;
    let source_rows = source.target.ids.len();
    let ids = (source_rows != total_rows).then(|| Arc::clone(&source.target.ids));
    drop(source);
    let mut accumulator = Accumulator::new(limits);
    walk(
        &path,
        &columns,
        total_rows,
        ids.as_deref().map(Vec::as_slice),
        &matcher,
        &mut accumulator,
        job,
    )?;
    job.check_cancelled()?;
    let occurrences = accumulator.occurrences;
    let matched_rows = accumulator.matched_rows;
    let values = accumulator.finish(job)?;
    let has_classification = payload.format.classifier.is_some();
    let summary = ExtractResult {
        result_id: uuid::Uuid::new_v4(),
        project_id: payload.project_id,
        format: payload.format,
        columns,
        scope: payload.scope,
        source_rows,
        total_occurrences: occurrences,
        matched_rows,
        unique_values: values.len(),
        has_classification,
    };
    let bytes = values
        .capacity()
        .saturating_mul(size_of::<ExtractValue>())
        .saturating_add(
            values
                .iter()
                .map(|value| {
                    value.value.capacity() + value.scope.as_ref().map_or(0, String::capacity) + 64
                })
                .sum::<usize>(),
        )
        .saturating_add(
            ids.as_ref()
                .map_or(0, |ids| ids.capacity() * size_of::<usize>()),
        )
        // The matcher limits its program and search caches. Reserve their
        // worst-case capacity as well as its cloned definition/classifier.
        // Short strings in vectors can retain much more heap space than their
        // JSON representation. Include cloned snapshots and allocation slots.
        .saturating_add(metadata_bytes * 64)
        .saturating_add(8 * 1024 * 1024);
    let snapshot = Snapshot {
        summary,
        values,
        matcher,
        path,
        revision,
        total_rows,
        ids,
        _query: payload.spec,
        bytes,
    };
    snapshot.validate(store)?;
    job.check_cancelled()?;
    Ok(snapshot)
}

pub(super) fn walk(
    path: &Path,
    columns: &[String],
    total_rows: usize,
    ids: Option<&[usize]>,
    matcher: &CompiledFormat,
    visitor: &mut impl Visitor,
    job: &JobContext,
) -> Result<(), AppError> {
    let mut reader = match ids {
        Some(ids) => ProjectBatchReader::for_rows(path, columns, ids),
        None => ProjectBatchReader::open(path, columns),
    }
    .map_err(AppError::from)?;
    let selected = ids.map(|ids| SelectedRows::new(total_rows, ids));
    let expected = ids.map_or(total_rows, <[usize]>::len);
    let mut read_rows = 0;
    let mut checked = 0usize;
    job.report_progress(0, Some(expected as u64), Some("Extracting values".into()));
    while let Some(batch) = reader.next_with_job(job) {
        let batch = batch.map_err(AppError::from)?;
        let series = columns
            .iter()
            .map(|column| batch.frame.column(column))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| AppError::Other(error.into()))?;
        for local in 0..batch.frame.height() {
            if local.is_multiple_of(256) {
                job.check_cancelled()?;
            }
            let row = batch.offset + local;
            if selected
                .as_ref()
                .is_some_and(|selected| !selected.contains(row))
            {
                continue;
            }
            for (column, series) in columns.iter().zip(&series) {
                job.check_cancelled()?;
                let value = series
                    .get(local)
                    .map_err(|error| AppError::Other(error.into()))?;
                let Some(text) =
                    present_text(&value, visitor.available()).map_err(|_| limit_error())?
                else {
                    continue;
                };
                let transient = match &text {
                    std::borrow::Cow::Owned(value) => value.capacity(),
                    _ => 0,
                };
                for FormatOccurrence { value, start, end } in matcher.occurrences(&text) {
                    if checked.is_multiple_of(256) {
                        job.check_cancelled()?;
                    }
                    checked = checked.wrapping_add(1);
                    if !visitor.occurrence(row, column, value, start, end, transient, matcher)? {
                        return Ok(());
                    }
                }
            }
            read_rows += 1;
        }
        job.report_progress(
            read_rows as u64,
            Some(expected as u64),
            Some("Extracting values".into()),
        );
    }
    job.check_cancelled()?;
    if read_rows != expected {
        return Err(AppError::Message(
            "Extraction source rows changed while scanning.".into(),
        ));
    }
    job.report_progress(
        expected as u64,
        Some(expected as u64),
        Some("Extraction complete".into()),
    );
    Ok(())
}

fn metadata_size(payload: &ExtractPayload) -> Result<usize, AppError> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            if self.0 > MAX_METADATA_BYTES {
                return Err(std::io::Error::other("extraction request too large"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, payload).map_err(|_| {
        AppError::Message(
            "Extraction request exceeds 512 KiB. Reduce its columns or conditions.".into(),
        )
    })?;
    Ok(count.0)
}
