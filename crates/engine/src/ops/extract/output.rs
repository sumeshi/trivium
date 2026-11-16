use std::{path::Path, sync::Arc};

use crate::{
    error::AppError,
    export::atomic::{validate_destination, write_atomically},
    jobs::JobContext,
    ops::export::ExportResult,
    store::ProjectsStore,
};

use super::{
    cache::{self, Snapshot},
    ExportExtractPayload,
};

/// An accepted export retains one shared, byte-accounted result across queued
/// worker admission. Closing its tab prevents new readers without cancelling
/// this already accepted output or copying the result outside the cache budget.
pub struct PreparedExtractExport {
    snapshot: Option<Arc<Snapshot>>,
    payload: ExportExtractPayload,
}

impl PreparedExtractExport {
    pub fn project_id(&self) -> uuid::Uuid {
        self.snapshot
            .as_ref()
            .expect("prepared export retains its result")
            .summary
            .project_id
    }
}

impl Drop for PreparedExtractExport {
    fn drop(&mut self) {
        drop(self.snapshot.take());
        cache::prune_released();
    }
}

pub fn prepare_extract_export(
    store: &ProjectsStore,
    payload: ExportExtractPayload,
) -> Result<PreparedExtractExport, AppError> {
    Ok(PreparedExtractExport {
        snapshot: Some(cache::pin(store, payload.result_id)?),
        payload,
    })
}

pub fn export_extract(
    store: &ProjectsStore,
    payload: ExportExtractPayload,
    job: &JobContext,
) -> Result<ExportResult, AppError> {
    export_prepared_extract(store, prepare_extract_export(store, payload)?, job)
}

pub fn export_prepared_extract(
    store: &ProjectsStore,
    prepared: PreparedExtractExport,
    job: &JobContext,
) -> Result<ExportResult, AppError> {
    let snapshot = prepared
        .snapshot
        .as_ref()
        .expect("prepared export retains its result");
    let destination = Path::new(&prepared.payload.destination);
    let headers = vec![
        "Value".to_string(),
        "Count".into(),
        "Rows".into(),
        "Scope".into(),
    ];
    let validate = || {
        snapshot.validate(store)?;
        validate_destination(destination, store, prepared.payload.allow_overwrite)
    };
    write_atomically(destination, job, validate, |_directory, writer| {
        let mut csv = csv::Writer::from_writer(writer);
        csv.write_record(&headers)
            .map_err(|error| AppError::Other(error.into()))?;
        for (index, value) in snapshot.values.iter().enumerate() {
            if index.is_multiple_of(256) {
                job.check_cancelled()?;
                job.report_progress(
                    index as u64,
                    Some(snapshot.values.len() as u64),
                    Some("Exporting extracted values".into()),
                );
            }
            csv.write_record([
                value.value.as_str(),
                &value.count.to_string(),
                &value.rows.to_string(),
                value.scope.as_deref().unwrap_or(""),
            ])
            .map_err(|error| AppError::Other(error.into()))?;
        }
        csv.flush().map_err(|error| AppError::Other(error.into()))?;
        job.check_cancelled()?;
        Ok(())
    })?;
    job.report_progress(
        snapshot.values.len() as u64,
        Some(snapshot.values.len() as u64),
        Some("Extraction export complete".into()),
    );
    Ok(ExportResult {
        rows: snapshot.values.len(),
        columns: headers,
        warnings: Vec::new(),
    })
}
