use std::path::Path;

use crate::{
    error::AppError,
    export::atomic::{validate_destination, write_atomically},
    jobs::JobContext,
    ops::export::ExportResult,
    store::ProjectsStore,
};

use super::{cache, ExportExtractPayload};

pub fn export_extract(
    store: &ProjectsStore,
    payload: ExportExtractPayload,
    job: &JobContext,
) -> Result<ExportResult, AppError> {
    let snapshot = cache::get(store, payload.result_id)?;
    let destination = Path::new(&payload.destination);
    let headers = vec![
        "Value".to_string(),
        "Count".into(),
        "Rows".into(),
        "Scope".into(),
    ];
    let validate = || {
        snapshot.validate(store)?;
        validate_destination(destination, store, payload.allow_overwrite)
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
