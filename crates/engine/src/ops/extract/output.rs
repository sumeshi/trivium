use std::{
    fs::{self, File},
    path::PathBuf,
};

use anyhow::Context;

use crate::{
    error::AppError,
    jobs::JobContext,
    models::ProjectSummary,
    ops::projects::{create_temporary_project_with_job, CreateProjectPayload},
    store::ProjectsStore,
};

use super::scan::ExtractedTable;

/// Generated input is private to this operation and is removed on every exit.
/// The normal importer retains its own complete CSV copy inside the new case.
struct CsvWorkspace(PathBuf);
impl Drop for CsvWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub(super) fn open_csv(
    store: &ProjectsStore,
    table: ExtractedTable,
    job: &JobContext,
) -> Result<ProjectSummary, AppError> {
    job.check_cancelled()?;
    fs::create_dir_all(store.staging_root()).context("failed to create extraction workspace")?;
    let directory = store
        .staging_root()
        .join(format!("extract-csv-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&directory).context("failed to create extraction workspace")?;
    let workspace = CsvWorkspace(directory);
    let path = workspace.0.join(&table.filename);
    let file = File::create(&path).context("failed to create extracted CSV")?;
    let mut csv = csv::Writer::from_writer(file);
    csv.write_record(["Value", "Count", "Rows"])
        .map_err(|error| AppError::Other(error.into()))?;
    for (index, value) in table.values.iter().enumerate() {
        if index.is_multiple_of(256) {
            job.check_cancelled()?;
            job.report_progress(
                index as u64,
                Some(table.values.len() as u64),
                Some("Preparing extracted CSV".into()),
            );
        }
        csv.write_record([
            value.value.as_str(),
            &value.count.to_string(),
            &value.rows.to_string(),
        ])
        .map_err(|error| AppError::Other(error.into()))?;
    }
    csv.flush().context("failed to flush extracted CSV")?;
    drop(csv);
    drop(table.values);
    job.check_cancelled()?;
    create_temporary_project_with_job(
        store,
        CreateProjectPayload {
            import_options: None,
            path: path.to_string_lossy().into(),
            description: Some(table.description),
            delimiter: Some(",".into()),
            restore_annotations: Some(false),
        },
        job,
    )
}
