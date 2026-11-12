//! Shared atomic output transaction and source-evidence guard.
use crate::{error::AppError, jobs::JobContext};
use anyhow::Context;
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub(crate) use super::destination::{
    ensure_destination_is_not_managed, ensure_destination_is_not_source,
};

struct Workspace(PathBuf);
impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// All intermediate files live in one sibling directory, removed on error,
/// cancellation, or success. The destination changes only after flush/sync.
pub(crate) fn write_atomically<T>(
    destination: &Path,
    job: &JobContext,
    validate: impl Fn() -> Result<(), AppError>,
    write: impl FnOnce(&Path, &mut BufWriter<File>) -> Result<T, AppError>,
) -> Result<T, AppError> {
    job.check_cancelled()?;
    validate()?;
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).context("failed to create output directory")?;
    if destination.file_name().is_none() {
        return Err(AppError::Message(
            "Output destination has no file name.".into(),
        ));
    }
    let workspace = Workspace(parent.join(format!(".trivium-export-{}", uuid::Uuid::new_v4())));
    fs::create_dir(&workspace.0).context("failed to create output workspace")?;
    let pending = workspace.0.join("output.pending");
    let mut writer =
        BufWriter::new(File::create(&pending).context("failed to create output file")?);
    let result = write(&workspace.0, &mut writer)?;
    writer.flush().context("failed to flush output")?;
    writer
        .get_ref()
        .sync_all()
        .context("failed to sync output")?;
    drop(writer);
    job.check_cancelled()?;
    // Revalidate aliases after a potentially long write. External concurrent
    // filesystem renames can still race this final check and the rename.
    validate()?;
    job.check_cancelled()?;
    fs::rename(&pending, destination).context("failed to finalize output")?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn evidence_alias_changed_during_write_is_rechecked_before_replacement() {
        let directory =
            std::env::temp_dir().join(format!("trivium-revalidate-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let source = directory.join("source.csv");
        let destination = directory.join("output.csv");
        fs::write(&source, b"original evidence").unwrap();
        fs::write(&destination, b"old output").unwrap();
        let meta:crate::models::ProjectMeta=serde_json::from_value(serde_json::json!({
            "id":uuid::Uuid::new_v4(),"name":"test","description":null,"created_at":chrono::Utc::now(),
            "total_records":0,"hidden_columns":[],"source_path":source.to_string_lossy()
        })).unwrap();
        let result = write_atomically(
            &destination,
            &JobContext::none(),
            || ensure_destination_is_not_source(&destination, &meta),
            |workspace, writer| {
                writer.write_all(b"replacement").context("output")?;
                fs::write(workspace.join("run"), b"temporary").context("run")?;
                // Simulate another program replacing the evidence path with a
                // hardlink to the chosen destination while export is in progress.
                fs::remove_file(&source).context("remove source alias")?;
                fs::hard_link(&destination, &source).context("retarget source alias")?;
                Ok(())
            },
        );
        assert!(result.unwrap_err().to_string().contains("source evidence"));
        assert_eq!(fs::read(&destination).unwrap(), b"old output");
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 2);
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn cancellation_after_writing_preserves_destination_and_cleans_all_runs() {
        use crate::jobs::{request_cancel, try_begin_job, JobKind};
        let directory =
            std::env::temp_dir().join(format!("trivium-atomic-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let destination = directory.join("saved.csv");
        fs::write(&destination, b"old destination").unwrap();
        let guard = try_begin_job(JobKind::Export, None).unwrap();
        let id = guard.id();
        let job = JobContext::with_job(guard, None);
        let result = write_atomically(
            &destination,
            &job,
            || Ok(()),
            |workspace, writer| {
                fs::write(workspace.join("intermediate-run"), b"temporary").context("run")?;
                writer.write_all(b"replacement").context("output")?;
                assert!(request_cancel(id));
                Ok(())
            },
        );
        assert_eq!(result.unwrap_err().to_string(), "Cancelled.");
        assert_eq!(fs::read(&destination).unwrap(), b"old destination");
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        fs::remove_dir_all(directory).unwrap();
    }
}
