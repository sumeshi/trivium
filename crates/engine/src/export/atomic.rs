//! Shared atomic output transaction and source-evidence guard.
use crate::{error::AppError, jobs::JobContext};
use anyhow::Context;
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub(crate) use super::destination::validate_destination;

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
    fn destination_created_during_write_requires_confirmation_before_replacement() {
        let directory =
            std::env::temp_dir().join(format!("trivium-revalidate-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let destination = directory.join("output.csv");
        let store = crate::store::ProjectsStore::new(directory.clone()).unwrap();
        let result = write_atomically(
            &destination,
            &JobContext::none(),
            || validate_destination(&destination, &store, false),
            |workspace, writer| {
                writer.write_all(b"replacement").context("output")?;
                fs::write(workspace.join("run"), b"temporary").context("run")?;
                fs::write(&destination, b"concurrent output")
                    .context("create concurrent output")?;
                Ok(())
            },
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .starts_with("Overwrite confirmation required:"));
        assert_eq!(fs::read(&destination).unwrap(), b"concurrent output");
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
