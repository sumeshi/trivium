//! Shared atomic output transaction and source-evidence guard.
use crate::{error::AppError, jobs::JobContext};
use anyhow::Context;
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub(crate) use super::destination::{validate_destination, PublishMode};

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
    validate: impl Fn() -> Result<PublishMode, AppError>,
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
    // Revalidate aliases after a potentially long write, then enforce consent
    // in the publication itself. A newly created output must never overwrite
    // another writer that arrived after the final existence check.
    let mode = validate()?;
    job.check_cancelled()?;
    match mode {
        PublishMode::Replace => {
            fs::rename(&pending, destination).context("failed to finalize output")?
        }
        PublishMode::CreateNew => {
            tempfile::TempPath::try_from_path(&pending)
                .context("failed to prepare output publication")?
                .persist_noclobber(destination)
                .map_err(|failure| {
                    if failure.error.kind() == std::io::ErrorKind::AlreadyExists {
                        AppError::Message(format!(
                            "Overwrite confirmation required: {}",
                            destination.display()
                        ))
                    } else {
                        AppError::from(
                            anyhow::Error::new(failure.error).context("failed to finalize output"),
                        )
                    }
                })?;
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn competing_unapproved_writers_publish_once_and_never_replace_each_other() {
        use std::{
            cell::Cell,
            sync::{Arc, Barrier},
            thread,
        };
        let root =
            std::env::temp_dir().join(format!("trivium-publish-race-{}", uuid::Uuid::new_v4()));
        let store = crate::store::ProjectsStore::new(root.clone()).unwrap();
        let destination = root.join("shared.csv");
        let barrier = Arc::new(Barrier::new(2));
        let writers = (0..2)
            .map(|index| {
                let store = store.clone();
                let destination = destination.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let checks = Cell::new(0);
                    write_atomically(
                        &destination,
                        &JobContext::none(),
                        || {
                            let mode = validate_destination(&destination, &store, false)?;
                            checks.set(checks.get() + 1);
                            // Both writers have checked the absent destination. The
                            // publication itself must still reject the second one.
                            if checks.get() == 2 {
                                barrier.wait();
                            }
                            Ok(mode)
                        },
                        |_, writer| {
                            writer
                                .write_all(&[b'A' + index])
                                .context("write candidate")?;
                            Ok(())
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        let results = writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .collect::<Vec<_>>();
        let published = fs::read(&destination).unwrap();
        let workspaces = fs::read_dir(&root)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".trivium-export-")
            })
            .count();
        fs::remove_dir_all(root).unwrap();
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "exactly one writer may claim a new output name"
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| result.as_ref().is_err_and(|error| error
                    .to_string()
                    .starts_with("Overwrite confirmation required:")))
                .count(),
            1
        );
        assert!(published == b"A" || published == b"B");
        assert_eq!(workspaces, 0);
    }
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
        use crate::jobs::{queue_job, request_cancel, JobKind};
        let directory =
            std::env::temp_dir().join(format!("trivium-atomic-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let destination = directory.join("saved.csv");
        fs::write(&destination, b"old destination").unwrap();
        let guard = queue_job(JobKind::Export, None).unwrap();
        let id = guard.id();
        let job = JobContext::with_job(guard, None);
        let result = write_atomically(
            &destination,
            &job,
            || Ok(PublishMode::Replace),
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
